# Rolling deploys

How to upgrade a cluster one node at a time, with the cluster serving
throughout, and how to make the first upgrade from unitdb v0.3.0. The build order
below is built: capabilities between nodes (`server/internal/cluster_caps.go`),
draining a node on SIGTERM (`Cluster.drain`), the versioned ring and moving
stored messages after a switch (`server/internal/cluster_ring.go`), and the
check that stops a node starting next to a v0.3.0 one
(`Cluster.checkPeers`).

## Why a rolling deploy breaks today

The upgrade from v0.3.0 to the current `master` changes almost everything
the nodes rely on to work together:

| What | Old nodes (v0.3.0) | New nodes (`master`) | In a mixed cluster |
| --- | --- | --- | --- |
| Ring key | contract | contract + topic | different owners for the same request |
| Ring hash | FNV-1a, 20 points per node | FNV-1a + `fmix32`, 160 points | signatures never match; followers rehash at the leader's request over and over |
| Accepting a request | ring signatures must match | the receiver's own ring | old nodes reject every request a new node forwards |
| RPCs | `Master`, `Proxy`, `Ping`, `Vote` | also `Replicate`, `Deliver`, `FetchSession`, `ForgetSession`, `RebuildTopics`, `RebuildHistory`, `Resync` | an old node answers "can't find method" |
| Delivery to another node | raw bytes in `ClusterResp.RespMsg` | `Deliver`, or `ClusterResp.Message` | an old node ignores `Message` and writes nothing |

Nothing tells a node which version its peers run, so any later change to the
protocol or the ring would break a mixed cluster again.

## Design

1. **Versions and capabilities in pings.** `ClusterPing`, and its answer, carry
   a `Version` and a capability set (`replicate`, `deliver`, `ring:2`, ...).
   Each node records its peers'.
2. **New RPCs only to peers that support them**, with a fallback: `Deliver` →
   one `Proxy` call per message; `Replicate` → keep a hint until the peer is
   upgraded; `FetchSession` and `Resync` → skip. RPCs and their structs only
   grow; gob ignores fields a node does not know. Removing one takes two
   releases: stop using it, then delete it.
3. **A versioned ring, switched in two phases.** The leader's pings name the
   cluster's ring version (key scheme, hash, points per node). Every node
   computes the rings it supports but routes by the leader's. The leader moves
   to a new version only once every node supports it; the switch is an
   ordinary rehash, so subscriptions move with the rebalance that exists.
4. **Moving stored data after a switch.** Each node goes through its topic
   index, and the first holder of each topic under the old ring hands its
   messages to the holders the new ring adds, so that each gets them once.
   Built without a relay fallback during the move: a relay then may miss
   what is not copied yet. Only a change of the version the cluster routes
   by moves data; a node that just starts adopts the cluster's version.
5. **Draining a node before it stops.** On SIGTERM a node tells the leader it
   is leaving. The leader takes it out of the ring at once, rather than after
   1.6 s of failure detection; the others move its clients' subscriptions; it
   hands off its hints, and exits. Its clients reconnect elsewhere and resume
   their sessions, which are replicated; request retries cover the switch.
6. **Stored formats readable across versions.** The message header starts with
   a magic: each format change gets a new one, and readers accept every older
   one. A node does not write a format an older node in the cluster cannot
   read: replicas and hints go only to peers whose capabilities include it.

A deploy then goes node by node: drain it, restart it on the new version, and
wait for it to rejoin and hand off its hints. unitdb does not report the
hint backlog yet, so give it a few heartbeats before the next node. Once every node
supports a new ring version, the leader switches to it, and the data follows.

## Capabilities

What each capability in a node's pings says it can do
(`server/internal/cluster_caps.go`). `UNITDB_CLUSTER_CAPS` lists fewer, so
that tests can run a node as an older one.

| Capability | The node | A peer without it |
| --- | --- | --- |
| `replicate` | takes stored messages and hints (`Replicate`), and rebuilds (`RebuildTopics`, `RebuildHistory`) | keeps hints for it until it has it |
| `deliver` | takes messages for its clients in one call (`Deliver`) | one `Proxy` call per message |
| `sessions` | takes session changes (`Replicate`'s log), and fetches and drops sessions (`FetchSession`, `ForgetSession`) | skipped |
| `resync` | sends its clients' subscriptions to a node back in the ring (`Resync`) | skipped |
| `service` | sets a forwarded connection's `Insecure` only for a trusted service's connection, never for a client's own insecure flag | its `Insecure` is not taken: forwarded requests are key-checked |
| `v2keys` | reads v2 client ids and v2 topic keys; since v0.7.0 a node reads and issues nothing else, and still sends it for v0.6.0 nodes | v0.6.0 nodes issue v1 ids and keys, and send no renewed ids; v0.7.0 nodes issue v2 ones anyway, and warn of the peer, which can't take them (see [upgrading to v0.7.0](#upgrading-to-v070)) |
| `tls` | listens for cluster connections over mutual TLS (`cluster_config.tls` is set) | nothing: no call depends on it |
| `revocations` | holds the security state, what `unitdb/revoke` revoked, and takes it from the others (`Revocations`) | sent none of it: it refuses nothing for being revoked |

### Upgrading to service ids

v0.5.0 nodes forward a client's own CONNECT insecure flag, and take the one
another node forwards. A node of this version refuses insecure clients unless
it runs standalone with `allow_insecure`, takes a forwarded connection's trust
only from peers that advertise `service`, and refuses to start with
`allow_insecure` in a cluster. So, before the upgrade:

1. Give the clients that connected insecure, such as backends, a service
   client id (`server/cmd/mintid -service`), or topic keys. Clients that
   send the insecure flag are refused by every upgraded node.
2. Upgrade node by node as usual. Delivery between old and new nodes goes on;
   only a service's request without keys can be refused until both nodes on
   its path are upgraded: one through an old node, for a topic a new node
   owns, is key-checked there; and an old node owning the topic takes a
   connection's trust only from its first forwarded request, so a connection
   vouched for after that stays key-checked there.

Sessions are now bound to the client id that started them. While a node
knows of a peer without `service`, it also keeps a session with a client
session key under the key old nodes find it by, and resumes one an old node
kept there, so that clients moving between old and new nodes keep their
sessions. Once every node advertises `service`, such old copies are not
resumed: a client that last connected to an old node with a session key
starts a new session once.

### Upgrading to v2 client ids and topic keys

This is how v0.6.0 moved a cluster to v2 ids and keys; since v0.7.0 nodes
issue and take v2 ones only (see [upgrading to v0.7.0](#upgrading-to-v070)).

v0.6.0 issues v2 client ids and topic keys, and reads v1 ones too.
Older nodes read only v1 ones, and two things cross between nodes:

- **Topic keys.** A node checks the key of a request another node forwards
  for a topic it owns, so an older owner refuses a v2 key with status 400.
- **Clients.** A client may connect to any node, and an older one takes a v2
  client id for an invalid one: it refuses it, and assigns the client a new
  primary id of a new contract.

Client ids are opened only on the client's own node: what nodes send each
other is the opened id. A v2 id opens to the 12 bytes of a v1 one followed
by its 8-byte uuid, which older nodes ignore: they read the contract and the
permissions where they always were.

So a node issues v2 ids and keys only once every other node has told it, in
the leader's pings, that it reads them (`v2keys`), and v1 ones until then:
keygen gives v1 signed keys, `unitdb/clientid` and assigned ids are v1, a
keygen request with a `ttl` is refused with status 503, since v1 keys can't
expire, and clients of v1 ids are not sent v2 ones. Right after a node
starts, until it hears the others, it issues v1 ones too; a node that is
down, and was never heard, holds the cluster on v1. So:

1. Upgrade node by node as usual. Nothing changes for clients: every id and
   key issued is v1, which every node reads.
2. Once every node runs this version, and the leader's pings have gone
   round, nodes issue v2 ids and keys, and send clients of v1 ids the same
   id as v2 on `unitdb/clientid/`.
3. Don't roll a node back to an older version after that: v2 ids and keys
   don't work on it. v0.6.0's `server/cmd/mintid -v1` minted v1 ids for
   such a cluster; v0.7.0's mints none.

`client_id_ttl`, `primary_id_ttl` and `topic_key_ttl` apply to v2 ids and
keys only: ids and keys issued while the cluster is on v1 never expire.

Nothing stored changes: session rows are keyed by a hash of the client id,
as before, and a v1 id keeps its sessions once it is renewed as v2, which is
the same id. A secondary id issued as v2 has a uuid, and so sessions of its
own: v1 secondary ids of a contract issued in the same second were the same
id, and shared them. Subscriptions are stored by topic, without their keys.

The keyring is read once at start. Rotating keys is a rolling restart: give
every node the new keyring, with the old key as a `read` key, before any
node issues with the new key, or a node not yet restarted refuses what the
new key issued; see the README.

### Upgrading to revocation

`unitdb/revoke` revokes a v2 client id or topic key by its uuid, or
everything a contract issued before now. Every node with `revocations` holds
what was revoked, and checks it where it opens a client id (CONNECT,
`unitdb/service`) or checks a topic key; see
[cluster-data-sync.md](cluster-data-sync.md#security-state-revocation). An
older node lacks the `Revocations` call: it is sent nothing, answers
`unitdb/revoke` with status 404, and refuses nothing for being revoked. So in
a mixed cluster:

- **A revoked client id still connects to an older node**, and its client is
  served there. Ids are opened only on the client's own node: a node that
  takes a request another node forwards checks its topic key, not its id.
- **A revoked topic key is refused** where the client's node or the topic's
  owner has the state: each checks the key, the client's node before it
  forwards the request. Only a request whose client's node and topic owner
  are both older is taken.
- **`{"all": true}` is refused by v0.6.0 with status 503** while the
  cluster issues v1 ids and keys (some node lacks `v2keys`): it would
  refuse them as soon as they were issued. Revoking a v2 id or key by its
  uuid is taken. v0.7.0 issues v2 ones only, and always takes it.

Nothing stored changes for older nodes: the state is kept under a namespace
of its own. Once every node runs this version, a node that was older is sent
the whole state when the others reconnect to it after its restart, and asks
for it itself as it starts: nothing revoked before is lost. Rolling a node
back to an older version stops enforcement there, as above; its store keeps
the state for when it is upgraded again.

### Turning on encryption at rest

`encrypt_at_rest` changes nothing on the wire, so it needs no capability:
records are sealed at the store, below everything the cluster sends, and
what nodes send each other (replicas, hints, session logs and rows, history
for a rebuild) is the opened record. Each node seals what it stores as it is
set to, and reads sealed and plain records alike, so:

1. Give every node the same keyring first, as for anything else the keyring
   does; a node opens sealed records only with keys of its own keyring.
2. Turn `encrypt_at_rest` on node by node, with a rolling restart. A cluster
   with it on some nodes and off on others, or with nodes of an earlier
   version, works: an earlier node stores what it is sent as it is.
3. Don't roll a node back to a version without `encrypt_at_rest` once it has
   sealed records: it would read them sealed. Turning it off is not enough,
   since what was sealed stays sealed.

Since v0.7.0 `encrypt_at_rest` is on unless set to `false`: a node upgraded
from v0.6.0 without it in its config starts sealing (step 2) as it is
upgraded, so give every node the same keyring first. v0.6.0 opens sealed
records, so a rollback to it reads them; v0.5.0 and before don't.

Rotating the keyring then works as for client ids: keep the old key as a
`read` key while the store may hold records it sealed.

### Upgrading to v0.7.0

v0.7.0 refuses v1 client ids, v1 signed topic keys and unsigned topic keys
(finding 12 of the security review), and drops `accept_unsigned_keys`. It
issues and reads v2 ids and keys only, whatever its peers say they read.
Every client must hold a v2 id and v2 keys before its node is upgraded, and
the only version that hands them out to clients of v1 ones is v0.6.0. So:

1. **Every node on v0.6.0 first.** A v0.5.0 node can't be upgraded straight
   to v0.7.0, nor run beside v0.7.0 nodes: it reads no v2 ids or keys (it
   lacks `v2keys`), and v0.7.0 nodes issue nothing else and refuse the v1
   ones it issues. A v0.7.0 node logs a warning naming such a peer ("reads
   no v2 client ids or topic keys"). Upgrade v0.5.0 nodes to v0.6.0 as
   above, and let the leader's pings go round, so that every node says it
   reads v2 ones and the cluster issues them.
2. **Every client renewed to a v2 id: the renewal push.** On v0.6.0, a
   client that connects with a v1 id is sent the same id as v2 on
   `unitdb/clientid/`, with the same contract and sessions; it must keep it
   and connect with it from then on. Have every client connect at least
   once, with a client library that keeps the pushed id, before step 4.
   Ids kept in configs, such as services', are sealed again with
   `server/cmd/mintid -from <v1 id>` (v0.6.0's or v0.7.0's, with the
   keyring holding the key that sealed it). Don't restart a v0.6.0 node
   meanwhile without need: until it hears its peers it issues v1 ids and
   keys again.
3. **Every client on v2 keys from keygen.** Keys are requested again with
   `unitdb/keygen`, which v0.6.0 answers with v2 keys once the cluster
   issues them; v1 signed keys and unsigned keys stop working at step 4.
   Then remove `accept_unsigned_keys` from every config: v0.7.0 refuses to
   start while it is `true` ("accept_unsigned_keys is set, but unsigned
   topic keys are refused since v0.7.0"), and warns while it is `false`.
4. **Then v0.7.0, node by node**, as below ([upgrading from
   v0.6.0](#upgrading-from-v060)). In the mixed cluster v0.6.0 nodes still
   take v1 ids and keys from their own clients, but v0.7.0 nodes refuse
   them: a v1 id with return code 0x02 and no new id, a v1 or unsigned key
   with status 401 ("no longer accepted"), on a client's own node or on the
   topic's owner a request is forwarded to. A client left on a v1 id after
   the upgrade has its owner seal it again with `mintid -from`.

Rolling a node back to v0.6.0 is safe for ids and keys: v0.6.0 reads the v2
ones v0.7.0 issued. Clients that were refused on v1 ids or keys are still
refused by v0.7.0 nodes once the rollback is undone.

### Upgrading from v0.6.0

v0.7.0 keeps the store's own records under `$sys` topics instead of under
fixed ids (see
[message-log-replication.md](message-log-replication.md#the-stores-own-records)),
and turns `encrypt_at_rest` on by default. Neither changes the wire: nodes
send each other contracts, topics and records, not where they store them, so
there is no new capability, and a cluster mixing v0.6.0 and v0.7.0 nodes
delivers, replicates, hands off hints and shares revocations as before.

1. Give every node the same keyring, if it hasn't one; or set
   `"encrypt_at_rest": false` to keep v0.6.0's behaviour.
2. Upgrade node by node, as an ordinary deploy. Each node moves what v0.6.0
   stored (the topic index, replicas, replicated messages' ids, hints, the
   security state) as it starts, before it takes clients or joins the
   cluster: the start takes longer in proportion to the replicas stored. A
   crash during the move leaves the rest to the next start. A node that
   fails to move them, on an error of the store, refuses to start, rather
   than serve without them.
3. Nothing else: the moved records are read where they are now, and the old
   namespaces are empty.

Rolling a node back to v0.6.0 after it ran v0.7.0: v0.6.0 doesn't read
what v0.7.0 stored under `$sys` (replicas, hints, the topic index, the
security state), and keeps no record of it, so the node lacks the replicas
v0.7.0 stored (the owners still have their messages) and refuses nothing it
was told was revoked until the other nodes send it the state again on
reconnect. Rolling every node back loses what was revoked, and the replicas:
revoke again after it. What v0.6.0 then stores under the old ids is moved
again at the next upgrade.

## Moving a cluster to TLS

A node with `cluster_config.tls` listens on its `tls_addr` beside its plain
`addr`, and dials a peer over TLS when its config lists the peer's
`tls_addr`; otherwise it dials the peer's plain `addr`. A node without `tls`
dials every peer's plain `addr`, whatever its config lists. With
`tls.require`, a node closes its plain listener and dials only `tls_addr`s.
So a running cluster moves to TLS in three passes, one node at a time, each
restart as in an ordinary deploy:

1. **Listen on TLS.** Issue each node a certificate from the cluster's CA,
   with its node name as a DNS name, for both server and client use. Restart
   each node with `tls` set (`ca_file`, `cert_file`, `key_file`), its own
   `tls_addr`, and the `tls_addr` of the nodes moved before it. The nodes
   not moved yet dial it on its plain `addr`, which it still takes. A node
   refuses to start with a certificate the CA did not sign or that does not
   name it, and before a node is restarted with `tls` its peers must not list
   its `tls_addr`: they would dial a port it does not listen on.
2. **Dial TLS.** Restart each node whose config lacks another node's
   `tls_addr` (all but the last moved) with every node's `tls_addr`. Every
   connection between nodes is then over TLS.
3. **Require TLS.** Restart each node with `"require": true`. It no longer
   takes plain connections; the others already dial it over TLS. Its check
   for a v0.3.0 peer (`Cluster.checkPeers`), which dials plain addresses, is
   skipped.

Until the last pass the plain ports are open, and serve any caller as
before: keep them firewalled to the other nodes. To go back, undo the passes
in reverse order. A node's `tls` capability tells its peers it is on TLS
(pass 1 done).

## The first upgrade, from v0.3.0

Nodes of v0.3.0 cannot tell what they can do, and teaching the new code
their protocol (the contract ring, signature checks) would be throwaway work
for one upgrade. The upgrade takes a maintenance window instead:

1. **Stop every node.** A node of the new version refuses to start while a
   node of v0.3.0 answers at a configured address: it calls
   `Cluster.Replicate`, which v0.3.0 lacks, and exits with the node's name.
2. **Deploy and start every node.** Subscriptions are held in memory: clients
   subscribe again when they reconnect.
3. **Sessions** need nothing: resuming a session asks every node for its copy.
4. **Messages stored before the upgrade stay where they are,** unreachable by
   relay, until they expire with their TTL: relays return what is published
   from the upgrade on. They cannot be moved: unitdb keeps only hashes of a
   topic's parts, so a v0.3.0 store cannot list its topics, and the old
   routing stored a message on one or two nodes with no id to tell the
   copies apart.

After this upgrade, later ones are rolling: drain a node, restart it on the
new version, and so on.

## Build order

1. Versions and capabilities in pings; RPCs by capability, with fallbacks.
   Test: a cluster with one node running with the new capabilities turned off
   still delivers, replicates and resumes sessions.
2. Drain on SIGTERM. Test: SIGTERM a node while publishing; every publish is
   acknowledged, and its clients resume elsewhere with nothing lost.
3. The versioned ring, switched by the leader. Test: switch the ring of a
   running cluster; delivery, subscriptions and ownership still hold.
4. The data move after a switch. Test: history published before the switch
   is relayed after it, exactly once.
5. The v0.3.0 upgrade: the runbook above, and the check that stops a node
   starting next to a v0.3.0 one. Test: a node whose cluster has a v0.3.0
   node up refuses to start.

## Decisions

- The first upgrade takes a maintenance window, rather than a compatibility
  mode for the old protocol, and does not move messages stored before it.
- A ring switch is automatic, once every node supports the version, up to
  `ring_version` in `cluster_config`, which an operator can set to hold the
  cluster on a version.
- A drain takes up to `drain_timeout` in `cluster_config`, 10s by default,
  before the node exits anyway.
