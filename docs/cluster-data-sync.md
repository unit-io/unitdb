# Cluster data synchronisation

How the nodes of a unitdb cluster agree on who holds what, and how
client requests and data move between them. The code is in
`server/internal/cluster.go`, `cluster_leader.go`, `conn.go` and
`hdl_conn.go`.

Each node has its own unitdb store (`db_path`). The cluster keeps three
things in sync:

- **which node owns each topic**: a ring hash every node computes;
- **which nodes are alive**: a leader's heartbeats;
- **where each client's subscriptions are held**, as the ring changes;
- **what was revoked** in each contract (`unitdb/revoke`), which every node
  holds.

Stored messages and session logs are also replicated to more than one node;
see [message-log-replication.md](message-log-replication.md).

## Ownership: a ring hash keyed by topic

Every topic has one owner: `ring.Get("<contract>/<topic>")`, the topic
without its options. The owner holds every subscription to the topic, stores
its messages and delivers them.

- The ring is a consistent hash of the node names in `cluster_config`, with
  160 points per node (`clusterHashReplicas`). Its hash is FNV-1a mixed by
  MurmurHash3's finalizer, so that keys differing in their last characters,
  such as consecutive ids, get independent owners. Each node of 3 to 5 owns
  within about 10% of an even share of the keys.
- **Wildcard subscriptions** (`*`, `...`) match topics of every owner, so every
  node holds them.
- `Ring.GetN(key, n)` gives a key's owner and the next distinct nodes: its
  replica set.
- **The ring is versioned** (`cluster_ring.go`): version 1 is FNV-1a with 20
  points per node, version 2 the mixed hash with 160. The leader's pings name
  the version the cluster routes by: the highest every live node supports, up
  to `ring_version` in `cluster_config`. A switch is a rehash, so
  subscriptions move with the rebalance. Each topic's first holder under the
  old version hands its stored messages to the holders the new version adds
  (`moveHistory`); relays during that may miss what is not copied yet.

## Membership: leader, heartbeats, rehash

A leader, elected in a cut-down Raft with no log, pings every node each
heartbeat and decides which nodes are live. The ring over the live nodes is
the one requests are routed by.

```
 leader ──Ping{term, signature, live nodes}──▶ followers     every heartbeat (75–125 ms)
   │  a ping times out after a heartbeat, and counts as a failure
   │  node_fail_after failures in a row → node out of the ring → leader rehashes
   │  the node answers again → back in the ring → rehash
 follower: signature differs on 2 pings in a row → rehash(ping's nodes)
 follower: no ping for vote_after heartbeats → term + 1 → Vote → leader on a majority
```

With the sample config (`heartbeat` 100 ms, `vote_after` 8,
`node_fail_after` 16) a lost leader is replaced in about 0.8 s, and a dead or
frozen node leaves the ring in about 1.6 s. Failover needs 3 nodes or more.

## Request path

A client's node forwards each subscribe, unsubscribe and publish for a topic
it does not own to the topic's owner, one topic at a time, over net/rpc
(`Cluster.Master`). The owner treats the forwarded client as a local session,
an `rpcConn` keyed by the client node's connection id.

The owner checks a forwarded request's topic key itself, with the client id
the client's node sends. It skips the check only when the request says the
connection is a trusted service's (`ClusterSess.Insecure`: a service client
id, or a connection a service vouched for with `unitdb/service`), and only if
the sending node advertises the `service` capability: an older node sends
its client's own CONNECT insecure flag, which no node takes any more. The
trust is taken per request, since a service may vouch for a connection after
its first request. A cluster refuses insecure clients, and a node with
`allow_insecure` refuses to start. Special requests (`unitdb/...`) are
answered by the client's node and never forwarded; one that arrives
forwarded is dropped.

Nodes trust each other's requests, so a node must know who calls it. With
`cluster_config.tls` set, nodes talk over mutual TLS (`cluster_tls.go`): each
has a certificate signed by the cluster's CA naming its node name (a DNS
name), and listens on its `tls_addr`. A node takes a TLS connection only from
a certificate naming exactly one other configured node, and serves its calls
as that node's: a call that names its sender (`Node` in `ClusterReq`,
`DeliverReq`, `ReplicateReq`, `RebuildReq`, `RebuildHistoryReq`,
`FetchSessionReq`, `ForgetSessionReq`, `ResyncReq` and `ClusterVoteRequest`;
`Leader` in `ClusterPing`) must name it, or is refused. `Proxy` names no
sender. `Master` also drops a request for a connection it holds for another
node. A node dials a peer's `tls_addr` and checks that the peer's certificate
names the peer.

A node still listens on its plain `addr` beside the TLS one unless
`tls.require` is set, so that a cluster can move to TLS node by node
([rolling-deploys.md](rolling-deploys.md#moving-a-cluster-to-tls)). The plain
port has no authentication or encryption: until `require` is set on every
node, bind the cluster ports to a private network and firewall them to the
other nodes.

```
client ── node C (client's node) ──────────────────── node O (topic owner)
SUBSCRIBE ─▶ ACK ◀─ (C acknowledges)
             ── Master{sub} ──────────────────────▶ rpcConn, subscription stored
PUBLISH   ─▶ ── Master{pub} ──────────────────────▶ stored, subscribers found
             ◀─ Deliver{messages for C's clients} ── one call per node, to all at once
message   ◀─ (C delivers as to a local subscriber: sent, batched, or logged + NOTIFY)
disconnect─▶ ── Master{ConnGone} ─────────────────▶ rpcConn stopped, subscriptions dropped
```

- **Delivery happens on the client's node.** The owner hands the message and
  its delivery mode to the client's node (`Cluster.Deliver`), which delivers
  it as to a local subscriber. Reliable and batch messages are therefore
  logged where the client's RECEIVE arrives, and message ids come from the
  client's own connection.
- **Which requests a node takes** is decided by its own ring, not by comparing
  ring signatures, as rings disagree for a few heartbeats after a rehash
  (`Cluster.takes`): every subscribe and unsubscribe; a publish only for a
  topic it owns; a relay only for a topic it holds, and not while it rebuilds.
- **Retries.** A publish or subscribe that was not processed (rejected, or not
  sent as the owner's connection is down) goes again to the owner the current
  ring gives, every 100 ms for up to 3 s: longer than failure detection and
  the rehash. So is one whose write failed on a connection closed meanwhile.
  One that failed after being sent is not retried: the owner may have
  processed it. A call the node answers with an error, such as a method it
  lacks, leaves the connection open for the others. A subscribe whose owner is still out of reach after
  that, as when failure detection is slower, is kept: the rebalance once the
  ring drops the owner, or once the owner is back, places it.
- **Relays** go to the topic's owner, which answers from its own messages and
  its replica copies, or to the next replica if the owner does not take it.

## Subscriptions across ring changes

Each client subscription records where it is held (`subRoute`): here, on the
topic's owner, or for a wildcard here and on every other node.

- **After a rehash** each node moves its clients' subscriptions to where the
  new ring holds them, adding the new places before removing the old, and
  retrying moves for up to 2 s while the other nodes catch up.
- **A node that reconnects or rejoins** is sent every subscription it should
  hold. Forwarded subscriptions are held once however often they are sent.
- **A node that starts** asks every other node that answers to send it those
  subscriptions, and waits for them (up to 3 s) before it takes clients
  (`Cluster.resyncOnStart`). A node that restarts before the others fail it
  over stays in their rings, so nothing else tells them it lost what it held
  for their clients, and a publish on one of its topics right after it takes
  clients again would miss those subscribers.
- **A node that stalled** without its connections failing, as in a
  partition, is asked by the others to send its clients' subscriptions again
  when it rejoins: they dropped what they held for it while it was out.
- Nodes stop the proxied connections of nodes that left the ring.

## Security state: revocation

What `unitdb/revoke` revoked is, per contract, a not-before time and the
uuids of revoked client ids and topic keys, each until a time or for ever
(`server/internal/revocation.go`). Every node holds all of it, as it holds
wildcard subscriptions, and checks it where it opens a client id (CONNECT,
`unitdb/service`) or checks a topic key: its own clients' requests, and the
ones other nodes forward to it for topics it owns.

```
client ── node A ──────────── Revocations{changes} ──────▶ every other node
revoke  ─▶ merged, stored, answered 200            merged, stored; what changed
                                                   is sent on to the others
node B (re)connects to node C:
           B ── Revocations{whole state, Full} ──▶ C   merged
           B ◀─ C's whole state ───────────────────    merged
```

- **States merge whatever their order.** The later not-before time wins, and
  for a uuid the later end, for ever the latest; a revocation that is over is
  dropped. Merges commute and repeat harmlessly, so every node ends with the
  same state however changes arrive, and a change is sent on only by a node
  it changed, which stops once every node has it.
- **On change** a node sends what changed to every other node, at once, and
  each sends on what changed for it: a change reaches a node the first sender
  could not reach in time, if another did.
- **A node that reconnects**, or restarts, exchanges the whole state with
  each node it connects to: the reconnecting node sends its own and is
  answered with the other's (`Full`). So a node that was down when something
  was revoked gets it as soon as it is back, from any node, and what it alone
  held reaches the others. Until then it refuses only what it had stored: a
  new node, or one whose store was reset, refuses nothing for the moment
  between taking clients and its first exchange.
- **Stored** in each node's store under a namespace of its own, as one record
  of the whole state, written and flushed to the store's log before the
  revoke is answered, so it survives a crash.
- **Not-before times are compared with issue times,** both in whole unix
  seconds, from the clocks of different nodes: keep them in sync.
- **An older node,** without the `revocations` capability, is sent none of
  it; see [rolling-deploys.md](rolling-deploys.md#upgrading-to-revocation).

Over cluster TLS, a `Revocations` call must name the node of the caller's
certificate, as the other calls do. Over a plain connection it is not
authenticated: the node it names must be one of the cluster's, but anyone
who can reach the plain cluster ports can revoke. Firewall them, or require
TLS.

## Shutting down

On SIGTERM a node leaves the cluster before it closes its clients'
connections (`Cluster.drain`). It takes itself out of its own ring, so that
it forwards its clients' requests to the topics' new owners and rejects those
for its old topics, which their senders retry. Its ping answers say it is
leaving, so the leader takes it out of the ring at once rather than after
failure detection. It waits for the other nodes to rehash, hands off its
hints and replication queues, and only then closes its clients, which resume
their sessions elsewhere. It waits `drain_timeout` (default 10s) at most.

## Open gaps

- **Forwarded requests have no timeout.** A client's publish to a node that is
  frozen but not yet out of the ring blocks until the node resumes or leaves.
- **Wildcard relays return nothing**, on a standalone server too: the
  storage engine does not match wildcard queries.
- **Nodes of v0.3.0 cannot run in one cluster with later ones**: the
  upgrade from it takes a maintenance window, and a later node refuses to
  start next to one. Later versions can run together: see
  [rolling-deploys.md](rolling-deploys.md).
- Resuming a session asks every node for its copy, adding up to 1 s to the
  connect.

## Tests

`server/e2e/cluster_test.go` runs real 3-node clusters with failover:
delivery across every subscriber/publisher/owner combination, reliable and
batch delivery, wildcards, relays, failover and rejoin, frozen nodes, requests
during a failover, and fan-out to many subscribers.
`server/e2e/cluster_tls_test.go` runs one over mutual TLS (delivery, failover,
replicated relays; callers without a node's certificate and calls naming
another sender are refused) and moves a plain one to TLS node by node.
