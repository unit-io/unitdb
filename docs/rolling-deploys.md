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
