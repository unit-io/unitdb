# Cluster subsystem: specification for a clean-room rewrite

Status: specification, 2026-10-08; implemented (`server/internal/cluster_*.go`,
`server/internal/peerwire`).

> Written for the server this cluster layer was first built in. Sections on
> WebAuthn and passkey records, backups off-site and some tests describe
> parts of that server this repository doesn't have; the cluster's behaviour,
> transport and contracts are the same here.

## 0. Purpose and ground rules

`server/internal/cluster.go` and `server/internal/cluster_leader.go` are listed
in `NOTICE` as derived from tinode/chat (GPL v3). They are to be replaced by an
independent implementation so that the notice can be removed. This document
is the only input the implementer gets about those two files. It says **what**
the cluster must do: its behaviour, its contracts with the rest of the
package, the data that crosses its boundary, and what the tests check. It does
not say how the two files are written, and the implementer must not read them.

Rules that follow from that:

- **No wire compatibility with the current build.** The node-to-node
  transport, its framing, its message structs, the election and membership
  protocol may all be new. An upgrade stops every node and starts every
  node on the new build at once; a mixed old/new cluster never exists. (Mixed
  *capability* clusters of the new build still matter: see 1.6.)
- **Persisted state must stay compatible.** Whatever a node stored through
  the store before the upgrade (hints, replica copies, seen ids, session logs
  and rows, the topic index, WebAuthn records, checkpoint markers) must be
  read correctly after it, and the placement of keys on the ring must not
  change, or data stored before the upgrade becomes unreachable. Section 4
  lists the formats.
- **Seams.** Other files of `server/internal` and `server/main.go` call into
  the cluster. Section 3 lists every such use. The new code keeps those seams,
  or the implementer changes the callers as described there. Files other than
  the two GPL ones may be edited where this document says so; they are not
  GPL-derived.
- The other `cluster_*.go` files (`cluster_caps.go`, `cluster_health.go`,
  `cluster_reconcile.go`, `cluster_ring.go`, `cluster_tls.go`,
  `cluster_webauthn.go`) are not part of the rewrite, but they reach into the
  cluster's state (section 3.3), so they will need small edits.
- When done, remove the two files' entry from `NOTICE` (note that the second
  file name there is misspelt `cluste_leaderr.go`).

Throughout, "the core" means the new code replacing the two files.

## 1. Scope and required behaviour

### 1.1 Terms

- **Configured nodes**: every entry of `cluster_config.nodes`, this node
  included. Node names are unique strings.
- **Live set** (also "ring members"): the configured nodes the cluster
  currently routes by. Decided by the leader when failover is enabled; equal
  to the configured nodes when it is not.
- **Ring**: a consistent-hash ring over the live set, built by
  `newRing(version, nodes)` in `cluster_ring.go` on `pkg/hash.Ring`. Exposes
  `Get(key)` (owner), `GetN(key, n)` (owner and the next `n-1` distinct
  nodes) and `Signature()` (a string identifying the ring's membership).
- **Full ring**: the same kind of ring over *all configured nodes*, live or
  not. Used to know which nodes *should* hold a key (for hints, rebuild,
  reconcile, session replica checks).
- **Ring version**: how keys are placed (hash function and points per node);
  see `cluster_ring.go`. Versions 1 and 2 exist; 2 is current.
- **Owner** of a key: `ring.Get(key)`. **Holders** / **replica set** of a key:
  `ring.GetN(key, replicas)`.
- **Ring keys** (must not change, section 4.1):
  - topic: `"<contract decimal>/<topic without options>"`;
  - session: `"session/<session id decimal>"`;
  - WebAuthn user: `"webauthn/<contract>/<user id>"` (built in
    `cluster_webauthn.go`).
- **Wildcard topic**: a topic containing the single-level wildcard symbol
  or ending in the multi-level one (`message.TopicWildcardSymbol`,
  `message.TopicMultiWildcardSymbol`). It has no owner.
- **Client node**: the node a client's socket is connected to. **Proxied
  session**: the stand-in a topic owner keeps for a client of another node.
- `replicas`: the configured replication factor (section 2).

### 1.2 Lifecycle

1. **Init** (`ClusterInit`, called by `server/main.go:110` before the store
   opens). Parses `cluster_config` (raw JSON). If it is empty, or no node name
   is set (neither `-cluster_node` nor `cluster_config.node`), the server runs
   standalone: `Globals.Cluster` stays nil, and every caller treats nil as
   "single server" (section 3.1 lists the nil-safe calls). Otherwise it sets
   `Globals.Cluster`, applies defaults, fails fatally on an unparsable
   duration (`rebuild_ttl`, `drain_timeout`) or an invalid `tls` block
   (`loadClusterTLS` in `cluster_tls.go`), builds the full ring at the initial
   ring version (`initialRingVersion(ring_version)`), logs it with
   `logRingVersion` (tests parse that line, section 6), and sets an initial
   live set of all configured nodes. Init must not need the store, and must
   not listen yet.
   - Between Init and Start, `CheckStart` (`restore.go:63-104`) makes one-shot
     calls to every peer (`StartedFrom`) through `ClusterNode.dial`, and
     `restore.go` sets `restoreRequested`/`restoredPath`. So outbound calls
     must work before Start, without this node listening.
   - Init returns an int "worker id" (1-based index of this node's name among
     the sorted configured names; 1 standalone). `main.go` ignores it.
2. **Start** (`Globals.Cluster.Start()`, `server/main.go:145`, after the
   service exists and `Globals.Service` is set). In order, the node must:
   - listen for peers (plain address unless TLS is required; TLS address when
     configured, `serveTLS` in `cluster_tls.go`);
   - begin connecting to every peer and keep reconnecting (retry every
     200 ms while down);
   - start per-peer replication senders and the periodic hint handoff;
   - register itself as `store.OnLogChange` so every session log change is
     replicated (1.11);
   - load up to 100,000 recently stored replica ids from `store.Seen.Recent`
     into its de-duplication set (1.10);
   - if `replicas >= 2`, `store.WasEmpty()` and it has the `replicate`
     capability: mark itself *rebuilding* and rebuild in the background (1.13);
   - if `restoreRequested` and `replicas >= 2` and it has the `reconcile`
     capability: mark itself *reconciling*, drop every message hint it keeps
     for any peer (they are as old as the checkpoint), and run
     `reconcileAll()` (in `cluster_reconcile.go`) in the background;
     otherwise, if `restoreRequested`, call `markRestored(restoredPath)`;
   - start membership/leader work if failover is enabled;
   - before returning (so before the service accepts clients): ask every
     peer that answers to resend the subscriptions of its clients that this
     node should hold, and wait for each, up to 3 s per peer (1.9,
     "start resync").
   - Today Start also refuses to start (fatal) when a configured peer runs a
     pre-replication build (`90d45ce`). With no wire compatibility this check
     is moot; see open point 7.3.
3. **Drain** (`Globals.Cluster.drain()`, `service.go:338`, nil-safe), called
   on SIGTERM before client connections are closed. The node must:
   - mark itself *leaving* (readiness fails from now on, metrics show it);
   - take itself out of its own ring at once, so that it forwards its
     clients' requests to the topics' next owners and rejects forwarded
     requests for topics it no longer owns (senders retry, 1.7);
   - get the other nodes to drop it from their live sets without waiting for
     failure detection, and wait until they have (or until the deadline), so
     that a publish to one of its topics during the drain is acknowledged in
     well under 1 s (`TestClusterDrainOnSIGTERM` asserts every publish acked,
     the slowest under 1 s);
   - hand off every hint it keeps, and wait until its replication queues are
     empty and nothing is in flight;
   - never take longer than `drain_timeout` (default 10 s) in total.
   A leaving node must never be put back in any live set while it is still
   leaving, and must not become leader.
4. **Shutdown** (`Globals.Cluster.shutdown()`, `service.go:370`, nil-safe,
   idempotent), after the store is closed: close listeners, stop membership
   work, reconnect loops, replication senders and handoff loops. Readiness
   reports "stopped". `Globals.Cluster` must stay set (goroutines may still
   read it).

### 1.3 Membership and the leader

The election and failure-detection protocol is free. It must give these
guarantees, which tests and other files rely on:

- **Enabled** only when `failover.enabled` and at least 3 configured nodes.
  With failover disabled (or fewer than 3 nodes), the live set is all
  configured nodes, permanently, and there is no leader.
- **One leader per majority.** At most one node acts as leader for any
  majority of configured nodes; a minority partition has none. Without
  failures, leadership is stable: `TestClusterElectsOneLeader` waits up to
  10 s for every node to agree on one leader, then checks it is unchanged 2 s
  later.
- **Re-election.** After the leader dies, freezes or leaves, a new one is
  in place within a few hundred milliseconds to a couple of seconds with the
  sample settings (heartbeat 100 ms, `vote_after` 8). Tests wait up to 10 s.
- **Failure detection.** A peer that does not answer the leader (dead, or
  frozen with its sockets open: tests use SIGSTOP) is removed from the live
  set after about `node_fail_after` heartbeats (1.6 s with the sample
  settings). Each probe of a peer must time out within about one heartbeat,
  so that one stalled peer neither delays the probes of the others nor goes
  undetected. `TestClusterFrozenNodeIsFailedOver` freezes a follower, waits
  3 s, and expects a publish on its topic to be acknowledged within 2 s.
  `TestClusterSubscribeOutlastsFailureDetection` sets `node_fail_after` 50
  (5 s, longer than the 3 s forward retry window).
- **Recovery.** A peer that answers again is put back in the live set within
  a few heartbeats. This must hold however the current live set came about:
  a node that answers and is not in the live set goes back in, even if this
  leader never saw it fail (e.g. the previous leader shut down and removed
  itself, and this leader inherited that ring). `TestClusterGracefulRestartRejoins`
  restarts the leader and then a follower gracefully and expects every node
  ready within 30 s and one leader within 10 s after each.
- **Leaving nodes.** A node that is draining is removed from the live set as
  soon as the leader learns it is leaving, and stays out while it leaves.
  If the leader itself drains, the remaining nodes must also stop routing to
  it promptly (they learn a live set without it, or elect a successor fast).
- **Propagation.** Every node adopts the leader's live set; a follower whose
  ring differs from the leader's must converge within a couple of heartbeats
  (tolerating one heartbeat of disagreement just after a change is fine;
  requests are accepted by the receiver's own ring, 1.7, so a short
  disagreement is harmless).
- **What the leader is used for** (and nothing else depends on it):
  1. deciding the live set, and so the ring everyone routes by;
  2. choosing the ring version (`chooseRingVersion` in `cluster_ring.go`:
     the highest version this node and every live node support, up to
     `ring_version`; unchanged until every live peer has reported its
     capabilities) and telling every node, which adopts it with
     `adoptRingVersion` and rehashes; see 1.4 for the subtle first-round
     rule;
  3. spreading each node's capabilities (protocol version, capability list,
     supported ring versions; `NodeCapabilities` in `cluster_caps.go`) to all
     nodes, so that each node calls `setCapabilities` for every peer;
  4. readiness: each node records "heard from a leader (or is one) at time
     T, leader name L" through `c.health.leaderSeen(L)` (type
     `clusterHealth` in `cluster_health.go`); readiness fails when that is
     older than `heartbeat × (vote_after + 1)`.
- **On every live-set change** a node must: rebuild its ring
  (`newRing(getRingVersion(), liveSet)`), replace the ring and live set
  atomically for readers, then (asynchronously) rebalance its clients'
  subscriptions (1.9), and for each node newly added to the live set: hand it
  its hints (1.12) and ask it to resend its clients' subscriptions to this
  node ("resync", 1.9). A node that is leaving leaves itself out of any live
  set it adopts.
- **Logging** (tests parse it, section 6): when a node becomes leader it
  logs `Elected myself as a new leader`; when a node learns that another node
  is leader it logs a line matching `leader '<name>' elected` or
  `leader set to '<name>'`. No other log line may match the regular
  expression `leader (?:set to )?'([a-z]+)'` unless it contains
  `wrong leader` (the test skips those).

### 1.4 Ring and rehash (with `cluster_ring.go`)

`cluster_ring.go` owns ring versions and the post-switch data move; the core
owns the current ring, the full ring, the live set and the leader's choice.
Contracts:

- Readers call `getRing()`, `getRingNodes()`, `getFullRing()`,
  `getRingVersion()` concurrently with rehashes; each returns a consistent
  snapshot (today under `ringMu`, an RWMutex that `cluster_ring.go` also
  locks to set `ringVersion` and `fullRing`).
- `setRingVersion(v)` (in `cluster_ring.go`) rebuilds the full ring from
  `allNodes` and logs the version; the caller must then rehash the live ring
  at the new version.
- `clusterRing` (an `atomic.Int32`): the ring version this node last saw the
  cluster route by, 0 before any. `adoptRingVersion(v)` swaps it and, when it
  changes from a non-zero value, starts `moveHistory(prev, v)`, which hands
  stored messages to the holders the new version adds and logs
  `cluster: moved history for ring version N`.
- **First round of a new leader.** A node that becomes leader before it has
  seen the cluster route by any version (`clusterRing == 0`, e.g. right after
  a restart) must take, as "the version the cluster routed by", the lowest
  non-zero version its followers report having routed by (or its own current
  version if none report one), then switch everyone to the chosen version.
  Consequence checked by `TestNewLeaderMovesHistory`: a leader started at v2
  whose follower reports v1 moves history (logs the line); with the follower
  at v2, or reporting none, it moves none, and in all cases the leader ends
  with `clusterRing == 2`. So followers must report, with every answer to the
  leader, the version they last saw the cluster route by.
- **Follower adoption.** A follower told a version it supports that differs
  from its ring's version adopts it and rehashes over the leader's live set at
  once. If it is told a version it does not support, it logs and ignores the
  version. If its ring version already matches but `clusterRing` differs, it
  adopts the version (which may move history) without a rehash.
- `TestClusterRingVersionSwitch` / `TestClusterRingSwitchMovesHistory` run a
  node with `UNITDB_RING_VERSIONS=1`, then restart it with both: the cluster
  must route by 1, then switch to 2, subscriptions must move without
  resubscribing, and relays after the switch return every message exactly
  once.

### 1.5 Transport and TLS

- Each node listens on `addr` (plain TCP) and, when `cluster_config.tls` is
  set, on `tls_addr` (mutual TLS). With `tls.require`, it neither listens on
  nor dials plain addresses.
- Dialing a peer (`ClusterNode.dial`, today in `cluster_tls.go:105-123`): TLS
  to the peer's `tls_addr` when this node has TLS and the peer has a
  `tls_addr`; else plain unless TLS is required (then an error); 1 s connect
  timeout.
- Over TLS a node accepts only client certificates signed by the cluster CA
  that name a configured node other than itself (`certNode`), and **every
  call that names a sending node must name the certificate's node**, or it is
  refused with an error containing `names` (e.g. `a call from two names
  "three" as its sender`). Today that is done by the `peerRPC` wrapper in
  `cluster_tls.go`, one method per RPC; `TestPeerRPCChecksSenders` checks by
  reflection that every net/rpc-shaped method of `*Cluster` (except `Proxy`)
  has a wrapper. With a new transport, enforce the check once, generically,
  at the connection level (open point 7.1).
- A call carries its sender's node name; a node ignores, with an error log,
  forwarded client requests from a name it does not know.
- **Connection semantics the tests assert** (`cluster_node_test.go`):
  - after a peer restarts while the connection was idle, the *first* call
    afterwards must reach it (detect the dead connection and redial before
    sending, rather than failing that call);
  - a call that may have been delivered is never re-sent automatically: a
    failure after sending is returned to the caller, who decides;
  - an *answered* error (e.g. "no such method", a capability refusal, a
    handler's error) must not tear down the connection or fail other calls
    in flight on it;
  - a transport failure marks the peer disconnected and starts reconnecting
    (every 200 ms), and the peer is "resynced" once reconnected (1.9, 1.12,
    and `pushRevocations` in `revocation.go:222`);
  - errors must be classifiable: *not sent* (peer not connected, connection
    already known broken, write on a closed connection) versus *sent and
    failed* versus *answered with an error*. The `retryable` predicate used
    by `conn.go` and `cluster_webauthn.go` is true exactly for "not
    processed": not sent, or rejected by the receiver's ring (`errRejected`).
- Calls need: blocking call; call with timeout (the call may still complete
  remotely after the timeout); asynchronous call with completion. Many calls
  to one peer run concurrently on one connection; one slow call must not
  block others.
- A forwarded request currently has no timeout (docs/cluster-data-sync.md,
  "Open gaps"); see open point 7.6.

### 1.6 Capabilities (with `cluster_caps.go`)

`cluster_caps.go` defines the capability names (`replicate`, `deliver`,
`sessions`, `resync`, `revocations`, `service`, `reconcile`, and `tls`, which
a node has when TLS is configured), `UNITDB_CLUSTER_CAPS` (lets a test run a
node with fewer), `hasCapability`, `refuse`, and per-peer knowledge
(`supports`, `knownToSupport`, `lacks`, `setCapabilities`, `capabilities`) kept
in the `ClusterNode.caps` field.

Required behaviour:

- A handler of a call that needs capability X answers, when this node lacks
  X, with an error the caller recognises as "lacks X" (today
  `refuse(cap)` returns an error shaped like net/rpc's "can't find method"
  answer, and `missingMethod` parses `rpc.ServerError` text). On such an
  answer the caller calls `n.lacks(err, X)` and falls back. With a new
  transport, `missingMethod`/`errCapabilityOff` must be adapted so answered
  errors still carry this meaning (open point 7.1).
- A peer not heard from yet is assumed to support everything (`supports`),
  except for trust decisions, which use `knownToSupport`.
- Fallbacks the core must implement when the peer lacks a capability:
  - `deliver`: one proxy delivery call per message instead of one batched
    call per node (1.8);
  - `replicate`: keep the message as a hint for when it can (1.10);
  - `sessions`: keep the session change as a hint; skip that peer for
    fetch/forget session;
  - `resync`: do not ask that peer to resync;
  - `service`: do not take the forwarded `Insecure` flag from that peer
    (force false);
  - `reconcile`/`revocations`: handled in their own files.
- This node does not originate work needing a capability it lacks itself:
  no replication without `replicate`, no session replication/fetch without
  `sessions`, no batched delivery without `deliver`, no rebuild without
  `replicate`.
- `TestClusterMixedCapabilities` (`UNITDB_CLUSTER_CAPS=none` on one node) and
  `TestServiceIDsCluster` (one node without `service`) check the fallbacks
  end to end.

### 1.7 Routing client requests to a topic's holder

Clients may connect to any node. The core routes per topic:

- **Publish** (`hdl_conn.go:595-617`). For a non-wildcard topic,
  `isRemoteTopic(contract, topic)` says whether another node owns it (nil
  cluster: false; wildcard: false). If so, `routeToTopic(msg, contract,
  topic, conn)` forwards a publish holding just that one topic's message to
  the owner, and returns `(forwarded bool, err error)`:
  - `false, nil` if, by the time it checks, this node owns the topic (the
    caller stores and delivers locally);
  - on a *retryable* failure (not sent, or rejected by the receiver) it
    retries, re-reading the ring each time, every 100 ms for up to 3 s
    (`forwardRetry`, `forwardRetryFor`); on success or non-retryable failure
    or deadline, returns `true, err`;
  - an error for an owner name with no configured node.
  The forward returns only after the owner has handled the publish: stored
  it, replicated it as required (1.10) and started fan-out. The client
  node then acknowledges to its client (`hdl_conn.go`), so an ack means
  "stored on the owner and, unless async, on one replica".
- **Subscribe/unsubscribe** are placed by `conn.go`'s `reconcile`/`release`
  through `holders`, `subscribeAt`, `unsubscribeAt` (1.9). `subscribeAt` and
  `unsubscribeAt` forward a single-subscription subscribe/unsubscribe for
  `conn`'s client to node `name` and return the forward's error (error if no
  such node).
- **Relay** (history request, `hdl_conn.go:475-477`). For a non-wildcard
  topic, `relayFromHolder(msg, contract, topic, conn)` tries the topic's
  holders (`GetN(key, replicas)` on the current ring) in ring order: if this
  node comes first, return false (answer locally) unless this node is
  catching up (rebuilding or reconciling), in which case skip itself; forward
  to the first other holder that takes it and return true; on a failure, log
  and try the next; if none takes it, return false (answer locally).
- **Marking forwarded messages.** Every forwarded subscribe, unsubscribe,
  publish or relay must arrive with its `IsForwarded` field set
  (`utp.Subscribe/Unsubscribe/Publish/Relay`): the owner's handler then
  skips client acknowledgements and does not forward again (`hdl_conn.go`
  around 233-300, `conn.go:237`, `conn.go:304`).
- **Forwarded request payload.** The receiver needs: sender node name; the
  client's connection id (`uid.LID`), session id (`uid.LID`), client id
  (`uid.ID`, from which the contract is read) and `Insecure` flag (a trusted
  service's connection skips topic key checks); the message (one of the four
  types); or a "connection gone" marker with the connection id.
- **Which forwarded requests a node takes** (by its *own* ring, never by
  comparing ring signatures): every subscribe and unsubscribe; a publish only
  if it owns every non-invalid topic in it; a relay only if it is not
  catching up and holds (is in `GetN(key, replicas)` of) every non-wildcard,
  valid topic in it. Otherwise it answers "rejected" without processing; the
  sender treats that as retryable (`errRejected`).
- **Proxied sessions on the owner.** For a taken request, the owner looks up
  `Globals.connCache.get(connID)`; if absent it creates one with
  `Globals.Service.newRpcConn(node, connID, sessID, clientID)`
  (`conn.go:103`; today the first argument is the `*ClusterNode` of the
  sender, stored in `_Conn.clnode`) and starts its outbound pump. Requests
  for one proxied connection are handled one at a time, in arrival order.
  The proxied conn's `insecure` flag is set per request to the forwarded
  flag if the sender is `knownToSupport(service)`, else false. Then the
  request is passed to `conn.handler(msg)` (`hdl_conn.go:106`).
- **Proxied session outbound pump** (replaces `_Conn.rpcWriteLoop`,
  `stopRPC`, `closeRPC`, today defined on `_Conn` in `cluster.go`): reads the
  proxied conn's `send` and `pub` channels and sends each message, encoded
  as client wire bytes, to the client node, which writes them to the
  client's socket (`_Conn.SendRawBytes`, `conn.go:183`). If the client node
  is not connected or the call fails, the pump stops. A stop request (non
  blocking, at most one pending) ends it; a stop carrying bytes sends them
  first. When the pump ends it removes the conn from `connCache` and calls
  `unsubAll()` (`conn.go:608`), which deletes the subscriptions it held.
- **Connection gone.** When a client connection closes, `conn.go:671` calls
  `Globals.Cluster.connGone(conn)` (nil-safe): every node this connection's
  requests were forwarded to (recorded per connection in `_Conn.nodes`, which
  the core writes on every forward and must guard with its own lock, since
  publishes run on their own goroutines while close reads it) is told; each
  stops the proxied session, which drops its subscriptions. Returns the
  first error.
- **Delivery back to the client node.** A proxied conn delivering a publish
  (`_Conn.deliver`, `conn.go:511-517`) calls the client node with the
  message, its `Reliable` flag and the connection id. Today this is
  `c.clnode.call("Cluster.Proxy", &ClusterResp{Message, Reliable, FromConnID}, ...)`
  in `conn.go:514`; the new core must offer a function for this (open point
  7.2). The client node then calls `conn.deliver(message, reliable)` on its
  local connection, so reliable messages are logged and message ids issued
  on the client's own node. Raw bytes (acks, errors produced by the owner
  for the client) go through the same call as raw bytes.
- A delivery or raw-bytes call for an unknown connection id is logged and
  dropped.

### 1.8 Delivery fan-out to subscribers on other nodes

`conn.publish` (`conn.go:473-500`) groups messages for proxied subscribers by
their client node into `map[*ClusterNode][]Delivery` (`Delivery{ConnID,
Message, Reliable}`) and calls `Globals.Cluster.deliverRemote(map)`
(nil-safe, no-op on an empty map). Required:

- one call per client node carrying all of that node's deliveries, all nodes
  in parallel, returning when all calls have returned;
- the receiving node delivers each to its local connection concurrently
  (one slow client must not hold up the others), and logs unknown ids;
- fallback per message if the peer lacks `deliver`;
- `UNITDB_DELIVER_DELAY` (dev builds only, `devDuration` in
  `build_kind.go`) sleeps once per received delivery call (and per proxy
  call carrying a message). `TestClusterDeliveryFanOut` sets 100 ms and
  expects every one of many subscribers on one node to get a message within
  1.5 s, which only one call per node achieves.

### 1.9 Subscription placement, rebalance and resync

`conn.go` owns each client's subscription routes (`subRoute`) and asks the
core where a subscription belongs:

- `holders(contract, topic) (local bool, nodes []string)`: nil cluster:
  `(true, nil)`; wildcard: `(true, every other node in the live set)`;
  otherwise `(false, [owner])` if another node owns it, else `(true, nil)`.
- **Rebalance** (core, after every live-set change and on reconnect): for
  every local client connection (those with no `clnode`) call
  `conn.rehome(resend, last)` (`conn.go:320`), where `resend` is the set of
  nodes to send every subscription to again even if recorded as placed
  there (nodes newly in the live set, or just reconnected). Retry the
  connections that failed every 200 ms, up to 10 attempts, passing
  `last=true` on the final one. Also stop every proxied session whose client
  node is no longer in the live set.
- **Resync request** (core RPC, capability `resync`): a node that puts a
  peer back in its live set asks it to rebalance with `resend = {asker}` (do
  not wait). Used for partitions: `TestClusterPartitionKeepsSubscriptions`.
- **Start resync**: before taking clients, a starting node asks each peer to
  rebalance towards it and *waits* for each, up to 3 s
  (`TestClusterRestartedOwnerKeepsSubscriptions`: a publish right after a
  restarted owner takes clients must reach subscribers of other nodes).
  Peers that are down or lack `resync` are skipped.
- **On reconnect** to a peer (transport level), also rebalance with
  `resend = {peer}`, hand it its hints, and `pushRevocations(peer)`.
- A forwarded subscription is idempotent on the owner (`conn.go:237`).
- Expected outcomes: `TestClusterFailoverKeepsSubscriptions`,
  `TestClusterRequestsDuringFailover`, `TestClusterSubscribeOutlastsFailureDetection`,
  `TestClusterDrainOnSIGTERM`, `TestClusterRingVersionSwitch`.

### 1.10 Replication of stored messages

Called from `hdl_conn.go:617` after the owner stored a publish:
`replicate(contract, name, topic, payload, ttl, wait)` where `name` is the
topic without options, `topic` the stored topic with options, and `wait =
waitsForReplica(isReliable(mode))`.

- `waitsForReplica(reliable)`: true if reliable; else true unless
  `async_replication` (nil cluster: just `reliable`).
- No-op when: nil cluster, `replicas < 2`, wildcard name, or this node lacks
  `replicate`.
- Each replicated message gets an id unique across the whole cluster and
  across restarts of its origin node (today node name + process start time
  + sequence). Replicas use it to store each message once.
- Targets: the other members of `GetN(key, replicas)` on the current ring.
  - With `wait`: deliver synchronously to the first target (ring order) that
    stores it within **1 s** (`replicaAckTimeout`); a target that fails or
    times out gets the message as a hint, and the next is tried. If none
    stored it, log a warning and return anyway (the publish is acked; this
    is the documented loss window).
  - All other targets (and all, without `wait`) get it through a per-peer
    queue of **4096** items, sent in batches of up to **256**; a full queue
    turns the item into a hint (never block the publisher).
  - A target lacking `replicate` gets a hint.
  - Every member of `GetN(key, replicas)` on the **full** ring that is not
    in the live set also gets a hint.
- Per-peer sender: one at a time, batches whatever is queued; if a batch
  fails, every message and session change in it becomes a hint for that
  peer (the peer may have stored part of it; de-duplication covers that), and
  waiters get the error. `UNITDB_REPLICATION_DELAY` (dev only) holds an
  asynchronous batch for that long, gathering more, but an item someone waits
  for must end the delay at once (`TestReplicationDelayLetsWaitersThrough`:
  a waited item queued during a 5 s delay is answered within 1 s, and the
  replica receives both items).
- **Receiving** a batch (capability `replicate` for messages, `sessions` for
  session changes): for each message, skip it if its id is in the
  de-duplication set (last 100,000 ids, preloaded at start from
  `store.Seen.Recent(100000)` oldest first); else
  `store.Message.PutReplica(contract, topic, payload, expiresAt)`; on failure
  forget the id so a resend is stored; on success record the id with
  `store.Seen.Put(id, expiresAt)` *after* the message (a crash in between
  stores it twice, never zero times). Then apply each session change with
  `store.Log.Apply`. If the batch is a hint handoff and this node is
  catching up, drop its messages (the rebuild/reconcile copies them) but
  apply its session changes.
- Tests: `TestClusterReplicatedRelay`, `TestClusterReliablePublishSurvivesCrash`
  (`UNITDB_REPLICATION_DELAY=3s`: reliable acks imply a replica stored it),
  `TestClusterReliablePublishHungReplica` (frozen replica: still acked, later
  one copy), `TestClusterReplicaRestartStoresOnce` (`UNITDB_HANDOFF_INTERVAL=1h`;
  id remembered across a restart), `TestClusterExpressPublishSurvivesCrash`,
  `TestClusterAsyncReplication` (with async, express acks do not wait: N
  publishes acked quickly, then lost on crash).

### 1.11 Replication of session logs

`store.OnLogChange` is set by Start to the core's handler; the store calls it
with a `store.LogOp{Block, Key, Raw, Reset}` for every change this node makes
to a session's log or row (`store.go:481-497`). Required:

- No-op if `replicas < 2` or this node lacks `sessions`.
- Targets: the other members of `GetN("session/<Block>", replicas)` on the
  current ring, through the **same per-peer queues** as messages, so that a
  peer sees one session's changes in order (a waited write must not overtake
  an older queued delete of the same key).
- Wait (up to 1 s for the first success among the targets it was queued to)
  only for a change that stores something (`Raw != nil`, not `Reset`) and
  only when not `async_replication`; deletions and resets never wait. On
  timeout, log a warning; the change stays queued.
- A target lacking `sessions`, or with a full queue, gets a *session hint*;
  so does every full-ring member not in the live set.
- Session hints store only *which* key changed (or that the session was
  reset), with the bytes dropped; the handoff sends the current state then:
  for a key, its current bytes (`store.Log.Raw(key)`; nil means delete); for
  a reset, the reset followed by every current key of the session
  (`store.Log.Keys(block)`). So hints need no ordering.
- Tests: `TestClusterSessionFailover`, `TestClusterSessionHandoff`,
  `TestClusterSessionSurvivesCrash`, `TestClusterMixedCapabilities`.

### 1.12 Hinted handoff

- A hint is stored with `store.Hint.NewID()` and `store.Hint.Put(node, id,
  payload, ttl)`; format in 4.2. TTL: the message's TTL string for message
  hints, `"24h"` for session hints.
- If the store refuses a hint, keep it in memory (at most **10,000**, the
  oldest dropped with an error log) and retry storing it before each handoff
  and on every handoff tick. `TestHintKeptWhenStoreFails` replaces the store
  write with a failing function, checks the hint is not in the store but
  counted in memory, then that a retry stores it and empties memory. Keep a
  test seam for "make the store's hint write fail" (today the package var
  `putHint`).
- **Handoff to node N**: at most one at a time per peer (a second request
  returns at once). Repeatedly read up to the store's query limit of N's
  hints (`store.Hint.Get(N)`), decode (log and skip unreadable ones), send
  them in batches of up to 256 marked as a handoff, and delete each sent
  hint (`store.Hint.Delete(N, id)`) once N accepted the batch. Message hints
  go only if N supports `replicate`, session hints only if it supports
  `sessions`; the rest stay. Stop on a send failure (keep them) or a delete
  failure, or when a round has nothing sendable. Log `handed off to N` with
  the count.
- Triggered: on reconnect to N; when N joins the live set; every
  **5 s** while N is connected (`UNITDB_HANDOFF_INTERVAL` overrides, any Go
  duration; tests set `1h`); during drain for every peer.
- **Dropping message hints for N** (keep N's session hints): when N starts a
  rebuild (it asks for its topics) or a reconciliation
  (`ReconcileTopics` in `cluster_reconcile.go:171-185` calls
  `c.dropHints(req.Node)` while holding `n.handoffMu`), and, for every peer,
  when this node starts restored. Must exclude a concurrent handoff to N.
  `cluster_reconcile.go` uses the per-peer lock `ClusterNode.handoffMu` and
  `Cluster.dropHints(name)`: keep both or change that file.
- `unitdb_cluster_pending_hints` reports the in-memory count.

### 1.13 Rebuilding a node with an empty store

When Start finds `store.WasEmpty()` (and `replicas >= 2`, capability
`replicate`), the node is *rebuilding* (not ready; relays it would answer go
to another holder; forwarded relays to it are rejected) until done:

- For each peer supporting `replicate`: ask it, up to **20** attempts
  **500 ms** apart with a **30 s** per-call timeout, for the topics it should
  hand over; skip the peer (error log) if it never answers. For each topic,
  fetch its messages (30 s timeout) and store each with
  `store.Message.PutReplica`, using the entry's expiry if known, else now +
  `rebuild_ttl`. Log per peer: topics and messages copied.
- **Serving a rebuild request** from node R (capability `replicate`): first
  drop R's message hints (1.12). Return each topic in
  `store.Message.Topics()` such that R is among its full-ring holders
  (`getFullRing().GetN(key, replicas)`) and this node is the first holder in
  that list that is not R and is in the live set. Fetching a topic's
  messages returns `store.Message.History(contract, topic)`
  (`[]store.HistoryEntry{Payload, ExpiresAt, Known}`).
- Tests: `TestClusterRebuildEmptyNode` (each message once, original expiry,
  answers relays alone afterwards), `TestClusterRebuildFromRestartedNode`,
  `TestRestoreLostNodeStartsEmpty`.

### 1.14 Session fetch and forget

- `fetchSession(sessKey uint64)` (`hdl_conn.go:183`, nil-safe; skipped for a
  clean session): no-op if `replicas < 2` or this node lacks `sessions`.
  Ask **every** peer supporting `sessions`, in parallel, for its copy of the
  session row stored under `sessKey`, waiting at most **1 s** in total.
  - A peer's answer: whether it has a row; the row bytes; the session id
    (first 4 bytes of the row, little-endian); and every log entry of that
    session it holds (`store.Log.Keys(id)` and `Raw`). A row shorter than 4
    bytes counts as none.
  - The local row, if any, wins; otherwise the first answer's row. Answers
    whose session id differs from the chosen one are ignored.
  - Apply every log entry from every answer with `store.Log.Apply`
    (union by key; redelivery is preferred to loss), then write the row
    unless this node had one.
  - Then tell each peer that answered with a copy and is not a session
    replica (in the current ring *or* the full ring) to forget it,
    asynchronously.
- **Forget** (capability `sessions`): unless this node is itself a replica
  of that session (current or full ring), reset the session's log
  (`LogOp{Block, Reset: true}`) and delete the row key
  (`LogOp{Block, Key: sessKey}`) with `store.Log.Apply`.
- Tests: `TestClusterSessionFailover`, `TestClusterSessionMoveForgetsStaleCopy`,
  `TestClusterDrainOnSIGTERM` (resume elsewhere).

### 1.15 Reconcile after restore (`cluster_reconcile.go`)

Not rewritten. The core must provide what it uses: the `reconciling` flag
(set in Start, cleared by `reconcileAll`), `waitInRing` inputs (this node in
`getRingNodes()`, and "heard from a leader" i.e. `c.health.lastLeader != 0`
when failover is on, `c.fo == nil` otherwise), `c.nodes`, `n.supports`,
`n.callTimeout(method, req, resp, d)` for `Cluster.ReconcileTopics`,
`Cluster.Digests`, `Cluster.Reconcile`, `getFullRing`, `topicRingKey`,
`c.replicas`, `c.rebuildTTL`, `c.thisNodeName`, `rebuildTimeout` (30 s),
`n.handoffMu`, `c.dropHints`, and the transport must serve its three
handlers. Hints received during reconciliation behave as in 1.10. Tests:
`TestRestoreReconcilesOneRun`, `TestRestoreWholeClusterFromOneRun`,
`TestRestoreKeepsRevocationBetweenCheckpoints`, `TestRunbookWholeClusterRestore`,
`TestRestoreWholeClusterFromRunID`, `TestBackupOffsiteRestoreReplaysJournal`.

### 1.16 WebAuthn record sync (`cluster_webauthn.go`)

Not rewritten. It routes WebAuthn requests to the user's ring owner,
syncs copies from every live node, replicates snapshots, and backfills when
the ring signature changes. It needs from the core: `c.thisNodeName`,
`c.nodes`, `c.replicas`, `getRing()` (`Get`, `GetN`, `Signature`),
`getRingNodes()`, `n.callTimeout` for `Cluster.WebAuthn`,
`Cluster.FetchWebAuthn`, `Cluster.ReplicateWebAuthn`, `errRejected`,
`retryable`, `forwardRetryFor` (3 s), `forwardRetry` (100 ms),
`replicaAckTimeout` (1 s); and the transport must serve its three handlers.
The ring signature must change whenever the live set changes (it drives the
backfill and the challenge epoch, `service.go:146`). Tests:
`TestSyncWebAuthnRetriesUnreachableNode` (unit), the WebAuthn e2e tests, `server/internal/webauthn_test.go`.

### 1.17 Other cross-node calls served by the transport

Handlers defined outside the core that the transport must serve, with the
sender check over TLS:

| Handler | File | Caller |
| --- | --- | --- |
| `WebAuthn`, `FetchWebAuthn`, `ReplicateWebAuthn` | `cluster_webauthn.go:168, 545, 562` | `cluster_webauthn.go` |
| `Revocations` | `revocation.go:229` | `revocation.go:214` (to all peers on change, 1 s timeout; whole state to a peer on reconnect) |
| `StartedFrom` | `restore.go:52` | `restore.go:107-121`, before Start, one-shot, 2 s |
| `ReconcileTopics`, `Digests`, `Reconcile` | `cluster_reconcile.go:171, 193, 209` | `cluster_reconcile.go` |

Their request structs all carry a `Node string` sender field. Today they are
net/rpc methods on `*Cluster` registered under the service name `Cluster`,
and their callers name them `"Cluster.<Method>"`.

### 1.18 Health

`cluster_health.go` (not rewritten) computes readiness and metrics from core
state; `health.go:269-275` registers it as check `"cluster"`. Readiness, in
order: stopped → not ready "stopped"; leaving → "leaving the cluster";
rebuilding / reconciling → "catching up: ..."; this node not in the live set
→ "not in the ring yet (the ring has X of Y nodes)"; no failover → ready "in
the ring: X of Y nodes"; no leader heard yet → "no leader heard from yet";
last leader contact older than `heartbeat × (vote_after + 1)` → "no leader
for D"; else ready "in the ring: X of Y nodes; leader L, D ago".
`TestHealthCluster` expects detail containing `in the ring: 3 of 3 nodes`;
`TestClusterReadiness` checks each state. The core must keep these inputs
up to date (section 3.3).

### 1.19 Timing and limits (normative)

| What | Value | Asserted by / reason |
| --- | --- | --- |
| Peer reconnect interval | 200 ms | rejoin speed |
| Dial timeout | 1 s | |
| Forward retry window / interval (publish, subscribe, WebAuthn) | 3 s / 100 ms | `TestClusterRequestsDuringFailover`; longer than failure detection + rehash with sample settings |
| Replica ack wait (message and session) | 1 s | `TestClusterReliablePublishHungReplica`, `TestReplicationDelayLetsWaitersThrough` |
| Per-peer replication queue / batch | 4096 / 256 | |
| Hint handoff period | 5 s, `UNITDB_HANDOFF_INTERVAL` | `TestClusterReplicaRestartStoresOnce` |
| In-memory hints | 10,000 | |
| Seen-id set | 100,000 | |
| Session hint TTL | 24 h | |
| Fetch session wait | 1 s total | connect latency |
| Start resync wait | 3 s per peer | `TestClusterRestartedOwnerKeepsSubscriptions` |
| Rebalance retries | 10 × 200 ms | other nodes rehash a few heartbeats later |
| Rebuild: attempts / gap / call timeout | 20 / 500 ms / 30 s | also used by `cluster_ring.go`, `cluster_reconcile.go` |
| Drain | `drain_timeout`, default 10 s | k8s grace 30 s |
| Plain listener idle read timeout | 120 s today | open point 7.8 |
| Leader readiness staleness | `heartbeat × (vote_after+1)` | `cluster_health.go` |

## 2. Configuration

Parsed from `cluster_config` (`config.Config.Cluster json.RawMessage`,
`server/internal/config/config.go:99`), sample with comments in
`server/unitdb.conf:54-101`, `docker/config.template:43-56`. The JSON keys
are an external contract (configmaps in deploy repos use them) and must not
change.

| Key | Type | Default | Meaning |
| --- | --- | --- | --- |
| `node` | string | "" | this node's name; overridden by `-cluster_node`; empty = standalone |
| `nodes[]` | `{name, addr, tls_addr?}` | | every node, this one included; `addr` host:port of plain cluster traffic; `tls_addr` of mutual TLS |
| `replicas` | int | 2 (≤0 → 2) | nodes storing each message and session; 1 disables replication |
| `async_replication` | bool | false | express publishes and session writes don't wait for a replica |
| `rebuild_ttl` | duration string | 24h | expiry for copied messages whose expiry is unknown |
| `drain_timeout` | duration string | 10s | longest a drain takes |
| `ring_version` | int | 0 (latest) | highest ring version the leader may pick |
| `failover.enabled` | bool | false | membership/leader on; needs ≥3 nodes |
| `failover.heartbeat` | int ms | (sample 100) | base heartbeat period |
| `failover.vote_after` | int | (sample 8) | heartbeats without a leader before electing |
| `failover.node_fail_after` | int | (sample 16; e2e 16 or 50) | missed heartbeats before a node leaves the live set |
| `tls.ca_file`, `tls.cert_file`, `tls.key_file` | paths | | cluster CA and this node's certificate (must name the node); `tls_addr` required for this node |
| `tls.require` | bool | false | no plain listener and no plain dials |

Note: the `failover` key has no JSON tag today and is matched
case-insensitively by `encoding/json`; keep accepting `"failover"`.
`heartbeat`, `vote_after`, `node_fail_after` have no defaults today (zero
values); the new code should reject or default them sensibly (open point
7.9). Today the effective heartbeat is randomised to 0.75–1.25 × the setting;
the new design may choose its own jitter.

Flags (`server/main.go:41-50`): `-cluster_node` (name override; the k8s
manifest passes `$(POD_NAME)`), `-restored` (start at a checkpoint and
reconcile), `-db_path`.

Environment read by the cluster (all optional):

| Variable | Read in | Effect |
| --- | --- | --- |
| `UNITDB_HANDOFF_INTERVAL` | core | handoff period (Go duration) |
| `UNITDB_REPLICATION_DELAY` | core via `devDuration` (`build_kind.go:57`) | dev builds only; production refuses to start with it (`build_kind.go:26`) |
| `UNITDB_DELIVER_DELAY` | core via `devDuration` | as above |
| `UNITDB_CLUSTER_CAPS` | `cluster_caps.go` | run with fewer capabilities (`none` for none) |
| `UNITDB_RING_VERSIONS` | `cluster_ring.go` | run supporting fewer ring versions |

Deploy: `deploy/kubernetes/unitdb.yaml` (StatefulSet of 3; container port
`cluster` 12000, headless Service with `publishNotReadyAddresses: true` so
peers resolve before ready; `readinessProbe` on `/_readyz` every 5 s,
2 failures; `terminationGracePeriodSeconds: 30`, longer than
`drain_timeout`; node names are pod names, addresses
`unitdb-N.unitdb:12000`). `deploy/kubernetes/restore-node-job.yaml` and
`restore-test.yaml` use `-restored`. Other deployments use the same config
keys.

## 3. Seams

### 3.1 Symbols of the two files used elsewhere

Signatures as today; "nil-safe" means callers call it on a nil `*Cluster`
(standalone) and it must do nothing / return the standalone answer.

**Package-level and lifecycle**

| Symbol | Semantics | Callers |
| --- | --- | --- |
| `type Cluster` | the cluster; `Globals.Cluster *Cluster` | `globals.go:20`; nil checks at `main.go:144`, `service.go:154`, `health.go:269`, `metrics.go:153`, `monitor.go:147`, `keys.go:159`, `checkpoint.go:158,194`, `offsite.go:72`, `revocation.go:141`, `restore.go:81`, `cluster_webauthn.go:193,222,365`, `hdl_conn.go:475,1239`, `cluster_caps.go:109`, `cluster_tls.go:106` |
| `func ClusterInit(config json.RawMessage, self *string) int` | 1.2 | `server/main.go:110` |
| `func (c *Cluster) Start()` | 1.2 | `server/main.go:145` |
| `drain()` (nil-safe) | 1.2 | `service.go:338` |
| `shutdown()` (nil-safe, idempotent) | 1.2 | `service.go:370` |
| `thisNodeName string` field | this node's name | `checkpoint.go:159,195`, `offsite.go:73`, `restore.go:114`, `revocation.go:208`, `cluster_webauthn.go:138,459,508,571,669`, `cluster_reconcile.go:289,302,334,362`, `cluster_ring.go:214`, `cluster_tls.go:167`, `cluster_health.go:45` |
| `nodes map[string]*ClusterNode` field (peers only, not self) | | `keys.go:164`, `restore.go:86`, `revocation.go:200,204`, `cluster_webauthn.go:145,458,505,666`, `cluster_reconcile.go:178,284,308`, `cluster_ring.go:219`, `cluster_health.go:44,89-98` |

**Routing and delivery (called from `conn.go`, `hdl_conn.go`)**

| Symbol | Semantics | Callers |
| --- | --- | --- |
| `isRemoteTopic(contract uint32, topic string) bool` (nil-safe) | 1.7 | `hdl_conn.go:595` |
| `routeToTopic(msg lp.MessagePack, contract uint32, topic string, conn *_Conn) (bool, error)` | 1.7 | `hdl_conn.go:598` |
| `relayFromHolder(msg *utp.Relay, contract uint32, topic string, conn *_Conn) bool` | 1.7 | `hdl_conn.go:477` |
| `replicate(contract uint32, name, topic string, payload []byte, ttl string, wait bool)` (nil-safe) | 1.10 | `hdl_conn.go:617` |
| `waitsForReplica(reliable bool) bool` (nil-safe) | 1.10 | `hdl_conn.go:617` |
| `fetchSession(sessKey uint64)` (nil-safe) | 1.14 | `hdl_conn.go:183` |
| `holders(contract uint32, topic string) (bool, []string)` (nil-safe) | 1.9 | `conn.go:344` |
| `subscribeAt(name string, sub *utp.Subscription, conn *_Conn) error` | 1.9 | `conn.go:359` |
| `unsubscribeAt(name string, sub *utp.Subscription, conn *_Conn) error` | 1.9 | `conn.go:397` |
| `deliverRemote(map[*ClusterNode][]Delivery)` (nil-safe) | 1.8 | `conn.go:500` |
| `type Delivery struct{ConnID uid.LID; Message *message.Message; Reliable bool}` | 1.8 | `conn.go:473,486` |
| `connGone(conn *_Conn) error` (nil-safe) | 1.7 | `conn.go:671` |
| `type ClusterNode` | a peer; `_Conn.clnode *ClusterNode` marks a proxied session | `conn.go:64,115,485-486,512,657`; also `newRpcConn(conn interface{}, ...)` type-asserts its first argument to `*ClusterNode` (`conn.go:115`) |
| `(*ClusterNode).call(method string, req, resp interface{}) error` and `type ClusterResp{Message, Reliable, FromConnID, Msg ...}` with method `"Cluster.Proxy"` | proxied delivery to the client node | `conn.go:514` |
| `isWildcardTopic(topic string) bool` | 1.1 | `conn.go:360`, `hdl_conn.go:475` |
| `retryable(err error) bool` | 1.5 | `conn.go:264`, `cluster_webauthn.go:154` |
| `forwardRetryFor`, `forwardRetry` (durations) | 1.19 | `conn.go:258,266`, `cluster_webauthn.go:131,162` |
| `errRejected` (error) | 1.5 | `cluster_webauthn.go:152` |
| `replicaAckTimeout` (duration) | 1.19 | `cluster_webauthn.go:514,523,672`; `cluster_repl_test.go:53` |

**Peer access used by other cluster files**

| Symbol | Callers |
| --- | --- |
| `(*ClusterNode).callTimeout(method string, req, resp interface{}, d time.Duration) error` | `revocation.go:214`, `cluster_webauthn.go:150,468,514,523,672`, `cluster_reconcile.go:289,334,370`, `cluster_ring.go:259` |
| `(*ClusterNode).name` field | `restore.go:94-98`, `revocation.go:216`, `cluster_webauthn.go` (logs), `cluster_ring.go:265`, `cluster_health.go:97` (via map key) |
| `ClusterNode.address`, `ClusterNode.tlsAddress` fields | `cluster_tls.go:110-121` (`dial`) |
| `ClusterNode.caps peerCapabilities` field | `cluster_caps.go:149-195` |
| `ClusterNode.handoffMu sync.Mutex` | `cluster_reconcile.go:179-181` |
| `ClusterNode.repl chan replicaItem` (queue length metric) | `cluster_health.go:98`; tests `metrics_test.go:51-52`, `cluster_repl_test.go:39,44,47` |
| `type watchedConn` (closed-connection detector returned by `dial`) | `cluster_tls.go:105,121`; `cluster_node_test.go:151` |
| `getRing() *rh.Ring` | `cluster_webauthn.go:141,169,369,444,497,571,665`; `webauthn_sync_test.go:23` |
| `getRingNodes() []string` | `cluster_webauthn.go:457`, `cluster_reconcile.go:277,391`, `cluster_ring.go:208`, `cluster_health.go:43,76` |
| `rehash`, `ringMu`, `ring`, `ringNodes`, `ringVersion`, `ringTarget`, `fullRing`, `allNodes`, `clusterRing` fields | `cluster_ring.go:136,157-169,181-183,192`; `cluster_tls.go:166`; `cluster_health.go:77` |
| `replicas int`, `rebuildTTL time.Duration` | `cluster_webauthn.go:497`, `cluster_reconcile.go:160,256,309`, `cluster_ring.go:213,234` |
| `tls *clusterTLS` field | `cluster_caps.go:109`, `cluster_tls.go:110-113` |
| `stopped`, `leaving`, `rebuilding`, `reconciling` (`atomic.Bool`) | `cluster_health.go:34-40,78-81`, `cluster_tls.go:130`, `cluster_reconcile.go:272,417` |
| `fo *clusterFailover` with `heartBeat time.Duration`, `voteTimeout int` | `cluster_health.go:48,57`, `cluster_reconcile.go:391` |
| `health clusterHealth` field (type in `cluster_health.go`) | `cluster_health.go`, `cluster_reconcile.go:391` |
| `pending`, `pendingMu` (in-memory hints) | `cluster_health.go:84-86` |
| `dropHints(name string)` | `cluster_reconcile.go:180` |
| `topicRingKey(contract uint32, topic string) string` | `cluster_reconcile.go:160,308`, `cluster_ring.go:212` |
| `containsNode([]string, string) bool` | `cluster_health.go:45`, `cluster_reconcile.go:391`, `cluster_ring.go:219` |
| `type ReplicaEntry`, `type ReplicateReq`, method `"Cluster.Replicate"` | `cluster_ring.go:230,255,259` (`sendHistory` moves history through the replica receive path) |
| `rebuildTimeout`, `rebuildAttempts`, `rebuildRetry`, `replicationBatchSize` | `cluster_ring.go:250-262`, `cluster_reconcile.go:274,289,334,370` |
| `clusterHashReplicas` (160) | `cluster_ring.go:48`; mirrored by hand in `server/e2e/cluster_test.go:48` |
| `type ClusterPing`, `type ClusterPong` | `cluster_tls.go:191` (peerRPC.Ping); `cluster_ring_leader_test.go:18-19` |
| RPC handler methods on `*Cluster`: `Ping`, `Vote`, `Master`, `Proxy`, `Deliver`, `Replicate`, `RebuildTopics`, `RebuildHistory`, `FetchSession`, `ForgetSession`, `Resync` and their request/response types | wrapped one by one in `cluster_tls.go:184-315` (`peerRPC`); enumerated by `cluster_tls_senders_test.go` |

**Reverse seams** (the core calls these; they stay where they are):
`Globals.connCache.get/all/delete` (`conn_cache.go:45,62,55`);
`Globals.Service.newRpcConn` (`conn.go:103`); `_Conn.handler`
(`hdl_conn.go:106`), `deliver` (`conn.go:511`), `SendRawBytes`
(`conn.go:183`), `rehome` (`conn.go:320`), `unsubAll` (`conn.go:608`),
fields `connID`, `sessID`, `clientID`, `insecure`, `nodes`, `clnode`, `send`,
`pub`, `stop`, `subs`; `lp.Encode`; store APIs of section 4;
`cluster_caps.go` helpers; `cluster_ring.go` (`newRing`, `initialRingVersion`,
`logRingVersion`, `chooseRingVersion`, `adoptRingVersion`,
`getRingVersion`, `getFullRing`, `supportsRingVersion`); `cluster_tls.go`
(`loadClusterTLS`, `dial`, `serveTLS`, `serverConfig`); `cluster_health.go`
(`clusterHealth.leaderSeen`); `cluster_reconcile.go` (`restoreRequested`,
`restoredPath`, `markRestored`, `reconcileAll`); `revocation.go`
(`pushRevocations`); `build_kind.go` (`devDuration`); `net/listener`
(`listener.New`).

### 3.2 How callers change

The cheapest path keeps every name in 3.1 with the same signature and
meaning; then only `cluster_tls.go` (if the per-method `peerRPC` is replaced
by a connection-level check) and `cluster_caps.go` (if the "lacks capability"
error shape changes) need edits, plus the tests in 3.4. If the implementer
renames or restructures:

- `conn.go:514` should call a core function (e.g. "deliver to the client
  node of this proxied conn") instead of naming a method string and struct.
- `restore.go:107-121` dials and calls `StartedFrom` by hand through net/rpc;
  give it a core one-shot call instead.
- `cluster_webauthn.go`, `revocation.go`, `cluster_reconcile.go`,
  `cluster_ring.go` call peers as `n.callTimeout("Cluster.<Method>", ...)`;
  keep a call-by-name API or give them typed calls.
- `cluster_health.go`, `cluster_ring.go`, `cluster_reconcile.go`,
  `keys.go` read fields directly; accessors are fine if those files are
  updated in the same change.

### 3.3 State other files read

The core must maintain, and expose concurrently safe reads of: this node's
name; the peer set; the replication factor and `rebuild_ttl`; the current
ring, live set and full ring; the ring version and `clusterRing`; flags
stopped/leaving/rebuilding/reconciling; failover settings (heartbeat,
vote_after) or nil when off; last leader contact (`health`); in-memory hint
count; per-peer replication queue length; per-peer capabilities; the TLS
setup.

### 3.4 Tests that construct or poke core internals

These may be rewritten; each must still check the behaviour named.

| Test | Pokes | Behaviour to keep checking |
| --- | --- | --- |
| `cluster_node_test.go` `TestClusterCallAfterRestart` | builds `ClusterNode{name,address,connected,done}`, `dial`, `endpoint`, `conn`, `call`; a net/rpc test server | first call after the peer restarted while idle reaches it (2 calls received) |
| `TestClusterCallNotRepeated` | `watchedConn`, `endpoint`, `call` | a call that failed after being sent is not sent again (peer saw it once) |
| `TestClusterErrorAnswerKeepsConnection` | `call`, `missingMethod` | an answered "no such method" error keeps the connection; an in-flight call and later calls succeed |
| `TestCallErrorKinds` | `connectionFailed`, `notSent` | error classification (1.5) |
| `cluster_repl_test.go` `TestReplicationDelayLetsWaitersThrough` | `replicationDelay`, `n.repl`, `n.replDone`, `replicaItem{entry,done}`, `replicateLoop`, net/rpc `Cluster.Replicate` server | a waited item ends an async batch's delay; answered within 1 s; both items reach the replica |
| `cluster_ring_leader_test.go` `TestNewLeaderMovesHistory` | `Cluster{thisNodeName, ringVersion, allNodes, nodes, replicas, fo: clusterFailover{leader, heartBeat, nodeFailCountLimit}}`, `fullRing`, `rehash(nil)`, `sendPings`, `clusterRing`; fake follower answering `Ping` with `ClusterPong{RingVersion}` | the first-round rule of 1.4 |
| `cluster_ring_test.go` `TestChooseRingVersion` | `Cluster{ringTarget, ringVersion}`, `ClusterNode{}` + `setCapabilities` | `cluster_ring.go` logic (needs those two fields or equivalents) |
| `cluster_caps_test.go` `TestNodeCapabilities` | `ClusterNode{name}`, `rpc.ServerError` | capability learning (1.6) |
| `cluster_tls_senders_test.go` `TestPeerRPCChecksSenders` | `peerRPC`, reflection over `*Cluster` methods | every sender-naming call over TLS is checked |
| `hint_test.go` `TestHintKeptWhenStoreFails` | `Cluster{}`, `putHint`, `hint`, `pending`, `storePendingHints` | 1.12 in-memory fallback |
| `webauthn_sync_test.go` `TestSyncWebAuthnRetriesUnreachableNode` | `Cluster{thisNodeName, nodes, ringNodes}`, `ring`, `newRing` | a user is not marked synced while a ring member's copy is missing |
| `metrics_test.go` `TestClusterMetrics` | `Cluster{thisNodeName, nodes{name, repl}}`, `ringNodes`, `health.leaderSeen` | metric lines of section 6 |
| `health_test.go` `TestClusterReadiness` | `Cluster{thisNodeName, nodes}`, `fo{heartBeat, voteTimeout}`, `ringNodes`, `health`, `rebuilding`, `leaving` | readiness states (1.18) |

E2E tests that speak the current wire protocol directly (rewrite if the
transport changes):

- `server/e2e/release2_test.go` `TestReleaseTwoClusterTLS`: dials a node's
  TLS address with net/rpc and calls `Cluster.Resync` with a gob struct
  `{Node string}` to check: no client cert → refused; cert from another CA →
  refused; cert naming no configured node → refused; node two's cert naming
  `three` as sender → error containing `names`; naming itself → accepted.
  Also delivery over TLS for all/some nodes.
- `server/e2e/cluster_test.go` `TestClusterRefusesOldPeer`: a fake net/rpc
  peer with only `Ping`; expects the node to exit logging `runs a version
  from before replication`. See 7.3.
- `server/e2e/cluster_test.go:48,160-180`: computes topic owners with
  `rh.NewRing(160, nil)` and the key `"%d/%s"`; ring placement must not
  change (4.1).

## 4. Persisted state and data formats

Defined by the store (`server/internal/store/store.go`,
`namespaces.go`); the core must read and write it exactly so.

### 4.1 Ring placement

Keys and ring versions of 1.1/1.4 decide which node holds what on disk.
They must be unchanged: topic key `"<contract>/<topic>"` (topic without
options, i.e. without `?...`), session key `"session/<id>"`, WebAuthn key in
`cluster_webauthn.go`; ring version specs in `cluster_ring.go:42-49`; hashing
in `pkg/hash/ringhash.go`. `GetN` order defines "first holder" for rebuild,
relay order, and synchronous replica choice. Documented in
`docs/cluster-data-sync.md` and `docs/message-log-replication.md`.

### 4.2 Hints

- Store: `store.Hint.NewID()`, `Put(node, id, payload, ttl)`, `Get(node)`
  (up to the store's query limit), `Delete(node, id)` (`store.go:395-426`):
  records under contract 0, topic `$sys.hint.hints.n<hash(node)>`.
- Payload: a Go `encoding/gob` encoding of one record with these fields
  (gob matches by field name and type, so a new type must use the same
  names and compatible types to read hints written before the upgrade):
  - `ID []byte`: the store id the hint was put under (used to delete it);
  - `Entry`: a record `{ID string; Contract uint32; Topic string;
    Payload []byte; Ttl string; ExpiresAt int64}`: a replicated message
    (`Topic` as stored, with options; `ExpiresAt` unix seconds, 0 = never;
    `ID` the replication id, may be empty);
  - `Op *store.LogOp` (`{Block uint32; Key uint64; Raw []byte; Reset bool}`):
    non-nil for a session hint, with `Raw` always nil (state is read at
    handoff).
  A hint is a message hint iff `Op` is nil.
- TTL string: the message's TTL (seconds or a Go duration, as
  `store.ExpiresAt` reads it) or `"24h"` for session hints.

### 4.3 Replica copies, seen ids, topic index

- `store.Message.PutReplica(contract, topic, payload, expiresAt)` stores a
  copy under `$sys.replica.<topic>` in the topic's contract, with a 12-byte
  expiry header, and indexes the topic (`store.go:307-322`,
  `namespaces.go:29-56`). Relays read both places
  (`store.Message.GetAll`).
- `store.Message.Topics()` lists indexed topics as `store.TopicRef{Contract,
  Topic}`; `store.Message.History(contract, topic)` returns
  `[]store.HistoryEntry{Payload, ExpiresAt, Known}` (expired skipped).
- `store.Seen.Put(id, expiresAt)` / `Recent(n)` (`store.go:428-457`): the
  replication ids a replica stored, kept as long as the message up to the
  store's maximum, newest first. Ids are opaque strings; new ones only need
  to be unique and never equal an old one.
- `store.WasEmpty()` (`store.go:102-106`).

### 4.4 Session log and rows

- Log entries keyed `messageID<<32 | sessionID`; a session's keys are those
  whose low 32 bits equal its id (`store.Log.Keys`).
- Session row under the session key (`uint64`, computed by
  `hdl_conn.go` `sessionKey`); bytes 0-3 are the session id little-endian,
  bytes 4-11 the owner key (`hdl_conn.go:190`).
- `store.LogOp` semantics for `store.Log.Apply`: `Reset` deletes every key of
  `Block`; `Raw == nil` deletes `Key`; else puts `Raw` under `Key`. `Apply`
  does not trigger `OnLogChange` (no echo).

### 4.5 Other

- `checkpoint.json` → `restored-from.json` rename after a successful
  reconcile (`cluster_reconcile.go` `markRestored`).
- WebAuthn records (`store.Users.Snapshot/Merge`), security state
  (`store.Security`) are handled by their own files.
- Subscriptions are stored per node in the store with a payload holding the
  subscriber's connection id (`conn.go:410-416`); a proxied subscriber is
  recorded under the client node's connection id.

## 5. Test oracle

### 5.1 Unit tests (`server/internal`)

Run: `go test -count=1 ./server/internal/` (≈15 s wall, no external
services). Cluster-relevant: the 13 tests in 3.4, plus `build_kind_test.go`
(dev-only env vars refused in production), `logstore_test.go`
(`TestLogDeleteAcrossTimeBlocks`), `webauthn_test.go`, and in
`pkg/hash` `TestRingGetN` (`go test ./pkg/hash/`).

**Baseline, 2026-10-08, this worktree, unmodified**: `go build ./...` OK
(8 s); `go test -count=1 ./server/internal/` **ok** in 13.4 s (15.5 s wall).

### 5.2 E2E tests (`server/e2e`)

- Harness: `harness_test.go` builds the server once per run with
  `go build -tags dev` (needed for the delay variables), `-race` when the
  tests run with `-race`; `UNITDB_SERVER_DIR` overrides the source dir.
  Each node is a real process with free ports, its own `db_path`, and
  `cluster_config` with failover on (heartbeat 100, vote_after 8,
  node_fail_after 16 unless set). `-short` skips everything. A watchdog
  kills servers if the test binary dies. Server stdout/stderr are captured;
  tests parse logs (section 6).
- Tests that need an external lab or service skip without it.
- Run the cluster set:
  `go test ./server/e2e -count=1 -timeout 40m -run 'TestCluster|TestHealthCluster|TestReleaseTwoClusterTLS|TestReleaseThreeRequireTLS|TestServiceIDsCluster|TestRestore|TestRunbook|TestBackupRun|TestBackupOffsite'`.

| File | Tests | What they check |
| --- | --- | --- |
| `cluster_test.go` | `ElectsOneLeader`, `Delivery`, `RoutesByTopic`, `NodeFailure`, `NodeRejoin`, `ReliableDelivery`, `WildcardDelivery`, `Relay`, `FailoverKeepsSubscriptions`, `ReplicatedRelay`, `SessionFailover`, `ReliablePublishSurvivesCrash`, `ReliablePublishHungReplica`, `RebuildEmptyNode`, `SessionHandoff`, `SessionMoveForgetsStaleCopy`, `RequestsDuringFailover`, `SubscribeOutlastsFailureDetection`, `DeliveryFanOut`, `PartitionKeepsSubscriptions`, `FrozenNodeIsFailedOver`, `ReplicaRestartStoresOnce`, `MixedCapabilities`, `DrainOnSIGTERM`, `RingVersionSwitch`, `RingSwitchMovesHistory`, `RefusesOldPeer`, `RestartedOwnerKeepsSubscriptions` (all `TestCluster…`) | sections 1.3-1.14; each test's doc comment states its claim |
| `durability_test.go` | `TestClusterExpressPublishSurvivesCrash`, `TestClusterSessionSurvivesCrash`, `TestClusterAsyncReplication` | sync vs async replication, crash right after acks |
| `reconcile_test.go` | `TestRestoreReconcilesOneRun`, `TestClusterRebuildFromRestartedNode`, `TestClusterGracefulRestartRejoins` | reconcile; rebuild from a restarted peer; rejoin after graceful restarts of leader and follower |
| `restore_cluster_test.go` | `TestRestoreLostNodeStartsEmpty`, `TestRestoreCheckpointRefusedInRunningCluster`, `TestRestoreWholeClusterFromOneRun`, `TestRestoreKeepsRevocationBetweenCheckpoints` | start guards (`StartedFrom` before Start), rebuild, readiness while catching up |
| `backup_run_test.go`, `offsite_test.go`, `runbook_test.go`, `restore_test_run_test.go` | whole-cluster backup/restore runs | |
| `health_test.go` | `TestHealthCluster` | `/_readyz`, `/_status` cluster check |
| `release2_test.go` | `TestReleaseTwoClusterTLS` | 1.5 TLS (speaks net/rpc, 3.4) |
| `release3_test.go` | `TestReleaseThreeRequireTLS` | `tls.require`: delivers, no plain cluster port |
| `service_ids_test.go` | `TestServiceIDsCluster` | forwarded `Insecure` taken only from `service`-capable peers |
| `hardening_test.go` | allow_insecure refused in a cluster | `service.go:154` |

`scalability_test.go` and `performance_test.go` run standalone servers;
they do not exercise the cluster.

**Baseline durations** (this machine, unmodified tree; see the result block
below): a 3-node cluster test takes 2-25 s; most are 5-15 s.

**E2E baseline, 2026-10-08, unmodified tree, macOS dev machine**: the
command above (minus `TestRunbook|TestBackupRun|TestBackupOffsite`,
plus three tests of a service this repository doesn't have) ran 46 tests: 43
passed, 3 skipped, 0 failed; `ok` in 435 s (7 min 17 s wall, serial).
Individual times: most 3-16 s; longest `TestClusterRebuildEmptyNode` 32.7 s,
`TestRestoreReconcilesOneRun` 23.6 s, `TestRestoreWholeClusterFromRunID`
23.9 s, `TestClusterReplicatedRelay` 23.2 s; shortest
`TestClusterRefusesOldPeer` 0.03 s, `TestClusterDeliveryFanOut` 1.6 s,
`TestClusterRestartedOwnerKeepsSubscriptions` 2.0 s.

## 6. Observable logs and metrics

### 6.1 Log lines tests or operators rely on

| Line (substring / regex) | Source | Relied on by |
| --- | --- | --- |
| `Elected myself as a new leader` | core | `server/e2e/cluster_test.go:183` (`waitLeader`) |
| `leader (?:set to )?'([a-z]+)'(?: elected)?` (lines containing `wrong leader` ignored) | core | `cluster_test.go:184-196` |
| `cluster: ring version N` | `cluster_ring.go` `logRingVersion` (core must call it at init) | `cluster_test.go:2222` |
| `cluster: moved history for ring version N` | `cluster_ring.go` | `cluster_test.go:2392`, `cluster_ring_leader_test.go` |
| `runs a version from before replication` | core (`checkPeers`) | `TestClusterRefusesOldPeer` (7.3) |
| `cluster.webauthnStats` / `WebAuthn counters` | `cluster_webauthn.go` | operators |
| `security counters` with `peers_without_tls` | `keys.go:169` (reads `c.nodes`) | operators, `docs/security-review.md` finding 3 |
| `reconciled after a restore` | `cluster_reconcile.go` | operators, `docs/backup-restore.md` |
| `rebuilt from <node>` (topics, messages), `handed off to <node>`, `no replica took the message`, `left the cluster` | core | operators (`docs/message-log-replication.md`); not parsed by tests |

The leader-related lines go through the standard library logger today
(stderr); tests read stdout+stderr together, so either is fine.

### 6.2 Metrics (`/_metrics` on the monitor port)

Written by `cluster_health.go` `writeMetrics` and
`cluster_reconcile.go` `writeReconcileMetrics`, called from
`metrics.go:153`; the core supplies the values:

| Metric | Type | Value |
| --- | --- | --- |
| `unitdb_cluster_nodes` | gauge | configured nodes |
| `unitdb_cluster_members` | gauge | live set size |
| `unitdb_cluster_ring_version` | gauge | `clusterRing` |
| `unitdb_cluster_rebuilding` | gauge | 0/1 |
| `unitdb_cluster_reconciling` | gauge | 0/1 |
| `unitdb_cluster_leaving` | gauge | 0/1 |
| `unitdb_cluster_leader_age_seconds` | gauge | since last leader contact (absent before any) |
| `unitdb_cluster_pending_hints` | gauge | in-memory hints |
| `unitdb_replication_queue{peer="<name>"}` | gauge | queued replication items per peer |
| `unitdb_cluster_peers_without_tls` | gauge | peers not advertising `tls` (or unknown) |
| `unitdb_cluster_term` | gauge | highest term seen (0 without failover) |
| `unitdb_cluster_is_leader` | gauge | 0/1 |
| `unitdb_cluster_elections_total{result="won\|lost\|prevote_refused\|stepped_down"}` | counter | `runElection` outcomes; `stepped_down` when this node stops leading (later term, another leader, no majority for a lease, resign) |
| `unitdb_cluster_peer_up{peer="<name>"}` | gauge | connection to the peer up, 0/1 |
| `unitdb_cluster_peer_calls_failed_total{peer="<name>"}` | counter | `callTimeout`/`goCall` transport failures (not sent, timeout, lost; not the peer's own errors) |
| `unitdb_cluster_forward_timeouts_total` | counter | forwards that hit `forwardTimeout` (7.6) |
| `unitdb_reconcile_*` | counters/gauge | reconcile stats |

`TestClusterMetrics` checks `unitdb_cluster_nodes 3`,
`unitdb_cluster_members 2`, `unitdb_replication_queue{peer="b"} 0`, the
same for `c`, `unitdb_cluster_peers_without_tls 2`, and the presence of
`unitdb_cluster_leader_age_seconds`, plus the zero-valued term, election and
peer series; `TestClusterElectionMetrics` checks them after a won election, a
step-down, a refused pre-vote and failed calls. `docs/backup-restore.md:274` and
`backup-restore-plan.md:239` name the reconcile metrics. `/varz` includes
`WebAuthnStats()` when clustered (`monitor.go:147`). No dashboard in this
repo reads other cluster metrics.

## 7. Open points for the implementer

7.1 **Transport.** Keep Go's `net/rpc` + `encoding/gob` (both standard
library, not GPL) with a new structure, or choose another framing. Keeping
net/rpc keeps `cluster_caps.go` (`missingMethod` parses
`rpc.ServerError`), `cluster_tls.go` (`peerRPC`, `rpc.NewServer`),
`restore.go:107-121`, and the net/rpc-speaking tests (3.4) working
unchanged. Changing it means updating those, and choosing how an answered
"capability missing" error is represented. Either way, consider binding the
TLS peer identity to the connection and checking the sender once for every
call instead of per-method wrappers (today `Proxy` has no sender field and
no check).

7.2 **Proxy delivery API.** Replace the method-name call in `conn.go:514`
with a core function, and decide whether proxied sessions keep
`_Conn.clnode *ClusterNode` (used by `conn.go` as "is proxied" and as the
fan-out grouping key) or get a narrower handle.

7.3 **Pre-replication peer check.** With all nodes restarted together,
refusing to start next to a `90d45ce` node is no longer needed. Decide to
drop it (and `TestClusterRefusesOldPeer`, and the paragraph in
`docs/rolling-deploys.md`), or keep an equivalent "refuse to start next to an
incompatible peer" check, which with a new wire format would need some
version handshake.

7.4 **Capabilities.** Keep the capability model (rolling deploys of future
builds rely on it; `TestClusterMixedCapabilities`, `TestServiceIDsCluster`,
the ring version tests use the env overrides). Decide the protocol version
number the new transport advertises (`clusterProtocolVersion` in
`cluster_caps.go`, currently 2).

7.5 **Election protocol.** Any protocol meeting 1.3. Decide behaviour in a
2-of-3 partition (the majority side elects; the minority must not, and its
readiness then fails) and with an even split. Decide whether followers also
probe each other or rely on the leader only.

7.6 **Forward timeout.** Forwarded requests have no timeout today, so a
client's publish to a frozen owner blocks until the owner is failed over or
resumes. Adding one is allowed, but a timed-out forward has an unknown
outcome and must not be retried automatically (1.5).

7.7 **Connection id collisions.** Proxied sessions are keyed in
`Globals.connCache` by the *client node's* `uid.LID`, which is a
per-process counter seeded from the clock, in the same map as the owner's
own client connections. A collision is unlikely but possible. Fixing it
(e.g. keying by node + id) touches `conn.go` (`newRpcConn`, subscription
payloads at `conn.go:410-416`, `publish` lookups); decide whether to do it
now.

7.8 **Idle connections.** The plain listener drops a connection idle for
120 s; the dialer then redials on the next call. Decide keepalives /
heartbeats on the new transport so that the first call after idleness
neither fails nor is lost (1.5).

7.9 **Config validation.** `failover.heartbeat`, `vote_after`,
`node_fail_after` have no defaults; zero values make no sense. Choose
defaults (the sample's 100/8/16) or fail at Init. Same for
`failover.enabled` with fewer than 3 nodes (today: logged, failover off).

7.10 **Hint format migration.** Either keep reading the gob record of 4.2
forever, or read it once at start and rewrite each hint in a new format.
Hints are TTL-bounded (message TTL; 24 h for session hints), so a
read-old/write-new period of one release suffices.

7.11 **`ClusterInit` return value.** The worker id is unused by
`main.go`; keep or drop it.

7.12 **Docs.** `docs/cluster-data-sync.md` ("Membership", diagrams),
`docs/message-log-replication.md`, `docs/rolling-deploys.md` and
`docs/security-review.md` cite `cluster.go` line numbers, method names and
the old election mechanism; update them to the new implementation once it
lands.

## 8. Decisions (implementation, 2026-10-08)

The core is now `cluster_core.go` (config, lifecycle, ring state),
`cluster_transport.go` (peers, calls, serving), `cluster_membership.go`
(election, failure detection, live set), `cluster_routing.go` (forwarding,
stand-ins, delivery, rebalance), `cluster_replication.go` (replication,
seen ids, hints, handoff) and `cluster_sessions.go` (rebuild, session
fetch/forget), over a new package `server/internal/peerwire` (framing).

- **7.1 Transport.** `net/rpc` is gone. `peerwire` frames calls over one
  TCP or TLS connection per direction (`uint32` length, kind, `uint64` call
  id, payload; bodies are gob), multiplexed by call id, with a hello
  exchange (magic, protocol, `From`, `To`, incarnation, capabilities) before
  any call. Answered errors are `*peerwire.RemoteError{Code, Message}`;
  `CodeNoMethod` means "lacks the method or capability" (`errCapabilityOff`,
  `missingMethod` adapted). Transport failures are
  `*peerwire.CallError{Sent}`: a frame not written whole is never processed,
  so any write failure is "not sent". **Sender check**: the accepting node
  binds each connection to one node, the certificate's node over TLS (the
  hello's `From` must match it, or the connection is refused with
  "... names X as its sender"), the hello's `From` on the plain listener
  (which must be a configured peer, and the hello's `To` this node). Every
  request type carries `Node`, and the dispatcher refuses, for every
  method, a request whose `Node` is not the connection's node, before
  handling it. This covers what `Proxy` used to skip: its replacements
  (`ToClient`, `Deliver`) are checked like the rest, and the client's node
  takes them only for a connection whose requests were forwarded to the
  sender. `peerRPC` and its per-method wrappers are removed;
  `TestPeerCallsCheckSender` checks every served method.
- **7.2 Proxy delivery.** `conn.go` calls `Globals.Cluster.proxyDeliver`.
  Stand-ins keep `_Conn.clnode` (the fan-out grouping key) and gain
  `_Conn.remoteID` (the connection id on the client's node).
- **7.3 Old-build check.** Kept as a protocol check: the hello carries the
  protocol version (`clusterProtocolVersion` = 3, `minPeerProtocol` = 3).
  While `Start` makes its first dial to each peer, a peer answering with an
  older protocol, or refusing this node's, stops the node with
  "node X runs an incompatible cluster protocol". A pre-rewrite (net/rpc)
  node can't be told from a dead one (it just drops the hello); the
  all-at-once upgrade is what excludes it. `TestClusterRefusesOldPeer` is
  replaced by `TestClusterRefusesIncompatiblePeer` (a peer whose hello says
  protocol 2).
- **7.4 Capabilities.** Model unchanged. Capabilities also travel in each
  hello, so even without failover a node knows its peers' (e.g. `service`
  for the forwarded `Insecure` flag) from the first connection.
- **7.5 Election.** Terms; pre-vote then vote; one vote per term; a
  majority of *configured* nodes elects (a 2-of-3 side elects, the
  minority does not and its readiness fails; an even split elects no one).
  A node that heard from a leader within `heartbeat × vote_after` votes for
  no other (no disruption by a node returning from a freeze). The leader
  probes every peer each heartbeat (timeout one heartbeat), steps down
  after `vote_after` heartbeats without a majority or on a later term.
  Followers rely on the leader only; they do not probe each other.
- **Leaving.** A draining node calls `Leave` on every peer: each drops it
  from its live set at once and keeps it out until a hello shows a new
  incarnation (this also holds without failover). A draining leader steps
  down and its successor (first remaining live node by name) elects itself
  one heartbeat later.
- **7.6 Forward timeout.** 5 s per forwarded request (`forwardTimeout`). A
  timed-out forward is a sent failure, so not retried.
- **7.7 Connection ids.** Stand-ins are keyed by (client node, client
  connection id) in the core and get their own connection id from this
  process's counter (`uid.NewLID`), so they can't clash in `connCache`.
  Stored subscriptions record the stand-in's id; deliveries carry the
  client node's id.
- **7.8 Idle connections.** The dialer pings every 15 s; either end drops a
  connection silent for 45 s; frame writes time out after 10 s. A call
  finding its connection down dials once, synchronously, before sending.
- **7.9 Config.** `heartbeat`, `vote_after`, `node_fail_after` default to
  100 ms / 8 / 16 when unset or ≤ 0; failover with fewer than 3 nodes is
  logged and off. A node not in `nodes`, or a name twice, is fatal.
- **7.10 Hints.** The stored gob record is kept as is (`ID`, `Entry`,
  `Op`); no migration.
- **7.11** `ClusterInit` still returns the worker id.
- **Replication ids** are `r3:<node>:<incarnation>:<seq>`, never equal to
  an id of the old format.
- **Ring keys.** `topicRingKey` drops `?options` from the topic, as 1.1
  says; callers already passed names without options.
