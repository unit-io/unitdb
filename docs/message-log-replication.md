# Message log replication

How stored messages (relay history) and session logs are kept on more than one
node, so that they survive a node failing or losing its disk. The code is in
`server/internal/cluster.go` and `server/internal/store/store.go`.

Each key has a **replica set**: its owner and the next `replicas − 1` distinct
nodes on the ring (`Ring.GetN`). `replicas` is set in `cluster_config` and
defaults to 2; 1 turns replication off. When an owner dies, the new owner is
normally its first replica, so it already has the data.

| Log | Store | Replica set |
| --- | --- | --- |
| Message store (relay history) | unitdb `db`, with TTL | `ring("<contract>/<topic>")` |
| Session log (deliveries awaiting RECEIVE / COMPLETE) and session row | memdb, keyed `msgID<<32 \| sessID`, and by session key | `ring("session/<sessID>")` |

```
MESSAGE STORE
 publisher's node ──publish──▶ topic owner ──Replicate (batched, queued)──▶ replica
                               stores, acks          same message, its own id
                               │ replica down: hint on the owner
                               └──────────────────── replayed on resync ──▶ replica
 relay, any node ──▶ topic owner, else a replica

SESSION LOG
 client's node ──every change (put, delete, reset)──▶ session replicas
 new node ◀──FetchSession: row + log── every other node, merged by key
```

## Message store

- **Replication.** After storing a message, the owner queues a copy for each
  other replica; a goroutine per node sends them in batches. A publish first
  waits for one replica, the first in ring order to take it, before it is
  acknowledged, up to 1 s per replica; the others get it from the queue. With
  `async_replication` set in `cluster_config`, only **reliable and batch
  publishes** wait, and express publishes are acknowledged at once.
- **Replica copies** are stored under the replica's own id, apart from the
  messages the node owns: under `$sys.replica.<topic>` in the contract's
  namespace, a topic no client can address. A unitdb id carries the sequence
  of the store that made it, so writing the owner's id on a replica could
  overwrite an unrelated message there. Up to v0.6.0 they were kept under the
  topic itself in the contract XOR a fixed id, which another contract could
  be; v0.7.0 moves them when it opens such a store (see
  [the store's own records](#the-stores-own-records)).
- **Each replicated message has an id** (origin node, start time, sequence).
  A replica records the ids it stored, for the message's TTL up to 24 h, loads
  the newest 100,000 when it starts, and skips a message it stored before: a
  hint for a batch it stored but did not answer in time.
- **Relays** go to the topic's owner, which answers from both its stores. A
  message is primary on one node only, so no relay returns it twice.
- **Expiry.** Stored messages start with a 12-byte header holding their expiry
  (`envelopeMagic`), as unitdb gives back neither a message's id nor its
  expiry. Relays strip it, and skip expired messages the store has not
  removed yet.

### Hints

A copy that cannot reach a node of its replica set is kept as a **hint**, under
`$sys.hint.hints.n<hash of the node's name>` in contract 0, on
the storing node: when the replica's queue is full, when it rejects a batch,
when it times out, or when it should be a replica but is not live (the replica
set here comes from every configured node). Hints are handed to the node when
it reconnects or rejoins, and every 5 s, then deleted. A hint the store does
not take is kept in memory, up to 10,000, and stored again later.

### Rebuilding a node with an empty store

A node that starts with an empty store (a new node, or one that lost its disk)
asks each other node for the topics it should hold whose messages that node is
the first live holder of (`RebuildTopics`), and copies their history into its
replica store with its remaining TTL (`RebuildHistory`). Messages stored before
the expiry header get `rebuild_ttl` (default 24h). While it rebuilds, it sends
relays to another replica, and the others drop their hints for it: the rebuild
copies those messages too. Each node indexes the topics it stores messages for,
since unitdb cannot list them.

## Session log

- Every change to a session's log or row goes, in order, to the session's
  replicas through the same queue, and they apply it to their own memdb under
  the same key: resuming on a replica works like a local resume.
- A change that stores something, such as a message logged for a client, is
  made only once one replica has stored it, up to 1 s: the change waits in the
  queue with the others, so that it cannot overtake an older deletion of the
  same key. Deletions do not wait: one a replica misses redelivers a message
  at worst. With `async_replication` set, no change waits.
- A change a replica missed is kept as a hint naming the key, and the handoff
  sends the key's state then, so that hints need no order.
- When a client resumes a session, its node gets every other node's copy
  (1 s timeout) and merges their logs by key before resuming. Mode 1 is
  at-least-once, so a copy that missed completions redelivers rather than
  loses. The nodes that are not the session's replicas then drop their copies,
  which would go stale.
- memdb keeps a version of a key per time block it was written in; the store
  deletes every version, and lists each key once.

## The store's own records

The store keeps what it needs for itself under `$sys` topics, which no
client request can address (`security.IsReserved`): a contract's
subscriptions and replicas in the contract's namespace, under
`$sys.sub.<topic>` and `$sys.replica.<topic>`; the node's own records in
contract 0, which `uid.NewContract` never draws: hints
(`$sys.hint.hints.n<hash>`), the topic index (`$sys.index.topics`), the ids of
replicated messages (`$sys.seen.seen`) and the security state
(`$sys.security.state`). See `server/internal/store/namespaces.go`.

Up to v0.6.0 they were kept under the contract XOR a fixed id (subscriptions,
replicas), or under a fixed id (the rest), so two contracts could share a
namespace, and a contract could be drawn as a fixed id. A v0.7.0 node moves
what such a store holds when it opens it, before it serves
(`server/internal/store/migrate.go`, `server/internal/hints_migrate.go`):

- the old topic index and, for each topic in it, its replicas; then the
  replicated messages' ids. The hints for each node of the cluster, rewritten
  with the id they are stored under now. The security state, merged into the
  node's and written once.
- Each record is copied, the copies flushed to the store's log, and then the
  old record deleted (`unitdb.DB.GetWithIDs` gives each record's id). A crash
  in between leaves both, and the next start moves the rest: a record whose
  copy is there already, the same bytes, is deleted without being copied
  again. An old index entry is deleted only once its topic's replicas have
  moved.
- A copy keeps the time in its id, so a relay of the last hour finds it as
  before, and its expiry; one that expired is not copied. A replicated
  message's id is kept for 24 h, the longest.
- Where a contract B is another contract A XOR the old replica id, and B has
  messages of its own on a topic A has replicas of, the two were kept
  together and can't be told apart: they are left where they are, B's
  namespace, and logged. A's owner still has A's messages; B still reads
  what it read before, until those replicas expire.
- Subscriptions are not moved: they are of connections the restart closed,
  and are made again as clients and the other nodes (`Resync`) subscribe.
- The move runs whenever an old namespace holds records: at the first start
  of v0.7.0, and again after a rollback to v0.6.0 wrote some there.

Nothing on the wire changes: nodes send each other contracts, topics and
records, never where a node stores them.

## Encryption at rest

With `encrypt_at_rest` on, the default since v0.7.0, a node seals each record as the store writes it
and opens it as the store reads it (`server/internal/store/sealing.go`), so
everything above the store, replication and handoff included, sees opened
records: a replica, a hint, a session's log or row and a rebuild's history
are sent opened, and the node that stores them seals them, or not, as it is
set to. Nodes need no part of each other's at-rest state but the shared
keyring, and a cluster may mix nodes with it on and off. The topic index is read at start with the sealing set, so a sealed index
lists its topics for a rebuild.

## Limits

- **A rebuilt node does not get its topics' older history** for topics that
  got no new message since the upgrade that added the topic index.
- **A write no replica took within 1 s can be lost** if its node crashes
  before a replica gets it: it is acknowledged anyway, so that a slow replica
  does not stop the cluster. With `async_replication` set, any express
  publish or session change acknowledged just before a crash can be lost.
- **A crash between storing a replica and recording its id** can store it
  twice.
- **Wildcard relays return nothing**: the storage engine does not match
  wildcard queries.

## Tests

In `server/e2e/cluster_test.go`: `TestClusterReplicatedRelay`,
`TestClusterRelay`, `TestClusterReliablePublishSurvivesCrash`,
`TestClusterReliablePublishHungReplica`, `TestClusterRebuildEmptyNode`,
`TestClusterSessionFailover`, `TestClusterSessionHandoff`,
`TestClusterSessionMoveForgetsStaleCopy`, `TestClusterReplicaRestartStoresOnce`;
with encryption at rest, in `server/e2e/at_rest_test.go`:
`TestClusterEncryptAtRest` and `TestClusterEncryptAtRestSessionFailover`, each
with every node sealing and with a mixed cluster.
Unit tests: `TestRingGetN` (`pkg/hash`), `TestLogDeleteAcrossTimeBlocks` and
`TestHintKeptWhenStoreFails` (`server/internal`).

The store's own records: `TestNewStoreLayout`, `TestMigrateFromV060`,
`TestMigrateInterrupted`, `TestMigrateSharedNamespace` and
`TestReservedContracts` (`server/internal/store`), `TestMoveLegacyHints`
(`server/internal`), `TestGetWithIDs` (the engine); and in
`server/e2e/release3_test.go`, `TestSysTopicsOutOfReach`. (A v0.6.0 node
can't join a cluster of the peerwire protocol: a store of v0.6.0 is upgraded
by starting the new version on it, `TestMigrateFromV060`.)
