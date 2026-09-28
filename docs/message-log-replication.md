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
  other replica; a goroutine per node sends them in batches. A **reliable or
  batch publish** first waits for one replica, the first in ring order to take
  it, before it is acknowledged, up to 1 s per replica; the others get it from
  the queue. Express publishes do not wait.
- **Replica copies** are stored under the replica's own id, in a separate
  replica store (the contract salted with `replicaStoreId`). A unitdb id
  carries the sequence of the store that made it, so writing the owner's id on
  a replica could overwrite an unrelated message there.
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

A copy that cannot reach a node of its replica set is kept as a **hint** on
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
- A change a replica missed is kept as a hint naming the key, and the handoff
  sends the key's state then, so that hints need no order.
- When a client resumes a session, its node gets every other node's copy
  (1 s timeout) and merges their logs by key before resuming. Mode 1 is
  at-least-once, so a copy that missed completions redelivers rather than
  loses. The nodes that are not the session's replicas then drop their copies,
  which would go stale.
- memdb keeps a version of a key per time block it was written in; the store
  deletes every version, and lists each key once.

## Limits

- **A rebuilt node does not get its topics' older history** for topics that
  got no new message since the upgrade that added the topic index.
- **The last asynchronous writes before a crash can be lost**: express
  publishes and session log changes do not wait for a replica.
- **A crash between storing a replica and recording its id** can store it
  twice.
- **Wildcard relays return nothing**: unitdb v0.3.0 does not match wildcard
  queries.

## Tests

In `server/e2e/cluster_test.go`: `TestClusterReplicatedRelay`,
`TestClusterRelay`, `TestClusterReliablePublishSurvivesCrash`,
`TestClusterReliablePublishHungReplica`, `TestClusterRebuildEmptyNode`,
`TestClusterSessionFailover`, `TestClusterSessionHandoff`,
`TestClusterSessionMoveForgetsStaleCopy`, `TestClusterReplicaRestartStoresOnce`.
Unit tests: `TestRingGetN` (`pkg/hash`), `TestLogDeleteAcrossTimeBlocks` and
`TestHintKeptWhenStoreFails` (`server/internal`).
