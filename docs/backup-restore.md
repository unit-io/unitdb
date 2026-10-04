# Backup and restore

How a unitdb cluster's data is backed up and restored: checkpoints, the
backup run and its manifest, copies off the cluster, the security journal,
reconciliation after a restore, the weekly restore test, and the runbooks.
Everything here is built; the deploy examples are in `deploy/kubernetes` and
`deploy/aws`.

## What a backup is

- **A checkpoint** is a copy of a node's store that opens as the store was
  at one moment (`server/internal/db/unitdb/checkpoint.go`). A copy of a
  running store's files isn't one: they are copied at different moments,
  and the WAL isn't fsynced, so a volume snapshot is like a power cut. The
  adapter holds writes back, for up to about 1.5 s, until the DB has written
  out the latest; then it syncs, copies the DB's files, and copies memdb's
  records (not its files) into a fresh memdb. Reads go on. Last it writes
  `checkpoint.json`: the node, the backup run, the time, the ring and
  engine versions, the keyring's key ids (never keys), and the copy's
  counts. A checkpoint without it is incomplete.
- `POST /_checkpoint` on the monitor port (`monitor_listen`), with
  `Authorization: Bearer <CHECKPOINT_TOKEN>`, takes one into `CHECKPOINT_DIR`;
  off unless both are set. One runs at a time, one per
  `CHECKPOINT_MIN_MINUTES` (10); the newest `CHECKPOINT_KEEP` (3) are kept.
- **A backup run** (`server/cmd/backup`, `deploy/kubernetes/backups.yaml`)
  checkpoints every node under one run id (`POST /_checkpoint?run=<id>`,
  into `ckpt-<run>-<node>`), one node after another, each tried three
  times, and keeps the run's `manifest.json` (each node's
  `checkpoint.json`, or why it failed) in every node's checkpoint
  (`PUT /_checkpoint/manifest`). A node that failed fails the run: a
  restore needs every node of one run.
- Just before its checkpoint of a run, a node writes **a canary**, the run's
  id, into its store as a message and as a memdb record; the restore test
  reads it back.

## Copies off the cluster

With `-upload`, the backup run has each node upload its own checkpoint (the
checkpoints are on each node's volume): `POST /_checkpoint/upload?run=<id>`
streams it, archived (tar), compressed (zstd) and encrypted with
[age](https://age-encryption.org) to the backup key's public half, to S3,
with the manifest (`server/internal/backup`):

```
<cluster>/<run>/manifest.json
<cluster>/<run>/<node>.tar.zst.age
<cluster>/journal/<date>/<node>-<time>.jsonl
```

- The bucket has Object Lock. Each object is locked in compliance mode by
  its uploader for as long as its tier is kept, and tagged with the tier;
  lifecycle rules expire it after: daily runs 8 days, weekly (Sundays) 29,
  monthly (the 1st) and the journal 396.
- The cluster's identity may only put objects: it can neither read, list
  nor delete them, so a compromised cluster can't read or erase its
  backups. The private half of the backup key is kept apart, readable by the
  restore test's identity and named admins only.
- **The keyring is escrowed** (`backup escrow-keyring`, in AWS Secrets
  Manager): a backup restores only with the keyring of its time. Put it in
  escrow before every change of the keyring; the escrow keeps every key it
  ever held, retired ones as `read` keys, and refuses a key id with another
  key.

A node's settings (`server/internal/backup/backup.go`):

| Variable | |
| --- | --- |
| `BACKUP_S3_BUCKET` | the bucket; uploads are off without it |
| `BACKUP_S3_REGION` | the region; the AWS SDK's (`AWS_REGION`, a profile) if unset |
| `BACKUP_S3_ENDPOINT` | another S3 (MinIO), path-style |
| `BACKUP_CLUSTER` | the cluster's prefix in the bucket |
| `BACKUP_AGE_RECIPIENT` | the backup key's public half (`age1...`) |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | the put-only identity, or any of the AWS SDK's credential sources |
| `SECURITY_JOURNAL_DIR` | where the security journal waits for its upload |

`deploy/aws/README.md` makes the bucket, its lifecycle, the identities and
the keys.

## The security journal

A backup can't know what was revoked after its run. So each change of the
security state a node makes (a revoked client id or topic key, a
contract's not-before time) is written as a JSON line to
`SECURITY_JOURNAL_DIR`, and fsynced, before it takes effect, and uploaded in
a batch every 10 s (`server/internal/offsite.go`). A batch is deleted from
the node only once it is in the bucket, so a restart uploads what was left.
`unitdb_security_journal_lag_seconds` is the age of the oldest line not up
yet.

A restore replays it: `-restored -journal <dir>`, with the journal since the
run (`backup fetch-journal`), before the node takes clients. Lines merge as
the changes did, so lines older than the checkpoint, or replayed twice,
change nothing. Nodes never read the bucket.

## Restoring: one node, or the whole cluster

- **One node lost, the others up:** don't restore it from a checkpoint. Start
  it with an empty store: it copies the topics it holds from the other nodes
  (`Cluster.rebuild`), up to the moment, and takes no clients until it has
  (`/_readyz` says "catching up").
- A node won't start at a checkpoint (a `db_path` with `checkpoint.json`)
  while a peer answers that wasn't itself started at a checkpoint of the
  same run (`Cluster.StartedFrom`): it would never get what was written
  since. It says to start empty, or with `-restored`.
- **The whole cluster lost:** start every node at its checkpoint of one run,
  with `-restored`. Each reconciles every topic it holds with the topic's
  other live holders before it takes clients (`/_readyz` says
  "reconciling"; `server/internal/cluster_reconcile.go`): both list the
  topic's messages as digests, `SHA-256(contract, topic, expiry, payload)`,
  with how many times each occurs, and each gets what it holds fewer of. A
  message one node took in the seconds between two nodes' checkpoints is
  then on both; two identical publishes stay two. A message is sent with
  how many its sender holds, and the receiver stores only what it lacks of
  that count, so two restored nodes reconciling a topic with each other at
  once settle on the larger count, not the sum. Hints are dropped: the
  restored nodes are reconciled instead. Once done, `checkpoint.json`
  becomes `restored-from.json`, and later restarts are ordinary ones.
- `-restored` also brings one node at its checkpoint into a running
  cluster: it catches up with the others the same way.

| Failure | Data lost | Back in |
| --- | --- | --- |
| One node | none: rebuilt from the others | minutes |
| The whole cluster | since the last run (a day, with one run a day); revocations since are replayed | under an hour, plus the download |

## The weekly restore test

`backup verify` (`deploy/kubernetes/restore-test.yaml`), with a reader's
identity, downloads the newest complete run, decrypts it, and opens each
node's checkpoint on a scratch server of its own (`-db_path` at it, no
cluster, no clients), with the escrowed keyring. Each must be ready (its
store probe writes and reads back), hold what the manifest says within 10%
(what expired since), and hold the run's canary; and the escrow must hold
every key id the run names. Only then it pushes
`unitdb_backup_restore_verified_timestamp_seconds` to a Pushgateway.

`deploy/kubernetes/backup-alerts.yaml` alerts when a checkpoint or an
upload is over 26 h old, checkpoints fail, the backup job fails, the
journal is over 5 minutes behind or fails to record, or the restore test
hasn't passed in 8 days.

## Runbooks

For a cluster deployed as a StatefulSet `unitdb` in a namespace `unitdb`,
with claims `data-unitdb-N`, as the examples in `deploy/kubernetes`; adjust
the names. `backup` is `server/cmd/backup`. The e2e tests rehearse them:
`TestRestoreLostNodeStartsEmpty` (A) and `TestRunbookWholeClusterRestore`
(B, against MinIO).

### A: one node lost

1. Check that it is one node: the others answer `/_readyz`.
2. Give it a new, empty claim; the StatefulSet makes the pod again:
   ```sh
   kubectl -n unitdb delete pvc data-unitdb-N --wait=false
   kubectl -n unitdb delete pod unitdb-N
   ```
3. Wait for `/_readyz`: "catching up" until it has its topics, then ready.

### B: the whole cluster lost

1. **Stop what is left:** `kubectl -n unitdb scale statefulset unitdb
   --replicas=0`. Snapshot a damaged volume first if it still exists.
2. **The keyring:** if its Secret is gone, make it again from the escrow
   (runbook C). Without the escrow there is no restore.
3. **The reader's identity, for the restore only:** a Secret
   `unitdb-backup-reader` with an admin's reader keys; deleted at step 9.
4. **The run:** `backup runs` lists them, newest first; take the newest
   `complete` one. `backup verify -run <run>` checks it first.
5. **New claims, with the run:** delete the old claims, make new empty ones,
   and fill each with its node's checkpoint and the journal since the run
   (`deploy/kubernetes/restore-node-job.yaml`, per node, `NODE` and `RUN`
   filled in). The Job runs as the user unitdb runs as: the files it writes
   are the node's store.
6. **Start every node restored:** add `-restored` and
   `-journal=/var/udb/data/restore-journal` to the StatefulSet's args, and
   scale it up. Each replays the journal, then reconciles (`/_readyz`:
   "reconciling"); its log says `reconciled after a restore` with
   `"failed":0`.
7. **Take the flags out** once all are ready; the rolling restart is an
   ordinary one. `-restored` on a node that isn't at a checkpoint is
   refused.
8. **Check:** every node ready, `unitdb_reconcile_failures_total` 0, the
   data up to the run there, nothing revoked since back.
9. **Clean up, and a new baseline:** delete the reader's Secret and the
   restore Jobs, and run a backup now
   (`kubectl create job --from=cronjob/unitdb-backup ...`).

### C: the keyring lost

There is no copy but the escrow. Read it with an admin's identity, make the
cluster's Secret again from it (`UNITDB_KEYRING`, and a new
`CHECKPOINT_TOKEN`), and restart the nodes.

### D: one node at its checkpoint into the running cluster

Start it with `-db_path` at its checkpoint and `-restored`: it reconciles
with the running nodes before it takes clients. Then take `-restored` out.
