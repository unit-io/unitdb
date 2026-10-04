# unitdb docs

- [About](about.md): what unitdb is.
- [Usage](usage.md): using the database from Go.
- [uTP](utp.md): the protocol clients speak to the server.
- [Cluster data synchronisation](cluster-data-sync.md): topic ownership,
  membership and failover, the request path, and subscriptions across ring
  changes.
- [Message log replication](message-log-replication.md): replicas of stored
  messages and session logs, hints, and rebuilding a node that lost its disk.
- [Backup and restore](backup-restore.md): checkpoints, the backup run, copies
  off the cluster, the security journal, reconciliation after a restore, the
  weekly restore test, and the runbooks.
- [Rolling deploys](rolling-deploys.md): upgrading a cluster node by node, and
  the maintenance-window upgrade from v0.3.0.
