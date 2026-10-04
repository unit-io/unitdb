package internal

import (
	"fmt"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
)

// A node started at a checkpoint (a db_path holding checkpoint.json) inside
// a cluster that runs never gets what was written since the checkpoint,
// unless it reconciles: it doesn't catch up, and as a topic's owner it
// answers relays without it (docs/backup-restore.md, gap 1). So such a node
// refuses to start, and says to start empty instead, which rebuilds it from
// the others, or with -restored, which reconciles it with them
// (cluster_reconcile.go) before it takes clients.
//
// A whole-cluster restore starts every node at a checkpoint of the same
// backup run, one after another, so the nodes tell each other the
// checkpoint they started at (Cluster.StartedFrom): a node at a checkpoint
// starts if every peer that answers started at one of the same run, even
// without -restored. With it, the nodes also settle on the union of what
// their checkpoints hold (the seconds between two nodes' checkpoints). A
// single server starts at a checkpoint as before: that's how one is
// restored.

// sameRun is how far apart two nodes' checkpoints taken without a run id
// may be and still be of one backup run: a run checkpoints its nodes one
// after another, in minutes. Checkpoints with a run id compare it instead.
const sameRun = time.Hour

// startedFrom is the checkpoint this node's store was started at; nil if it
// wasn't. Set before the cluster starts, read-only after.
var startedFrom *store.CheckpointInfo

// StartedFromReq asks a node which checkpoint its store was started at.
type StartedFromReq struct {
	// Node is the name of the node asking.
	Node string
}

// StartedFromResp is a node's answer: the checkpoint its store was started
// at, nil if none.
type StartedFromResp struct {
	Checkpoint *store.CheckpointInfo
}

// StartedFrom answers which checkpoint this node's store was started at. A
// node built before it answers that it lacks the method, which a node at a
// checkpoint takes as "a running cluster".
func (c *Cluster) StartedFrom(req *StartedFromReq, resp *StartedFromResp) error {
	resp.Checkpoint = startedFrom
	return nil
}

// CheckStart refuses a start that would leave this node's data silently
// behind the cluster's: at a checkpoint, while a peer that wasn't restored
// from the same backup run answers. It reads the store's checkpoint.json,
// if any, so it runs before the store opens, and after ClusterInit.
// restored is the -restored flag: the node starts at its checkpoint
// whatever its peers run, and reconciles with them.
func CheckStart(dbPath string, restored bool) error {
	const startEmpty = "start this node with an empty store instead (a new, empty claim): it copies its topics from the other nodes, and takes no clients until it has (docs/backup-restore.md, case A); or start it with -restored, which reconciles its checkpoint with them"
	info, err := store.ReadCheckpointInfo(dbPath)
	if err != nil {
		return fmt.Errorf("db_path %s looks like a checkpoint, but its %s is unreadable: %v", dbPath, store.CheckpointInfoFile, err)
	}
	if restored && info == nil {
		return fmt.Errorf("-restored: db_path %s isn't a checkpoint (it has no %s): -restored is for a node started at its checkpoint of a backup run", dbPath, store.CheckpointInfoFile)
	}
	if info == nil {
		return nil
	}
	startedFrom = info
	if restored {
		restoreRequested, restoredPath = true, dbPath
		log.ErrLogger.Info().Str("context", "CheckStart").Time("checkpoint", info.Time).Str("run", info.Run).Msg("restored: reconciling with the other nodes before taking clients")
		return nil
	}
	c := Globals.Cluster
	if c == nil {
		log.ErrLogger.Info().Str("context", "CheckStart").Time("checkpoint", info.Time).Msg("a single server, started at a checkpoint")
		return nil
	}
	for _, n := range c.nodes {
		peer, answered, err := n.startedFrom()
		if !answered {
			continue
		}
		at := info.Time.Format(time.RFC3339)
		switch {
		case err != nil:
			return fmt.Errorf("db_path %s is a checkpoint of %s, and node %s answers but not which checkpoint it started at (%v), so it's taken to run with its own data: started here, this node would never get what was written since; %s", dbPath, at, n.name, err, startEmpty)
		case peer == nil:
			return fmt.Errorf("db_path %s is a checkpoint of %s, and node %s runs with its own data: started here, this node would never get what was written since; %s", dbPath, at, n.name, startEmpty)
		case !sameBackupRun(peer, info):
			return fmt.Errorf("db_path %s is a checkpoint of %s, and node %s was started at one of %s, not the same backup run: start every node of a whole-cluster restore at one run's checkpoints, or, to restore one node, %s", dbPath, at, n.name, peer.Time.Format(time.RFC3339), startEmpty)
		}
	}
	log.ErrLogger.Info().Str("context", "CheckStart").Time("checkpoint", info.Time).Msg("started at a checkpoint: no peer answers that wasn't restored from the same run")
	return nil
}

// startedFrom asks the node which checkpoint it was started at. answered is
// false if it can't be reached: a node that is down doesn't stop a start.
func (n *ClusterNode) startedFrom() (cp *store.CheckpointInfo, answered bool, err error) {
	endpoint, _, err := n.dial()
	if err != nil {
		return nil, false, nil
	}
	defer endpoint.Close()
	var resp StartedFromResp
	call := endpoint.Go("Cluster.StartedFrom", &StartedFromReq{Node: Globals.Cluster.thisNodeName}, &resp, nil)
	select {
	case <-call.Done:
		return resp.Checkpoint, true, call.Error
	case <-time.After(2 * time.Second):
		return nil, true, fmt.Errorf("no answer in 2s")
	}
}

// sameBackupRun reports whether two checkpoints are of one backup run: the
// same run id, or, for checkpoints taken without one, within sameRun.
func sameBackupRun(a, b *store.CheckpointInfo) bool {
	if a.Run != "" && b.Run != "" {
		return a.Run == b.Run
	}
	return absDuration(a.Time.Sub(b.Time)) <= sameRun
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
