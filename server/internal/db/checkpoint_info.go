package adapter

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// CheckpointInfoFile is the file in a checkpoint that describes it
// (docs/backup-restore.md). A store's own directory never
// holds one: a db_path that does is a checkpoint.
const CheckpointInfoFile = "checkpoint.json"

// CheckpointInfo describes a checkpoint: what a backup's manifest lists for
// each node, and what a node started at the checkpoint tells its peers.
type CheckpointInfo struct {
	// Node is the cluster node the checkpoint is of; empty for a single
	// server.
	Node string `json:"node"`
	// Run is the backup run the checkpoint is of (its id, the run's start
	// as 20060102T150405Z); empty for a checkpoint taken on its own.
	Run string `json:"run,omitempty"`
	// Time is when the checkpoint was taken, in UTC.
	Time time.Time `json:"time"`
	// RingVersion is the version of the ring the cluster routed by; 0 for a
	// single server.
	RingVersion int `json:"ring_version"`
	// Engine is the version of the unitdb module that wrote the store.
	Engine string `json:"engine"`
	// KeyIDs are the ids of the keyring's keys (never the keys): the
	// checkpoint opens only with a keyring that holds them.
	KeyIDs []int `json:"key_ids"`
	// Stats is the size of the store the checkpoint copied.
	Stats Stats `json:"stats"`
}

// ManifestFile is the file in a checkpoint of a backup run that lists the
// run's checkpoints, on every node: the backup job writes it.
const ManifestFile = "manifest.json"

// ReadCheckpointInfo returns the description of the checkpoint in dir, or
// nil if dir holds none: a store that isn't a checkpoint.
func ReadCheckpointInfo(dir string) (*CheckpointInfo, error) {
	b, err := os.ReadFile(filepath.Join(dir, CheckpointInfoFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var info CheckpointInfo
	if err := json.Unmarshal(b, &info); err != nil {
		return nil, fmt.Errorf("%s: %v", filepath.Join(dir, CheckpointInfoFile), err)
	}
	return &info, nil
}

// WriteCheckpointInfo writes info into dir as CheckpointInfoFile, whole or
// not at all, and fsyncs it and dir.
func WriteCheckpointInfo(dir string, info CheckpointInfo) error {
	b, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return err
	}
	return writeSynced(dir, CheckpointInfoFile, append(b, '\n'))
}

// WriteManifest writes a backup run's manifest into dir, a checkpoint of
// the run, as ManifestFile, whole or not at all.
func WriteManifest(dir string, manifest []byte) error {
	return writeSynced(dir, ManifestFile, manifest)
}

// writeSynced writes b into dir as name, through a temporary file renamed
// into place, and fsyncs it and dir.
func writeSynced(dir, name string, b []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
