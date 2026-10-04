package store_test

import (
	"testing"

	"github.com/unit-io/unitdb/server/internal/store"
)

// TestCheckpointAndStats checks the store's checkpoint and stats, which the
// adapter's tests cover in depth.
func TestCheckpointAndStats(t *testing.T) {
	newStore(t, 0, ring(0), true)
	if err := store.Message.Put(contract, "groups.stats", []byte("x"), ""); err != nil {
		t.Fatal(err)
	}
	if err := store.Probe(); err != nil {
		t.Fatal(err)
	}
	if s := store.StoreStats(); s.DiskBytes == 0 || s.MemEntries == 0 {
		t.Errorf("stats %+v", s)
	}
	dst := t.TempDir()
	if err := store.Checkpoint(dst); err != nil {
		t.Fatal(err)
	}
	if err := store.Checkpoint(dst); err == nil {
		t.Error("a checkpoint into a directory that isn't empty was taken")
	}
	if err := store.Probe(); err != nil {
		t.Errorf("the store after a checkpoint: %v", err)
	}
	if got, err := store.Message.Get(contract, "groups.stats", ""); err != nil || len(got) == 0 {
		t.Errorf("the store after a checkpoint: %d messages (%v)", len(got), err)
	}
}
