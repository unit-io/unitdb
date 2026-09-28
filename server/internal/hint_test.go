package internal

import (
	"errors"
	"testing"

	"github.com/unit-io/unitdb/server/internal/store"
)

// TestHintKeptWhenStoreFails makes the store fail to take a hint: the hint is
// kept in memory, and stored once the store takes it again.
func TestHintKeptWhenStoreFails(t *testing.T) {
	openTestStore(t)
	replica := "hint-test-replica"
	c := &Cluster{}

	putHint = func(string, []byte, []byte, string) error { return errors.New("store unavailable") }
	c.hint(replica, ReplicaEntry{ID: "hint-test/1", Contract: 1, Topic: "groups.hint", Payload: []byte("m"), Ttl: "1h"})
	putHint = store.Hint.Put
	if hints, _ := store.Hint.Get(replica); len(hints) != 0 {
		t.Fatalf("store has %d hints while failing", len(hints))
	}
	if len(c.pending) != 1 {
		t.Fatalf("%d hints kept in memory, want 1", len(c.pending))
	}

	c.storePendingHints()
	if hints, _ := store.Hint.Get(replica); len(hints) != 1 {
		t.Fatalf("store has %d hints once working again, want 1", len(hints))
	}
	if len(c.pending) != 0 {
		t.Fatalf("%d hints still kept in memory", len(c.pending))
	}
}
