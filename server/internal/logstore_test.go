package internal

import (
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/store"
)

// TestLogDeleteAcrossTimeBlocks puts a session log entry, puts it again in a
// later time block of the store, as a RECEIPT replaces a logged PUBLISH, and
// deletes it, as a COMPLETE does: the entry must be gone, not the first
// version back.
func TestLogDeleteAcrossTimeBlocks(t *testing.T) {
	openTestStore(t)
	block := uint32(0x7e57b10c)
	key := uint64(2)<<32 | uint64(block)
	store.Log.Apply(store.LogOp{Block: block, Key: key, Raw: []byte("publish")})
	time.Sleep(1500 * time.Millisecond)
	store.Log.Apply(store.LogOp{Block: block, Key: key, Raw: []byte("receipt")})
	if keys := store.Log.Keys(block); len(keys) != 1 {
		t.Fatalf("keys after two puts = %v, want the key once", keys)
	}
	store.Log.Apply(store.LogOp{Block: block, Key: key})
	if raw := store.Log.Raw(key); raw != nil {
		t.Fatalf("after delete, the entry is %q", raw)
	}
	if keys := store.Log.Keys(block); len(keys) != 0 {
		t.Fatalf("after delete, keys = %v", keys)
	}
}
