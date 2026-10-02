package unitdb

import (
	"testing"
	"time"

	"github.com/unit-io/unitdb/message"
)

// A topic's name is packed into its first entry only. Deleting that entry
// before a sync used to drop it from memory, so the topic's later entries
// reached disk without the name, and the DB no longer opened:
// "recovery.recoverWindowBlocks: timeWindow sync error, unable to get topic
// offset from trie".

var unsyncedTopic = []byte("unsynced.delete.topic")

// putTwo writes two entries with ids on unsyncedTopic, and returns the ids.
func putTwo(t *testing.T, db *DB) ([]byte, []byte) {
	t.Helper()
	var ids [2][]byte
	for i, body := range []string{"first", "second"} {
		id := db.NewID()
		if err := db.PutEntry(NewEntry(unsyncedTopic, []byte(body)).WithID(id)); err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	return ids[0], ids[1]
}

func unsyncedBodies(t *testing.T, db *DB) []string {
	t.Helper()
	items, err := db.Get(NewQuery(unsyncedTopic).WithLast("1h"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, it := range items {
		out = append(out, string(it))
	}
	return out
}

func wantOnlySecond(t *testing.T, what string, got []string) {
	t.Helper()
	if len(got) != 1 || got[0] != "second" {
		t.Errorf("%s: %q, want only the second entry", what, got)
	}
}

// TestDeleteFirstUnsyncedReopens deletes a topic's first entry before any
// sync, closes the DB and opens it again.
func TestDeleteFirstUnsyncedReopens(t *testing.T) {
	db, dir := openTestDB(t, WithMutable())
	first, _ := putTwo(t, db)
	if err := db.DeleteEntry(NewEntry(unsyncedTopic, nil).WithID(first)); err != nil {
		t.Fatal(err)
	}
	wantOnlySecond(t, "right after the delete", unsyncedBodies(t, db))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenTestDB(t, dir, WithMutable())
	wantOnlySecond(t, "after reopening", unsyncedBodies(t, db))
}

// TestDeleteFirstUnsyncedThenSync deletes a topic's first entry before a
// sync, lets the entries reach disk, and reopens.
func TestDeleteFirstUnsyncedThenSync(t *testing.T) {
	db, dir := openTestDB(t, WithMutable())
	first, _ := putTwo(t, db)
	if err := db.DeleteEntry(NewEntry(unsyncedTopic, nil).WithID(first)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond) // past the entries' time block
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	wantOnlySecond(t, "after the sync", unsyncedBodies(t, db))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenTestDB(t, dir, WithMutable())
	wantOnlySecond(t, "after reopening", unsyncedBodies(t, db))
}

// TestRecoverySkipsUnnamedTopic opens a DB broken as the old delete left it:
// a topic's first entry dropped from memory before a sync. It must open,
// without the topic it can't name.
func TestRecoverySkipsUnnamedTopic(t *testing.T) {
	db, dir := openTestDB(t, WithMutable())
	first, _ := putTwo(t, db)
	// What delete did before.
	db.internal.mem.Delete(message.ID(first).Sequence())
	other := []byte("unsynced.other.topic")
	if err := db.Put(other, []byte("kept")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenTestDB(t, dir, WithMutable())
	items, err := db.Get(NewQuery(other).WithLast("1h"))
	if err != nil || len(items) != 1 || string(items[0]) != "kept" {
		t.Errorf("another topic after recovery: %q (%v), want it kept", items, err)
	}
}

// TestCloseWaitsForDeferredDelete closes the DB while a sync is applying a
// delete that waited for its entry to reach disk. Close used to see nothing
// waiting once the sync had taken the delete, and closed the DB under it: the
// delete failed, and the entry came back on open.
func TestCloseWaitsForDeferredDelete(t *testing.T) {
	db, dir := openTestDB(t, WithMutable())
	first, _ := putTwo(t, db)
	if err := db.DeleteEntry(NewEntry(unsyncedTopic, nil).WithID(first)); err != nil {
		t.Fatal(err)
	}
	// Until the entry is on disk, without applying the delete.
	seq := message.ID(first).Sequence()
	for deadline := time.Now().Add(5 * time.Second); !db.onDisk(seq); {
		if time.Now().After(deadline) {
			t.Fatal("the entry didn't reach disk")
		}
		time.Sleep(100 * time.Millisecond)
		if err := db.syncOnce(); err != nil {
			t.Fatal(err)
		}
	}

	// Hold the sync lock, so that the delete being applied waits for it
	// while close runs.
	db.internal.syncLockC <- struct{}{}
	applied := make(chan struct{})
	go func() {
		db.applyDeferred()
		close(applied)
	}()
	time.Sleep(200 * time.Millisecond)
	closed := make(chan error, 1)
	go func() { closed <- db.Close() }()
	time.Sleep(200 * time.Millisecond)
	<-db.internal.syncLockC
	<-applied
	if err := <-closed; err != nil {
		t.Fatal(err)
	}

	db = reopenTestDB(t, dir, WithMutable())
	wantOnlySecond(t, "after reopening", unsyncedBodies(t, db))
}
