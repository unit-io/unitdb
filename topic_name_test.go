package unitdb

import (
	"testing"
	"time"

	"github.com/unit-io/unitdb/message"
)

// putNamedLater puts a topic's first entry, which holds its name, in a
// batch, and a second entry with Put: the second goes to the memdb block of
// the second, which sorts before the batch's and so reaches disk first.
func putNamedLater(t *testing.T, db *DB, topic []byte) {
	t.Helper()
	if err := db.Batch(func(b *Batch, _ <-chan struct{}) error {
		return b.Put(topic, []byte("first"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(topic, []byte("second")); err != nil {
		t.Fatal(err)
	}
}

func wantBoth(t *testing.T, db *DB, topic []byte, when string) {
	t.Helper()
	items, err := db.Get(NewQuery(topic).WithLimit(10))
	if err != nil || len(items) != 2 || string(items[0]) != "second" || string(items[1]) != "first" {
		t.Errorf("%s: %q, %v; want [second first]", when, items, err)
	}
	if err := db.Verify(); err != nil {
		t.Errorf("%s: %v", when, err)
	}
}

// TestTopicNamedBySyncedLater syncs a topic whose name is in an entry
// written after another of the topic, and reopens. Open read the name from
// the topic's oldest window entry only, and lost the topic.
func TestTopicNamedBySyncedLater(t *testing.T) {
	db, dir := openTestDB(t, WithMutable())
	topic := []byte("unit.named.later")
	putNamedLater(t, db, topic)
	time.Sleep(1100 * time.Millisecond) // past the blocks' second
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	wantBoth(t, db, topic, "after the sync")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenTestDB(t, dir, WithMutable())
	wantBoth(t, db, topic, "after reopening")
}

// TestTopicNamedByRecoveredLater reopens with the same entries in the WAL
// only. Recovery holds back the window entries of a topic named by a later
// block, and didn't write them: the second entry was in the index and no
// query found it. Recovery put a batch's log in the block of its second
// until the WAL recorded blocks, which hid this.
func TestTopicNamedByRecoveredLater(t *testing.T) {
	db, dir := openTestDB(t, WithMutable())
	topic := []byte("unit.named.later")
	putNamedLater(t, db, topic)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenTestDB(t, dir, WithMutable())
	wantBoth(t, db, topic, "after reopening")
}

// TestBatchNamesItsTopics puts a topic's first entry, and then a batch of
// the topic: the batch is written to the WAL as it commits, maybe before
// the Put, and a crash can lose the Put. The batch's first entry of the
// topic must hold its name too, or the batch's entries can't be found.
func TestBatchNamesItsTopics(t *testing.T) {
	db, _ := openTestDB(t, WithMutable())
	defer db.Close()
	topic := []byte("unit.named.batch")
	if err := db.Put(topic, []byte("put")); err != nil {
		t.Fatal(err)
	}
	ids := [2][]byte{db.NewID(), db.NewID()}
	if err := db.Batch(func(b *Batch, _ <-chan struct{}) error {
		for _, id := range ids {
			if err := b.PutEntry(NewEntry(topic, []byte("batched")).WithID(id)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	for i, id := range ids {
		m, _, ok := db.memEntry(message.ID(id).Sequence())
		if !ok {
			t.Fatalf("batch entry %d is not in memory", i)
		}
		if named := m.topicSize != 0; named != (i == 0) {
			t.Errorf("batch entry %d holds the topic's name: %v; want %v", i, named, i == 0)
		}
	}
}
