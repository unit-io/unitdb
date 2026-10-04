package unitdb

import (
	"path/filepath"
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

// TestTopicNamedBeforeEntries puts a topic's first entry, and checks its
// name is written to its file when Put returns: the entry may reach the WAL
// any time after, and a crash then must find the topic named.
func TestTopicNamedBeforeEntries(t *testing.T) {
	db, dir := openTestDB(t, WithMutable())
	defer db.Close()
	topic := []byte("unit.named.first")
	if err := db.Put(topic, []byte("m")); err != nil {
		t.Fatal(err)
	}
	f, err := newFile(dir, 1, _FileDesc{fileType: typeTopics})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	names := newTopicNames(f)
	if err := names.load(); err != nil {
		t.Fatal(err)
	}
	tp, _, err := db.parseTopic(message.MasterContract, topic)
	if err != nil {
		t.Fatal(err)
	}
	tp.AddContract(message.MasterContract)
	if _, ok := names.get(tp.GetHash(message.MasterContract)); !ok {
		t.Fatal("the topic is not named in its file after Put returned")
	}
}

// TestTopicCollision names a topic hash another topic has: its entries
// would be the other's.
func TestTopicCollision(t *testing.T) {
	db, _ := openTestDB(t, WithMutable())
	defer db.Close()
	a := &message.Topic{Depth: 1, Parts: []message.Part{{Hash: 1}}}
	b := &message.Topic{Depth: 1, Parts: []message.Part{{Hash: 2}}}
	if err := db.nameTopic(42, a); err != nil {
		t.Fatal(err)
	}
	if err := db.nameTopic(42, a); err != nil {
		t.Fatalf("naming a topic again: %v", err)
	}
	if err := db.nameTopic(42, b); err != errTopicCollision {
		t.Fatalf("another topic of the hash: %v; want %v", err, errTopicCollision)
	}
}

// TestOpenFormat3 opens a DB written by format 3 (testdata/compat/v3), in
// which topics are named by their first entries: on disk, in the WAL, and
// one whose first entry was deleted before a sync. The topics file must
// name them all once it is open, and the messages read as before.
func TestOpenFormat3(t *testing.T) {
	dir := copyDir(t, filepath.Join("testdata", "compat", "v3"))
	want := map[string]int{"compat.disk": 10, "compat.deleted.first": 2, "compat.wal": 4}
	check := func(db *DB, when string) {
		t.Helper()
		for topic, n := range want {
			items, err := db.Get(NewQuery([]byte(topic)).WithLimit(100))
			if err != nil || len(items) != n {
				t.Errorf("%s: %s: %d messages, %v; want %d", when, topic, len(items), err, n)
			}
		}
		if err := db.Verify(); err != nil {
			t.Errorf("%s: %v", when, err)
		}
	}
	db, err := Open(dir, WithMutable())
	if err != nil {
		t.Fatal(err)
	}
	check(db, "opened")
	if n := len(db.internal.topics.all()); n != len(want) {
		t.Errorf("the topics file names %d topics; want %d", n, len(want))
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(dir, WithMutable())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if v := db.internal.dbInfo.header.version; v != version {
		t.Errorf("format %d after a close; want %d", v, version)
	}
	check(db, "reopened")
}
