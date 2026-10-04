package memdb

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func timeBlockCount(db *DB) int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.timeBlocks)
}

// TestBatchReleasesItsBlocks checks that a batch, committed or aborted,
// leaves no time block behind but those of its entries: a block holds a
// buffer of the pool until it is released.
func TestBatchReleasesItsBlocks(t *testing.T) {
	db, err := Open(WithLogFilePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	before := timeBlockCount(db)
	for i := 0; i < 50; i++ {
		if err := db.Batch(func(b *Batch, _ <-chan struct{}) error {
			return b.Put(uint64(i), []byte("v"))
		}); err != nil {
			t.Fatal(err)
		}
	}
	// One block for each batch's entries; the block a batch opens after
	// writing them must go.
	if got := timeBlockCount(db) - before; got > 50 {
		t.Fatalf("50 committed batches left %d time blocks", got)
	}
	before = timeBlockCount(db)
	aborted := errors.New("aborted")
	for i := 0; i < 50; i++ {
		db.Batch(func(b *Batch, _ <-chan struct{}) error {
			if err := b.Put(uint64(1000+i), []byte("v")); err != nil {
				return err
			}
			return aborted
		})
		if _, err := db.Get(uint64(1000 + i)); err == nil {
			t.Fatalf("the entry of an aborted batch is found")
		}
	}
	if got := timeBlockCount(db) - before; got != 0 {
		t.Fatalf("50 aborted batches left %d time blocks", got)
	}
}

// TestDeletesOnlyBlockIsReleased deletes entries of a past block: their
// deletes go to the current block, which then holds deletes only. Once
// writes go to a later block, both are released, with their buffers.
func TestDeletesOnlyBlockIsReleased(t *testing.T) {
	const d = 20 * time.Millisecond
	db, err := Open(WithLogFilePath(t.TempDir()), WithTimeBlockInterval(d), WithLogInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for k := uint64(0); k < 10; k++ {
		if _, err := db.Put(k, []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(2 * d)
	for k := uint64(0); k < 10; k++ {
		if err := db.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	for deadline := time.Now().Add(2 * time.Second); timeBlockCount(db) > 1; {
		if time.Now().After(deadline) {
			t.Fatalf("%d time blocks left; want the current one only", timeBlockCount(db))
		}
		time.Sleep(d)
	}
	if err := db.Verify(); err != nil {
		t.Fatal(err)
	}
}

// TestIdleBlocksAreReleased leaves a DB idle: writes go to a new time block
// every block duration, and the empty ones they leave were never released,
// each with a buffer of the pool.
func TestIdleBlocksAreReleased(t *testing.T) {
	const d = 20 * time.Millisecond
	db, err := Open(WithLogFilePath(t.TempDir()), WithTimeBlockInterval(d), WithLogInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	time.Sleep(25 * d)
	// Rotation lags on a loaded machine: give it a while.
	for deadline := time.Now().Add(2 * time.Second); timeBlockCount(db) > 2; {
		if time.Now().After(deadline) {
			t.Fatalf("%d time blocks after %d idle block durations; want the current one, and one rotating", timeBlockCount(db), 25)
		}
		time.Sleep(d)
	}
}

// TestLogIDsAreUnique takes log IDs from many goroutines at once: a log is
// written to a file named by its ID, and two logs of one ID wrote one file.
func TestLogIDsAreUnique(t *testing.T) {
	db, err := Open(WithLogFilePath(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const n, each = 8, 2000
	ids := make(chan _TimeID, n*each)
	done := make(chan struct{})
	for g := 0; g < n; g++ {
		go func() {
			for i := 0; i < each; i++ {
				ids <- db.newLogID()
			}
			done <- struct{}{}
		}()
	}
	for g := 0; g < n; g++ {
		<-done
	}
	close(ids)
	seen := make(map[_TimeID]bool)
	for id := range ids {
		if seen[id] {
			t.Fatalf("log ID %d given twice", id)
		}
		seen[id] = true
	}
}

// TestRewritesFreeBlocks rewrites and deletes a few keys over and over,
// across many blocks, as a store of message logs does: the blocks and the
// WAL's logs must stay few. With a version of a key in each block it was
// put in, a store that never deleted every version kept every block, and
// every log, until it jammed.
func TestRewritesFreeBlocks(t *testing.T) {
	const d = 10 * time.Millisecond
	dir := t.TempDir()
	db, err := Open(WithLogFilePath(dir), WithTimeBlockInterval(d), WithLogInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 3000; i++ {
		k := uint64(i % 20)
		if _, err := db.Put(k, []byte("value")); err != nil {
			t.Fatal(err)
		}
		if i%3 == 2 {
			if err := db.Delete(k); err != nil {
				t.Fatal(err)
			}
		}
		if i%100 == 0 {
			time.Sleep(d)
		}
	}
	time.Sleep(5 * d)
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * d)
	logs, _ := filepath.Glob(filepath.Join(dir, logDir, "*.log"))
	if n := timeBlockCount(db); n > 10 || len(logs) > 60 {
		t.Fatalf("%d time blocks and %d logs after 3000 rewrites of 20 keys", n, len(logs))
	}
	if err := db.Verify(); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d time blocks, %d logs, %d keys", timeBlockCount(db), len(logs), db.Size())
}

// TestBlocksDeletingFromEachOtherGo puts a key in the current block, then a
// batch of it and another, then the other in the current block again: the
// batch's block deletes from the current block, which deletes from the
// batch's. Each waited for the other's logs to go, and both kept theirs for
// good; a store recovered from logs of several versions of a key had
// thousands of such blocks.
func TestBlocksDeletingFromEachOtherGo(t *testing.T) {
	const d = 300 * time.Millisecond
	dir := t.TempDir()
	db, err := Open(WithLogFilePath(dir), WithTimeBlockInterval(d), WithLogInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// Start at the beginning of a block, so the writes share one.
	time.Sleep(time.Until(time.Now().Truncate(d).Add(d)))
	if _, err := db.Put(1, []byte("current")); err != nil {
		t.Fatal(err)
	}
	if err := db.Batch(func(b *Batch, _ <-chan struct{}) error {
		if err := b.Put(1, []byte("batch")); err != nil {
			return err
		}
		return b.Put(2, []byte("batch"))
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Put(2, []byte("current")); err != nil {
		t.Fatal(err)
	}
	for _, k := range []uint64{1, 2} {
		if err := db.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(3 * time.Second); ; {
		logs, _ := filepath.Glob(filepath.Join(dir, logDir, "*.log"))
		if timeBlockCount(db) <= 1 && len(logs) <= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d time blocks and %d logs left", timeBlockCount(db), len(logs))
		}
		time.Sleep(d / 3)
	}
}
