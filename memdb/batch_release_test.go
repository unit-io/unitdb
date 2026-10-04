package memdb

import (
	"errors"
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
	if n := timeBlockCount(db); n > 2 {
		t.Fatalf("%d time blocks after %d idle block durations; want the current one, and one rotating", n, 25)
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
