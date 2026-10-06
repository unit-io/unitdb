package memdb

import (
	"fmt"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func walLogs(t *testing.T, dir string) int {
	t.Helper()
	logs, err := filepath.Glob(filepath.Join(dir, logDir, "*.log"))
	if err != nil {
		t.Fatal(err)
	}
	return len(logs)
}

// TestCompactFreesPinnedBlocks keeps a key put first, and rewrites and
// deletes others over many blocks, as a message store does: the key holds
// its block, and every block chaining deletes back to it keeps its logs.
// Compact moves the key, and the blocks go with their logs.
func TestCompactFreesPinnedBlocks(t *testing.T) {
	const d = 10 * time.Millisecond
	dir := t.TempDir()
	db, err := Open(WithLogFilePath(dir), WithTimeBlockInterval(d), WithLogInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	const pinned = 7
	if _, err := db.Put(pinned, []byte("kept")); err != nil {
		t.Fatal(err)
	}
	// A key put in the first block too, and rewritten in each later one:
	// each rewrite deletes from the block before, which chains back.
	for i := 0; i < 150; i++ {
		if _, err := db.Put(8, []byte(fmt.Sprintf("row-%d", i))); err != nil {
			t.Fatal(err)
		}
		k := uint64(1000 + i)
		if _, err := db.Put(k, []byte("message")); err != nil {
			t.Fatal(err)
		}
		if err := db.Delete(k); err != nil {
			t.Fatal(err)
		}
		time.Sleep(d)
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * d)
	before := walLogs(t, dir)
	if before < 50 {
		t.Fatalf("%d logs before compacting: the key didn't hold the blocks", before)
	}

	moved, err := db.Compact()
	if err != nil {
		t.Fatal(err)
	}
	if moved == 0 {
		t.Fatal("Compact moved no value")
	}
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	var after int
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(5 * d) {
		if after = walLogs(t, dir); after <= 10 || time.Now().After(deadline) {
			break
		}
	}
	if after > 10 {
		t.Fatalf("%d logs after compacting, %d before", after, before)
	}
	t.Logf("moved %d values; %d logs before, %d after", moved, before, after)
	if err := db.Verify(); err != nil {
		t.Fatal(err)
	}
	check := func(db *DB, when string) {
		t.Helper()
		if v, err := db.Get(pinned); err != nil || string(v) != "kept" {
			t.Errorf("%s: the kept key reads %q, %v", when, v, err)
		}
		if v, err := db.Get(8); err != nil || string(v) != "row-149" {
			t.Errorf("%s: the rewritten key reads %q, %v", when, v, err)
		}
		if v, err := db.Get(1000); err == nil {
			t.Errorf("%s: a deleted key reads %q", when, v)
		}
		if n := db.Size(); n != 2 {
			t.Errorf("%s: %d keys; want 2", when, n)
		}
	}
	check(db, "compacted")
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(WithLogFilePath(dir), WithTimeBlockInterval(d))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	check(db, "reopened")
	if err := db.Verify(); err != nil {
		t.Fatal(err)
	}
}

// TestCompactLeavesMostlyLiveBlocks compacts a store whose blocks are mostly
// live: nothing moves. Each round of writes goes in one block: it starts once
// the log has rotated into a new block, and takes far less than a block. A
// write goes to the block of the log's last rotation, every log interval, so
// a round that started as a block began could put its first writes in the
// block before: split, it could leave one mostly dead, and rightly compacted.
func TestCompactLeavesMostlyLiveBlocks(t *testing.T) {
	const d = 50 * time.Millisecond
	db, err := Open(WithLogFilePath(t.TempDir()), WithTimeBlockInterval(d), WithLogInterval(2*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for i := 0; i < 5; i++ {
		// Into the next block, a few log intervals after it begins.
		now := time.Now()
		time.Sleep(now.Truncate(d).Add(d + 5*time.Millisecond).Sub(now))
		for k := 0; k < 10; k++ {
			if _, err := db.Put(uint64(i*10+k), []byte("value")); err != nil {
				t.Fatal(err)
			}
		}
		// One of ten deleted: the block stays mostly live.
		if err := db.Delete(uint64(i * 10)); err != nil {
			t.Fatal(err)
		}
	}
	// The last round's block in the past too.
	time.Sleep(2 * d)
	if moved, err := db.Compact(); err != nil || moved != 0 {
		t.Fatalf("Compact moved %d, %v; want none", moved, err)
	}
}

// TestCompactWhileWriting compacts over and over while writers put and
// delete keys of their own: each key keeps the last value its writer put,
// or none after its delete.
func TestCompactWhileWriting(t *testing.T) {
	const d = 5 * time.Millisecond
	dir := t.TempDir()
	db, err := Open(WithLogFilePath(dir), WithTimeBlockInterval(d), WithLogInterval(time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	const writers, keys = 4, 20
	want := make([]map[uint64]string, writers)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		want[w] = make(map[uint64]string)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rnd := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < 1500; i++ {
				k := uint64(w*keys + rnd.Intn(keys))
				if rnd.Intn(3) == 0 {
					if err := db.Delete(k); err == nil {
						delete(want[w], k)
					}
				} else {
					v := fmt.Sprintf("w%d-%d", w, i)
					if _, err := db.Put(k, []byte(v)); err != nil {
						t.Error(err)
						return
					}
					want[w][k] = v
				}
				if i%15 == 0 {
					time.Sleep(d)
				}
			}
		}(w)
	}
	var compactions int
	var cwg sync.WaitGroup
	cwg.Add(1)
	go func() {
		defer cwg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := db.Compact(); err != nil {
				t.Error(err)
				return
			}
			compactions++
			time.Sleep(d)
		}
	}()
	wg.Wait()
	close(stop)
	cwg.Wait()

	check := func(db *DB, when string) {
		t.Helper()
		if err := db.Verify(); err != nil {
			t.Fatalf("%s: %v", when, err)
		}
		n := 0
		for w := 0; w < writers; w++ {
			for k := uint64(w * keys); k < uint64((w+1)*keys); k++ {
				v, err := db.Get(k)
				if s, ok := want[w][k]; ok {
					n++
					if err != nil || string(v) != s {
						t.Errorf("%s: key %d reads %q, %v; want %q", when, k, v, err, s)
					}
				} else if err == nil {
					t.Errorf("%s: key %d, deleted, reads %q", when, k, v)
				}
			}
		}
		if got := db.Size(); got != int64(n) {
			t.Errorf("%s: %d keys; want %d", when, got, n)
		}
	}
	check(db, "after the writes")
	t.Logf("%d compactions", compactions)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(WithLogFilePath(dir), WithTimeBlockInterval(d))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	check(db, "reopened")
}
