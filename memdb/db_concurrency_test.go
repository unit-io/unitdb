/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package memdb

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

const (
	nWorkers   = 8
	nPerWorker = 200
)

// fastOpts rotates tiny logs and time blocks quickly so the background
// write loop runs concurrently with the operations under test.
func fastOpts() []Options {
	return []Options{WithLogInterval(time.Millisecond), WithTimeBlockInterval(10 * time.Millisecond)}
}

// runWorkers runs fn in n goroutines and fails the test on the first error.
func runWorkers(t *testing.T, n int, fn func(w int) error) {
	t.Helper()
	var wg sync.WaitGroup
	errC := make(chan error, n)
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			if err := fn(w); err != nil {
				errC <- err
			}
		}(w)
	}
	wg.Wait()
	close(errC)
	for err := range errC {
		t.Fatal(err)
	}
}

func workerKeys(w int) (uint64, uint64) {
	start := uint64(w * nPerWorker)
	return start, start + nPerWorker
}

func TestConcurrentPutAcrossTimeBlocks(t *testing.T) {
	db, _ := openTestDB(t, fastOpts()...)

	runWorkers(t, nWorkers, func(w int) error {
		start, end := workerKeys(w)
		for k := start; k < end; k++ {
			if _, err := db.Put(k, testVal(k)); err != nil {
				return err
			}
			if k%20 == 0 {
				time.Sleep(time.Millisecond)
			}
		}
		return nil
	})

	if size := db.Size(); size != nWorkers*nPerWorker {
		t.Fatalf("expected %d records; got %d", nWorkers*nPerWorker, size)
	}
	verifyGet(t, db, 0, nWorkers*nPerWorker)
}

func TestConcurrentGet(t *testing.T) {
	db, _ := openTestDB(t)
	putN(t, db, 0, nWorkers*nPerWorker)

	runWorkers(t, nWorkers, func(w int) error {
		for k := uint64(0); k < nWorkers*nPerWorker; k++ {
			v, err := db.Get(k)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(testVal(k), v) {
				t.Errorf("key %d: expected %q; got %q", k, testVal(k), v)
			}
		}
		return nil
	})
}

func TestConcurrentPutAndGet(t *testing.T) {
	db, _ := openTestDB(t, fastOpts()...)

	// Readers only read keys their paired writer has already put.
	runWorkers(t, nWorkers, func(w int) error {
		start, end := workerKeys(w)
		for k := start; k < end; k++ {
			if _, err := db.Put(k, testVal(k)); err != nil {
				return err
			}
			v, err := db.Get(k)
			if err != nil {
				return err
			}
			if !reflect.DeepEqual(testVal(k), v) {
				t.Errorf("key %d: expected %q; got %q", k, testVal(k), v)
			}
		}
		return nil
	})
}

func TestConcurrentDelete(t *testing.T) {
	db, _ := openTestDB(t, fastOpts()...)
	putN(t, db, 0, nWorkers*nPerWorker)

	runWorkers(t, nWorkers, func(w int) error {
		start, end := workerKeys(w)
		for k := start; k < end; k++ {
			if err := db.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})

	if size := db.Size(); size != 0 {
		t.Fatalf("expected zero records; got %d", size)
	}
}

func TestConcurrentDeleteSameKey(t *testing.T) {
	db, _ := openTestDB(t)
	putN(t, db, 0, 1)

	var mu sync.Mutex
	deleted := 0
	runWorkers(t, nWorkers, func(w int) error {
		err := db.Delete(0)
		if err == errEntryDoesNotExist {
			return nil
		}
		if err != nil {
			return err
		}
		mu.Lock()
		deleted++
		mu.Unlock()
		return nil
	})

	if deleted != 1 {
		t.Fatalf("expected exactly one successful delete; got %d", deleted)
	}
	if size := db.Size(); size != 0 {
		t.Fatalf("expected zero records; got %d", size)
	}
}

func TestConcurrentBatches(t *testing.T) {
	db, _ := openTestDB(t, fastOpts()...)

	runWorkers(t, nWorkers, func(w int) error {
		start, end := workerKeys(w)
		return db.Batch(func(b *Batch, completed <-chan struct{}) error {
			for k := start; k < end; k++ {
				if err := b.Put(k, testVal(k)); err != nil {
					return err
				}
			}
			return nil
		})
	})

	if size := db.Size(); size != nWorkers*nPerWorker {
		t.Fatalf("expected %d records; got %d", nWorkers*nPerWorker, size)
	}
	verifyGet(t, db, 0, nWorkers*nPerWorker)
}

// TestInterleavedBatches writes to an older batch after a newer batch has
// started, the ordering concurrent batches produce, and checks both are readable.
func TestInterleavedBatches(t *testing.T) {
	db, _ := openTestDB(t)

	// Pick two keys in the same block so they share a time filter.
	k1 := uint64(1)
	k2 := k1 + 1
	for db.blockKey(k2) != db.blockKey(k1) {
		k2++
	}

	older := db.NewBatch()
	time.Sleep(time.Millisecond) // ensure distinct batch time IDs
	newer := db.NewBatch()
	if older.TimeID() >= newer.TimeID() {
		t.Fatalf("expected older batch timeID %d < newer %d", older.TimeID(), newer.TimeID())
	}

	if err := newer.Put(k1, testVal(k1)); err != nil {
		t.Fatal(err)
	}
	if err := older.Put(k2, testVal(k2)); err != nil {
		t.Fatal(err)
	}
	if err := older.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := newer.Commit(); err != nil {
		t.Fatal(err)
	}

	for _, k := range []uint64{k1, k2} {
		v, err := db.Get(k)
		if err != nil {
			t.Fatalf("key %d: %v", k, err)
		}
		if !reflect.DeepEqual(testVal(k), v) {
			t.Fatalf("key %d: expected %q; got %q", k, testVal(k), v)
		}
	}
	if err := db.Delete(k2); err != nil {
		t.Fatalf("delete key %d: %v", k2, err)
	}
}

func TestConcurrentBatchesAndPuts(t *testing.T) {
	db, _ := openTestDB(t, fastOpts()...)

	// Even workers use batches, odd workers use Put.
	runWorkers(t, nWorkers, func(w int) error {
		start, end := workerKeys(w)
		if w%2 == 0 {
			return db.Batch(func(b *Batch, completed <-chan struct{}) error {
				for k := start; k < end; k++ {
					if err := b.Put(k, testVal(k)); err != nil {
						return err
					}
				}
				return nil
			})
		}
		for k := start; k < end; k++ {
			if _, err := db.Put(k, testVal(k)); err != nil {
				return err
			}
		}
		return nil
	})

	verifyGet(t, db, 0, nWorkers*nPerWorker)
}

func TestConcurrentReadersDuringWrites(t *testing.T) {
	db, _ := openTestDB(t, fastOpts()...)

	stop := make(chan struct{})
	var readers sync.WaitGroup
	// Stop readers even if a worker fails, so they don't race with Close on cleanup.
	var stopOnce sync.Once
	stopReaders := func() {
		stopOnce.Do(func() { close(stop) })
		readers.Wait()
	}
	defer stopReaders()
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			db.Size()
			db.Keys()
			if _, err := db.Varz(); err != nil {
				t.Error(err)
				return
			}
			db.BlockIterator(func(timeID int64, keys []uint64) (bool, error) {
				return false, nil
			})
		}
	}()

	runWorkers(t, nWorkers, func(w int) error {
		start, end := workerKeys(w)
		for k := start; k < end; k++ {
			if _, err := db.Put(k, testVal(k)); err != nil {
				return err
			}
			if k%3 == 0 {
				if err := db.Delete(k); err != nil {
					return err
				}
			}
		}
		return nil
	})
	stopReaders()

	var want int64
	for k := uint64(0); k < nWorkers*nPerWorker; k++ {
		if k%3 != 0 {
			want++
		}
	}
	if size := db.Size(); size != want {
		t.Fatalf("expected %d records; got %d", want, size)
	}
}

func TestConcurrentWritesRecovery(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(append([]Options{WithLogFilePath(dir), WithLogReset()}, fastOpts()...)...)
	if err != nil {
		t.Fatal(err)
	}
	runWorkers(t, nWorkers, func(w int) error {
		start, end := workerKeys(w)
		for k := start; k < end; k++ {
			if _, err := db.Put(k, testVal(k)); err != nil {
				return err
			}
		}
		return nil
	})
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(append([]Options{WithLogFilePath(dir)}, fastOpts()...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if size := db.Size(); size != nWorkers*nPerWorker {
		t.Fatalf("expected %d recovered records; got %d", nWorkers*nPerWorker, size)
	}
	verifyGet(t, db, 0, nWorkers*nPerWorker)
}

func TestCloseDuringWrites(t *testing.T) {
	db, err := Open(append([]Options{WithLogFilePath(t.TempDir()), WithLogReset()}, fastOpts()...)...)
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for w := 0; w < nWorkers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			start, end := workerKeys(w)
			for k := start; k < end; k++ {
				// Errors are expected once the DB is closed; the test checks
				// that in-flight writes neither panic nor deadlock.
				db.Put(k, testVal(k))
			}
		}(w)
	}
	time.Sleep(5 * time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- db.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close deadlocked with writes in flight")
	}
	wg.Wait()
}
