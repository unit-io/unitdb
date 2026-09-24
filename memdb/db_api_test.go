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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// openTestDB opens a DB in a fresh temp dir and closes it on test cleanup.
func openTestDB(t *testing.T, opts ...Options) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(append([]Options{WithLogFilePath(dir), WithLogReset()}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, dir
}

func testVal(k uint64) []byte {
	return []byte(fmt.Sprintf("msg.%d", k))
}

func putN(t *testing.T, db *DB, start, end uint64) {
	t.Helper()
	for k := start; k < end; k++ {
		if _, err := db.Put(k, testVal(k)); err != nil {
			t.Fatal(err)
		}
	}
}

func verifyGet(t *testing.T, db *DB, start, end uint64) {
	t.Helper()
	for k := start; k < end; k++ {
		v, err := db.Get(k)
		if err != nil {
			t.Fatalf("key %d: %v", k, err)
		}
		if !reflect.DeepEqual(testVal(k), v) {
			t.Fatalf("key %d: expected %q; got %q", k, testVal(k), v)
		}
	}
}

func sortedKeys(keys []uint64) []uint64 {
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func keyRange(start, end uint64) []uint64 {
	var keys []uint64
	for k := start; k < end; k++ {
		keys = append(keys, k)
	}
	return keys
}

func TestDefaultOptions(t *testing.T) {
	o := &_Options{}
	WithDefaultOptions().set(o)

	if o.logFilePath != "/tmp/unitdb" {
		t.Errorf("logFilePath: expected /tmp/unitdb; got %s", o.logFilePath)
	}
	if o.memdbSize != defaultMemdbSize {
		t.Errorf("memdbSize: expected %d; got %d", defaultMemdbSize, o.memdbSize)
	}
	if o.bufferSize != defaultBufferSize {
		t.Errorf("bufferSize: expected %d; got %d", defaultBufferSize, o.bufferSize)
	}
	if o.logInterval != 15*time.Millisecond {
		t.Errorf("logInterval: expected 15ms; got %s", o.logInterval)
	}
	if o.timeBlockDuration != time.Second {
		t.Errorf("timeBlockDuration: expected 1s; got %s", o.timeBlockDuration)
	}
	if o.logResetFlag {
		t.Error("logResetFlag: expected false")
	}
}

func TestOptionsOverrideDefaults(t *testing.T) {
	o := &_Options{}
	for _, opt := range []Options{
		WithLogFilePath("custom"),
		WithMemdbSize(1 << 20),
		WithBufferSize(1 << 10),
		WithLogInterval(5 * time.Millisecond),
		WithTimeBlockInterval(50 * time.Millisecond),
		WithLogReset(),
	} {
		opt.set(o)
	}
	// Defaults are applied after user options in Open and must not clobber them.
	WithDefaultOptions().set(o)

	want := _Options{
		logFilePath:       "custom",
		memdbSize:         1 << 20,
		bufferSize:        1 << 10,
		logResetFlag:      true,
		logInterval:       5 * time.Millisecond,
		timeBlockDuration: 50 * time.Millisecond,
	}
	if *o != want {
		t.Fatalf("expected %+v; got %+v", want, *o)
	}
}

func TestOpenIgnoresNilOption(t *testing.T) {
	db, err := Open(WithLogFilePath(t.TempDir()), nil, WithLogReset())
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGetMissingKey(t *testing.T) {
	db, _ := openTestDB(t)

	if _, err := db.Get(42); err != errEntryDoesNotExist {
		t.Fatalf("empty db: expected %v; got %v", errEntryDoesNotExist, err)
	}

	putN(t, db, 0, 10)
	if _, err := db.Get(1000); err != errEntryDoesNotExist {
		t.Fatalf("populated db: expected %v; got %v", errEntryDoesNotExist, err)
	}
}

func TestPutEmptyValue(t *testing.T) {
	db, _ := openTestDB(t)

	if _, err := db.Put(1, nil); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 0 {
		t.Fatalf("expected empty value; got %q", v)
	}
}

func TestPutOverwriteReturnsLatest(t *testing.T) {
	db, _ := openTestDB(t)

	if _, err := db.Put(1, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Put(1, []byte("second")); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if string(v) != "second" {
		t.Fatalf("expected %q; got %q", "second", v)
	}
}

func TestPutLargeValue(t *testing.T) {
	db, _ := openTestDB(t)

	val := []byte(strings.Repeat("x", 1<<16))
	if _, err := db.Put(7, val); err != nil {
		t.Fatal(err)
	}
	v, err := db.Get(7)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(val, v) {
		t.Fatalf("expected %d byte value; got %d bytes", len(val), len(v))
	}
}

func TestDeleteMissingKey(t *testing.T) {
	db, _ := openTestDB(t)

	if err := db.Delete(42); err != errEntryDoesNotExist {
		t.Fatalf("expected %v; got %v", errEntryDoesNotExist, err)
	}
}

func TestDeleteThenGet(t *testing.T) {
	db, _ := openTestDB(t)
	putN(t, db, 0, 10)

	for k := uint64(0); k < 10; k += 2 {
		if err := db.Delete(k); err != nil {
			t.Fatal(err)
		}
	}

	for k := uint64(0); k < 10; k++ {
		v, err := db.Get(k)
		if k%2 == 0 {
			if err != errEntryDoesNotExist {
				t.Fatalf("deleted key %d: expected %v; got %v (%q)", k, errEntryDoesNotExist, err, v)
			}
			continue
		}
		if err != nil || !reflect.DeepEqual(testVal(k), v) {
			t.Fatalf("key %d: expected %q; got %q, %v", k, testVal(k), v, err)
		}
	}
	if size := db.Size(); size != 5 {
		t.Fatalf("expected 5 records; got %d", size)
	}
	if err := db.Delete(0); err != errEntryDoesNotExist {
		t.Fatalf("double delete: expected %v; got %v", errEntryDoesNotExist, err)
	}
}

func TestClosedDB(t *testing.T) {
	db, err := Open(WithLogFilePath(t.TempDir()), WithLogReset())
	if err != nil {
		t.Fatal(err)
	}
	timeID, err := db.Put(1, []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != errClosed {
		t.Errorf("Close: expected %v; got %v", errClosed, err)
	}
	if _, err := db.Put(2, []byte("v")); err == nil {
		t.Error("Put: expected error on closed db")
	}
	if _, err := db.Get(1); err == nil {
		t.Error("Get: expected error on closed db")
	}
	if _, err := db.Lookup(timeID, 1); err == nil {
		t.Error("Lookup: expected error on closed db")
	}
	if err := db.Delete(1); err == nil {
		t.Error("Delete: expected error on closed db")
	}
}

func TestLookup(t *testing.T) {
	db, _ := openTestDB(t)

	timeID, err := db.Put(5, testVal(5))
	if err != nil {
		t.Fatal(err)
	}
	v, err := db.Lookup(timeID, 5)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(testVal(5), v) {
		t.Fatalf("expected %q; got %q", testVal(5), v)
	}

	if _, err := db.Lookup(timeID, 6); err != errEntryDoesNotExist {
		t.Errorf("missing key: expected %v; got %v", errEntryDoesNotExist, err)
	}
	if _, err := db.Lookup(timeID+1, 5); err != errEntryDoesNotExist {
		t.Errorf("missing timeID: expected %v; got %v", errEntryDoesNotExist, err)
	}
}

func TestKeys(t *testing.T) {
	db, _ := openTestDB(t)

	if keys := db.Keys(); len(keys) != 0 {
		t.Fatalf("expected no keys; got %v", keys)
	}

	putN(t, db, 0, 20)
	if err := db.Delete(3); err != nil {
		t.Fatal(err)
	}

	want := append(keyRange(0, 3), keyRange(4, 20)...)
	if got := sortedKeys(db.Keys()); !reflect.DeepEqual(want, got) {
		t.Fatalf("expected %v; got %v", want, got)
	}
}

func TestFreeUnknownTimeID(t *testing.T) {
	db, _ := openTestDB(t)

	if err := db.Free(1); err != errEntryDoesNotExist {
		t.Fatalf("expected %v; got %v", errEntryDoesNotExist, err)
	}
}

func TestBatch(t *testing.T) {
	db, _ := openTestDB(t)

	err := db.Batch(func(b *Batch, completed <-chan struct{}) error {
		for k := uint64(0); k < 50; k++ {
			if err := b.Put(k, testVal(k)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	verifyGet(t, db, 0, 50)
	if size := db.Size(); size != 50 {
		t.Fatalf("expected 50 records; got %d", size)
	}
}

func TestBatchCompletedSignal(t *testing.T) {
	db, _ := openTestDB(t)

	var completed <-chan struct{}
	err := db.Batch(func(b *Batch, c <-chan struct{}) error {
		completed = c
		return b.Put(1, testVal(1))
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case <-completed:
	case <-time.After(time.Second):
		t.Fatal("commit complete channel was not closed")
	}
}

func TestBatchReturnsFnError(t *testing.T) {
	db, _ := openTestDB(t)

	wantErr := errors.New("batch failed")
	err := db.Batch(func(b *Batch, completed <-chan struct{}) error {
		if err := b.Put(1, testVal(1)); err != nil {
			return err
		}
		return wantErr
	})
	if err != wantErr {
		t.Fatalf("expected %v; got %v", wantErr, err)
	}
}

func TestBatchManagedCommitPanics(t *testing.T) {
	db, _ := openTestDB(t)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when committing a managed batch")
		}
	}()
	db.Batch(func(b *Batch, completed <-chan struct{}) error {
		return b.Commit()
	})
}

func TestNewBatchCommit(t *testing.T) {
	db, _ := openTestDB(t)

	b := db.NewBatch()
	if b.TimeID() == 0 {
		t.Fatal("expected non-zero batch timeID")
	}
	for k := uint64(0); k < 10; k++ {
		if err := b.Put(k, testVal(k)); err != nil {
			t.Fatal(err)
		}
	}
	// Write flushes the current entries and starts a new tiny log for the batch.
	if err := b.Write(); err != nil {
		t.Fatal(err)
	}
	for k := uint64(10); k < 20; k++ {
		if err := b.Put(k, testVal(k)); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Commit(); err != nil {
		t.Fatal(err)
	}

	verifyGet(t, db, 0, 20)
}

func TestBlockIterator(t *testing.T) {
	db, _ := openTestDB(t, WithLogInterval(5*time.Millisecond), WithTimeBlockInterval(20*time.Millisecond))

	putN(t, db, 0, 30)
	// Wait for the time block to be committed to the WAL and a new block to start.
	time.Sleep(100 * time.Millisecond)

	var got []uint64
	err := db.BlockIterator(func(timeID int64, keys []uint64) (bool, error) {
		if timeID == 0 {
			t.Error("expected non-zero timeID")
		}
		got = append(got, keys...)
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := keyRange(0, 30); !reflect.DeepEqual(want, sortedKeys(got)) {
		t.Fatalf("expected %v; got %v", want, got)
	}
}

func TestBlockIteratorStop(t *testing.T) {
	db, _ := openTestDB(t, WithLogInterval(5*time.Millisecond), WithTimeBlockInterval(20*time.Millisecond))

	// Spread puts across several time blocks.
	for i := uint64(0); i < 3; i++ {
		putN(t, db, i*10, i*10+10)
		time.Sleep(40 * time.Millisecond)
	}
	time.Sleep(60 * time.Millisecond)

	calls := 0
	wantErr := errors.New("stop")
	err := db.BlockIterator(func(timeID int64, keys []uint64) (bool, error) {
		calls++
		return false, wantErr
	})
	if err != wantErr {
		t.Fatalf("expected %v; got %v", wantErr, err)
	}
	if calls != 1 {
		t.Fatalf("expected iterator to stop after 1 call; got %d", calls)
	}
}

func TestRecoveryWithDeletes(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(WithLogFilePath(dir), WithLogReset())
	if err != nil {
		t.Fatal(err)
	}
	putN(t, db, 0, 20)
	for k := uint64(0); k < 10; k++ {
		if err := db.Delete(k); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(WithLogFilePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	if size := db.Size(); size != 10 {
		t.Fatalf("expected 10 records; got %d", size)
	}
	verifyGet(t, db, 10, 20)
	for k := uint64(0); k < 10; k++ {
		if _, err := db.Get(k); err != errEntryDoesNotExist {
			t.Fatalf("deleted key %d: expected %v; got %v", k, errEntryDoesNotExist, err)
		}
	}
}

func TestAll(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(WithLogFilePath(dir), WithLogReset())
	if err != nil {
		t.Fatal(err)
	}
	putN(t, db, 0, 15)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(WithLogFilePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var got []uint64
	if err := db.All(func(timeID int64, keys []uint64) (bool, error) {
		got = append(got, keys...)
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if want := keyRange(0, 15); !reflect.DeepEqual(want, sortedKeys(got)) {
		t.Fatalf("expected %v; got %v", want, got)
	}
}

func TestConcurrentPutGet(t *testing.T) {
	db, _ := openTestDB(t)

	const workers = 8
	const perWorker = 200
	var wg sync.WaitGroup
	errC := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			start := uint64(w * perWorker)
			for k := start; k < start+perWorker; k++ {
				if _, err := db.Put(k, testVal(k)); err != nil {
					errC <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errC)
	for err := range errC {
		t.Fatal(err)
	}

	if size := db.Size(); size != workers*perWorker {
		t.Fatalf("expected %d records; got %d", workers*perWorker, size)
	}
	verifyGet(t, db, 0, workers*perWorker)
}

func TestVarz(t *testing.T) {
	db, _ := openTestDB(t)

	putN(t, db, 0, 10)
	verifyGet(t, db, 0, 5)
	for k := uint64(0); k < 3; k++ {
		if err := db.Delete(k); err != nil {
			t.Fatal(err)
		}
	}

	v, err := db.Varz()
	if err != nil {
		t.Fatal(err)
	}
	if v.Puts != 10 {
		t.Errorf("Puts: expected 10; got %d", v.Puts)
	}
	if v.Gets != 5 {
		t.Errorf("Gets: expected 5; got %d", v.Gets)
	}
	if v.Dels != 3 {
		t.Errorf("Dels: expected 3; got %d", v.Dels)
	}
	if v.Count != 7 {
		t.Errorf("Count: expected 7; got %d", v.Count)
	}
	if v.Start.IsZero() || v.Now.Before(v.Start) {
		t.Errorf("unexpected start/now: %v / %v", v.Start, v.Now)
	}
}

func TestHandleVarz(t *testing.T) {
	db, _ := openTestDB(t)
	putN(t, db, 0, 4)

	rec := httptest.NewRecorder()
	db.HandleVarz(rec, httptest.NewRequest(http.MethodGet, "/varz", nil))
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json; got %s", ct)
	}
	var v Varz
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v.Puts != 4 || v.Count != 4 {
		t.Fatalf("expected 4 puts and count; got puts=%d count=%d", v.Puts, v.Count)
	}

	rec = httptest.NewRecorder()
	db.HandleVarz(rec, httptest.NewRequest(http.MethodGet, "/varz?callback=cb", nil))
	if ct := rec.Header().Get("Content-Type"); ct != "application/javascript" {
		t.Fatalf("expected application/javascript; got %s", ct)
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, "cb(") || !strings.HasSuffix(body, ")") {
		t.Fatalf("expected JSONP response wrapped in cb(...); got %s", body)
	}
}

func TestUptime(t *testing.T) {
	tests := []struct {
		d    time.Duration
		want string
	}{
		{0, "0s"},
		{45 * time.Second, "45s"},
		{2*time.Minute + 5*time.Second, "2m5s"},
		{3*time.Hour + 4*time.Minute + 5*time.Second, "3h4m5s"},
		{2*24*time.Hour + time.Hour, "2d1h0m0s"},
		{366 * 24 * time.Hour, "1y1d0h0m0s"},
	}
	for _, tt := range tests {
		if got := uptime(tt.d); got != tt.want {
			t.Errorf("uptime(%s): expected %s; got %s", tt.d, tt.want, got)
		}
	}
}

// TestOpenCloseNoGoroutineLeak checks Close stops the goroutines Open starts,
// including the buffer pool's drain goroutine.
func TestOpenCloseNoGoroutineLeak(t *testing.T) {
	dir := t.TempDir()
	open := func() {
		db, err := Open(WithLogFilePath(dir), WithLogReset())
		if err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	open() // start any process-wide goroutines first
	before := runtime.NumGoroutine()
	for i := 0; i < 20; i++ {
		open()
	}
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := runtime.NumGoroutine(); got > before {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines: %d before, %d after 20 open/close cycles\n%s", before, got, buf[:runtime.Stack(buf, true)])
	}
}
