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

package unitdb

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var (
	infoPath   = func(dir string) string { return filepath.Join(dir, "unitdb.info") }
	indexPath  = func(dir string) string { return filepath.Join(dir, "index", "unitdb0000.index") }
	windowPath = func(dir string) string { return filepath.Join(dir, "window", "unitdb0000.win") }
	dataPath   = func(dir string) string { return filepath.Join(dir, "data", "unitdb0000.data") }
	sumPath    = func(dir string) string { return filepath.Join(dir, "unitdb.sum") }
	leasePath  = func(dir string) string { return filepath.Join(dir, "unitdb.lease") }
	filterPath = func(dir string) string { return filepath.Join(dir, "unitdb.filter") }
)

const integrityMsgs = 300

// syncedDB creates a closed DB whose messages are all on disk, spanning index
// and window blocks.
func syncedDB(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, crashOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < integrityMsgs; i++ {
		if err := db.Put(crashTopic, testMsg(i)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for db.Count() < integrityMsgs {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d synced; got %d", integrityMsgs, db.Count())
		}
		time.Sleep(100 * time.Millisecond)
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return dir
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	b := make([]byte, 1)
	if _, err := f.ReadAt(b, off); err != nil {
		t.Fatalf("read %s at %d: %v", path, off, err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b, off); err != nil {
		t.Fatal(err)
	}
}

// assertRefused checks Open fails with errCorrupted naming want, and that the
// failed Open released the lock.
func assertRefused(t *testing.T, dir, want string) {
	t.Helper()
	for attempt := 0; attempt < 2; attempt++ {
		db, err := Open(dir, crashOpts()...)
		if err == nil {
			db.Close()
			t.Fatalf("expected open to be refused for %s", want)
		}
		if !errors.Is(err, errCorrupted) {
			t.Fatalf("attempt %d: expected %v; got %v", attempt, errCorrupted, err)
		}
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error to mention %q; got %v", want, err)
		}
	}
}

func assertRestored(t *testing.T, dir string) *DB {
	t.Helper()
	db, err := Open(dir, crashOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	items, err := db.Get(NewQuery(crashTopic))
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != integrityMsgs {
		t.Fatalf("expected %d messages; got %d", integrityMsgs, len(items))
	}
	for i, item := range items {
		if want := string(testMsg(integrityMsgs - 1 - i)); string(item) != want {
			t.Fatalf("message %d: expected %q; got %q", i, want, item)
		}
	}
	return db
}

func TestOpenVerifiesCleanDB(t *testing.T) {
	dir := syncedDB(t)
	assertRestored(t, dir)
}

func TestCorruptionRefusesOpen(t *testing.T) {
	tests := []struct {
		name string
		path func(string) string
		off  int64
		want string
	}{
		{"info header", infoPath, 13, "info header"},
		{"index block", indexPath, 100, "index block"},
		{"window block", windowPath, int64(blockSize) + 20, "window block"},
		{"reserved window block", windowPath, 20, "window block"},
		{"message", dataPath, 5, "message"},
		{"message checksum", sumPath, 2 * checksumSize, "message 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := syncedDB(t)
			flipByte(t, tt.path(dir), tt.off)
			assertRefused(t, dir, tt.want)
		})
	}
}

func TestUnsupportedFormatRefusesOpen(t *testing.T) {
	dir := syncedDB(t)
	f, err := os.OpenFile(infoPath(dir), os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, fixed)
	f.ReadAt(buf, 0)
	binary.LittleEndian.PutUint32(buf[7:11], version+1)
	putChecksum(buf, infoChecksumOff)
	f.WriteAt(buf, 0)
	f.Close()

	assertRefused(t, dir, "unsupported file format version")
}

func TestCorruptWALRefusesRestore(t *testing.T) {
	dir := t.TempDir()
	// Crash before any sync, so the messages are only in the WAL.
	crashChild(t, "no-sync", dir, 0, func(string) bool { return true })
	logs, err := filepath.Glob(filepath.Join(dir, "logs", "*.log"))
	if err != nil || len(logs) == 0 {
		t.Fatalf("expected WAL logs; got %v, %v", logs, err)
	}
	flipByte(t, logs[0], 40)

	assertRefused(t, dir, "checksum mismatch")
}

func TestCorruptFilterIsRebuilt(t *testing.T) {
	dir := syncedDB(t)
	flipByte(t, filterPath(dir), 100)

	db := assertRestored(t, dir)
	// Deletes depend on the filter not ruling out synced messages.
	for i := 0; i < integrityMsgs; i++ {
		if !db.internal.filter.Test(uint64(i + 1)) {
			t.Fatalf("rebuilt filter rules out synced seq %d", i+1)
		}
	}
}

func TestZeroedFilterIsRebuilt(t *testing.T) {
	dir := syncedDB(t)
	if err := os.WriteFile(filterPath(dir), make([]byte, 20060), 0666); err != nil {
		t.Fatal(err)
	}

	db := assertRestored(t, dir)
	if !db.internal.filter.Test(1) {
		t.Fatal("an all-zero filter must not be loaded")
	}
}

func TestCorruptFreeListIsDropped(t *testing.T) {
	dir := syncedDB(t)
	if err := os.WriteFile(leasePath(dir), []byte{3, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, 0666); err != nil {
		t.Fatal(err)
	}

	assertRestored(t, dir)
}

func TestGetDetectsCorruption(t *testing.T) {
	dir := syncedDB(t)
	db, err := Open(dir, crashOpts()...)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Corrupt a message on disk while the DB is open.
	flipByte(t, dataPath(dir), 5)
	if _, err := db.Get(NewQuery(crashTopic)); !errors.Is(err, errCorrupted) {
		t.Fatalf("expected %v from Get; got %v", errCorrupted, err)
	}
}

// downgradeToFormat1 strips everything format 2 added.
func downgradeToFormat1(t *testing.T, dir string) {
	t.Helper()
	zeroChecksums := func(path string, off int) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for b := 0; b+int(blockSize) <= len(data); b += int(blockSize) {
			copy(data[b+off:b+off+checksumSize], make([]byte, checksumSize))
		}
		if err := os.WriteFile(path, data, 0666); err != nil {
			t.Fatal(err)
		}
	}
	zeroChecksums(indexPath(dir), indexChecksumOff)
	zeroChecksums(windowPath(dir), windowChecksumOff)
	if err := os.Truncate(sumPath(dir), 0); err != nil {
		t.Fatal(err)
	}
	info, err := os.ReadFile(infoPath(dir))
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(info[7:11], 1)
	copy(info[infoChecksumOff:], make([]byte, checksumSize))
	if err := os.WriteFile(infoPath(dir), info, 0666); err != nil {
		t.Fatal(err)
	}
}

func TestUpgradeFormat1(t *testing.T) {
	dir := syncedDB(t)
	downgradeToFormat1(t, dir)

	db := assertRestored(t, dir)
	if v := db.internal.dbInfo.header.version; v != version {
		t.Fatalf("expected format %d after upgrade; got %d", version, v)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The upgrade wrote checksums, so corruption is now detected.
	flipByte(t, dataPath(dir), 5)
	assertRefused(t, dir, "message")
}
