package unitdb

import (
	"encoding/binary"
	"os"
	"runtime"
	"testing"
)

// corruptIndex sets the value size of the first entry of the first index
// block of a copy of the fuzz template, with the block's checksum put back,
// and returns the copy.
func corruptIndex(t *testing.T, valueSize uint32) string {
	t.Helper()
	tmpl, err := fuzzTemplateDir()
	if err != nil {
		t.Fatal(err)
	}
	dir := copyDir(t, tmpl)
	path := filePath(dir, _FileDesc{fileType: typeIndex})
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// The block's base sequence, then entries of 16 bytes: relative
	// sequence, topic size, value size, offset.
	binary.LittleEndian.PutUint32(raw[8+4:8+8], valueSize)
	putChecksum(raw[:blockSize], indexChecksumOff)
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestHugeValueSizeIsNotAllocated opens a DB with an entry whose index
// entry says its value is 2GB: reading it allocated that much before
// finding the data file shorter. Open, which checks every message, must
// refuse it without.
func TestHugeValueSizeIsNotAllocated(t *testing.T) {
	dir := corruptIndex(t, 1<<31)
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	db, err := Open(dir, WithMutable(), WithBufferSize(1<<16), WithMemdbSize(1<<20))
	runtime.ReadMemStats(&after)
	if err == nil {
		db.Close()
		t.Fatal("opened a DB with an entry past the end of the data file")
	}
	if got := after.TotalAlloc - before.TotalAlloc; got > 64<<20 {
		t.Errorf("Open allocated %d MB reading a corrupt entry", got>>20)
	}
}

// TestFreeListOverLiveData opens a DB whose free list, checksum and all,
// holds the space of live messages, as a bug freeing the wrong block would
// leave it, and writes more: new messages must not overwrite live ones.
func TestFreeListOverLiveData(t *testing.T) {
	tmpl, err := fuzzTemplateDir()
	if err != nil {
		t.Fatal(err)
	}
	dir := copyDir(t, tmpl)
	dataSize, err := os.Stat(filePath(dir, _FileDesc{fileType: typeData}))
	if err != nil {
		t.Fatal(err)
	}
	// Free blocks over the whole data file, of sizes messages fit.
	var blocks [][2]int64
	for off := int64(1); off+64 < dataSize.Size(); off += 64 {
		blocks = append(blocks, [2]int64{off, 64})
	}
	raw := make([]byte, 4+12*len(blocks)+checksumSize)
	binary.LittleEndian.PutUint32(raw, uint32(len(blocks)))
	for i, b := range blocks {
		binary.LittleEndian.PutUint64(raw[4+12*i:], uint64(b[0]))
		binary.LittleEndian.PutUint32(raw[4+12*i+8:], uint32(b[1]))
	}
	putChecksum(raw, len(raw)-checksumSize)
	if err := os.WriteFile(filePath(dir, _FileDesc{fileType: typeLease}), raw, 0600); err != nil {
		t.Fatal(err)
	}

	db, err := Open(dir, WithMutable(), WithBufferSize(1<<16), WithMemdbSize(1<<20), WithFreeBlockSize(1))
	if err != nil {
		t.Logf("open: %v", err)
		return
	}
	defer db.Close()
	before, err := db.Get(NewQuery(fuzzTopics[1]).WithLimit(1000))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 300; i++ {
		if err := db.Put(fuzzTopics[0], []byte("new")); err != nil {
			t.Fatal(err)
		}
	}
	for db.Count() < 514+300 {
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	after, err := db.Get(NewQuery(fuzzTopics[1]).WithLimit(1000))
	if err != nil {
		t.Fatalf("reading the old messages after the writes: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("%d old messages after the writes; %d before", len(after), len(before))
	}
	for i := range before {
		if string(before[i]) != string(after[i]) {
			t.Fatalf("old message %d was %q; is %q", i, before[i], after[i])
		}
	}
	if err := db.Verify(); err != nil {
		t.Fatal(err)
	}
}
