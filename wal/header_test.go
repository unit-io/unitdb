package wal

import (
	"encoding/binary"
	"hash/crc32"
	"os"
	"testing"

	"github.com/unit-io/bpool"
)

// TestLogBlockID checks that a log records the block it is of, and that a
// log of version 2, which doesn't, still reads, with no block.
func TestLogBlockID(t *testing.T) {
	dir := t.TempDir()
	wal, err := New(Options{Path: dir, BufferSize: 1 << 8})
	if err != nil {
		t.Fatal(err)
	}
	w, err := wal.NewWriter()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-w.Append([]byte("entry")); err != nil {
		t.Fatal(err)
	}
	w.SetBlockID(77)
	if err := <-w.SignalInitWrite(1); err != nil {
		t.Fatal(err)
	}

	// A version 2 log, as written before blocks were recorded.
	data := bpool.NewBufferPool(1<<20, nil).Get()
	data.Write([]byte{9, 0, 0, 0, 'o', 'l', 'd', 'e', 'r'})
	info := _LogInfo{version: 2, timeID: 2, count: 1, size: uint32(data.Size())}
	info.checksum = crc32.Checksum(data.Bytes(), crcTable)
	hdr, _ := info.MarshalBinary()
	f, err := os.Create(logPath(dir, 2))
	if err != nil {
		t.Fatal(err)
	}
	f.Write(hdr[:logHeaderSizeV2])
	f.Write(data.Bytes())
	f.Close()
	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	wal, err = New(Options{Path: dir, BufferSize: 1 << 8})
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	r, err := wal.NewReader()
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[int64]int64)
	vals := make(map[int64]string)
	if err := r.Iterator(func(timeID int64) (bool, error) {
		got[timeID] = r.BlockID()
		v, ok, err := r.Next()
		if !ok || err != nil {
			t.Fatalf("log %d: no entry: %v", timeID, err)
		}
		vals[timeID] = string(v)
		return false, nil
	}); err != nil {
		t.Fatal(err)
	}
	if got[1] != 77 || vals[1] != "entry" {
		t.Errorf("version 3 log: block %d, entry %q; want 77, \"entry\"", got[1], vals[1])
	}
	if b, ok := got[2]; !ok || b != 0 || vals[2] != "older" {
		t.Errorf("version 2 log: read %v, block %d, entry %q; want block 0, \"older\"", ok, b, vals[2])
	}
}

// TestLogHeaderLayout checks the fields of a version 3 header at their
// offsets: version, 2 bytes; time ID, 8; entries, 4; data size, 4; CRC32C
// of the data, 4; block, 8.
func TestLogHeaderLayout(t *testing.T) {
	info := _LogInfo{version: 3, timeID: 0x0102030405060708, count: 5, size: 300, checksum: 0xdeadbeef, blockID: 1_700_000_000_000_000_000}
	raw, _ := info.MarshalBinary()
	le := binary.LittleEndian
	if len(raw) != 30 || le.Uint16(raw[0:]) != 3 || le.Uint64(raw[2:]) != 0x0102030405060708 || le.Uint32(raw[10:]) != 5 || le.Uint32(raw[14:]) != 300 || le.Uint32(raw[18:]) != 0xdeadbeef || le.Uint64(raw[22:]) != 1_700_000_000_000_000_000 {
		t.Fatalf("header fields not at their offsets: % x", raw)
	}
	var got _LogInfo
	got.UnmarshalBinary(raw)
	if got != info {
		t.Fatalf("decoded %+v; want %+v", got, info)
	}
	// Version 1, with no checksum and no block.
	v1 := raw[:18]
	le.PutUint16(v1, 1)
	got = _LogInfo{}
	got.UnmarshalBinary(v1)
	if got.version != 1 || got.timeID != info.timeID || got.count != 5 || got.size != 300 || got.checksum != 0 || got.blockID != 0 {
		t.Fatalf("version 1 decoded %+v", got)
	}
}
