package wal

import (
	"hash/crc32"
	"os"
	"testing"
)

// FuzzReadLog opens a WAL holding one log of the fuzzed bytes, as a raw
// file and inside a header with a valid checksum, which is what a bug that
// writes bad data leaves: reading it must fail or succeed, not panic.
func FuzzReadLog(f *testing.F) {
	entry := []byte{9, 0, 0, 0, 'e', 'n', 't', 'r', 'y'}
	f.Add(entry, uint32(1), int64(0))
	f.Add([]byte{3, 0, 0, 0}, uint32(2), int64(7))
	f.Add([]byte{0xff, 0xff, 0xff, 0x7f, 1}, uint32(1), int64(-1))
	f.Fuzz(func(t *testing.T, data []byte, count uint32, blockID int64) {
		for _, raw := range []bool{false, true} {
			dir := t.TempDir()
			file := data
			if !raw {
				info := _LogInfo{version: version, timeID: 1, count: count, size: uint32(len(data)), blockID: blockID}
				info.checksum = crc32.Checksum(data, crcTable)
				hdr, _ := info.MarshalBinary()
				file = append(hdr, data...)
			}
			if err := os.WriteFile(logPath(dir, 1), file, 0600); err != nil {
				t.Fatal(err)
			}
			readAll(t, dir)
		}
	})
}

func readAll(t *testing.T, dir string) {
	w, err := New(Options{Path: dir, BufferSize: 1 << 16})
	if err != nil {
		return
	}
	defer w.Close()
	r, err := w.NewReader()
	if err != nil {
		return
	}
	r.Iterator(func(timeID int64) (bool, error) {
		for {
			_, ok, err := r.Next()
			if !ok || err != nil {
				return false, nil
			}
		}
	})
}
