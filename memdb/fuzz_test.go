package memdb

import (
	"encoding/binary"
	"testing"

	"github.com/unit-io/unitdb/wal"
)

// fuzzEntry is an entry as a block holds it, and the WAL a log of a block:
// its length, its deleted flag, its key, and its value.
func fuzzEntry(flag byte, key uint64, val []byte) []byte {
	e := make([]byte, 4+1+8+len(val))
	binary.LittleEndian.PutUint32(e, uint32(len(e)))
	e[4] = flag
	binary.LittleEndian.PutUint64(e[5:], key)
	copy(e[13:], val)
	return e
}

// FuzzRecovery opens a DB on a WAL of two logs whose records are the fuzzed
// bytes, in logs with valid checksums, as a bug that writes bad entries
// leaves them. Open must fail or succeed, not panic; and a DB that opens
// must pass Verify, and answer Get.
func FuzzRecovery(f *testing.F) {
	var ref [8]byte
	binary.LittleEndian.PutUint64(ref[:], 1_000_000_000)
	f.Add(fuzzEntry(0, 1, []byte("one")), fuzzEntry(1, 1, ref[:]), int64(0))
	f.Add(append(fuzzEntry(0, 1, []byte("a")), fuzzEntry(0, 2, []byte("b"))...), fuzzEntry(1, 2, ref[:]), int64(1_000_000_000))
	f.Add(fuzzEntry(1, 3, []byte("ab")), []byte{}, int64(5))
	f.Add([]byte{2, 0, 0, 0}, []byte{20, 0, 0, 0, 0}, int64(0))
	f.Fuzz(func(t *testing.T, first, second []byte, blockID int64) {
		dir := t.TempDir()
		w, err := wal.New(wal.Options{Path: dir + "/" + logDir, BufferSize: 1 << 16})
		if err != nil {
			t.Fatal(err)
		}
		for i, rec := range [][]byte{first, second} {
			if len(rec) == 0 {
				continue
			}
			lw, err := w.NewWriter()
			if err != nil {
				t.Fatal(err)
			}
			if err := <-lw.Append(rec); err != nil {
				t.Fatal(err)
			}
			lw.SetBlockID(blockID)
			if err := <-lw.SignalInitWrite(int64(1_000_000_000 + i)); err != nil {
				t.Fatal(err)
			}
		}
		w.Close()

		db, err := Open(WithLogFilePath(dir))
		if err != nil {
			return
		}
		defer db.Close()
		if err := db.Verify(); err != nil {
			t.Fatalf("opened a DB that fails Verify: %v", err)
		}
		for k := uint64(0); k < 4; k++ {
			db.Get(k)
		}
		db.Size()
	})
}
