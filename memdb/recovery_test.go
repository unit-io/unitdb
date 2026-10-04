package memdb

import (
	"encoding/binary"
	"testing"

	"github.com/unit-io/unitdb/wal"
)

// TestRecoveryReadsDeletedPut opens a WAL whose last entry is a put marked
// deleted, as older versions wrote a put deleted before it reached the WAL.
// Recovery took it for a delete, read 8 bytes of its 2 byte value, and
// panicked.
func TestRecoveryReadsDeletedPut(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.New(wal.Options{Path: dir + "/" + logDir, BufferSize: 1 << 16})
	if err != nil {
		t.Fatal(err)
	}
	entry := func(flag byte, key uint64, val []byte) []byte {
		e := make([]byte, 4+1+8+len(val))
		binary.LittleEndian.PutUint32(e, uint32(len(e)))
		e[4] = flag
		binary.LittleEndian.PutUint64(e[5:], key)
		copy(e[13:], val)
		return e
	}
	lw, err := w.NewWriter()
	if err != nil {
		t.Fatal(err)
	}
	if err := <-lw.Append(append(entry(0, 1, []byte("kept")), entry(1, 2, []byte("ab"))...)); err != nil {
		t.Fatal(err)
	}
	if err := <-lw.SignalInitWrite(1_000_000_000); err != nil {
		t.Fatal(err)
	}
	w.Close()

	db, err := Open(WithLogFilePath(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if v, err := db.Get(1); err != nil || string(v) != "kept" {
		t.Errorf("Get(1) = %q, %v; want \"kept\"", v, err)
	}
	if v, err := db.Get(2); err == nil {
		t.Errorf("Get(2) = %q; it was deleted", v)
	}
	if err := db.Verify(); err != nil {
		t.Error(err)
	}
}
