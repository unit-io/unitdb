package wal

import (
	"os"
	"testing"
)

func newTestWal(path string, del bool) (*WAL, bool, error) {
	logOpts := Options{Path: path + ".log", TargetSize: 1 << 27, BufferSize: 1 << 20}
	if del {
		os.Remove(logOpts.Path)
	}
	return New(logOpts)
}

func TestEmptyLog(t *testing.T) {
	wal, needRecover, err := newTestWal("test.db", true)
	if needRecover || err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	seq := wal.Seq()
	if seq != 0 {
		t.Fatal(err)
	}
}

func TestRecovery(t *testing.T) {
	wal, needRecovery, err := newTestWal("test.db", true)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	if needRecovery {
		t.Fatalf("Write ahead log non-empty")
	}

	var i byte
	var n uint8 = 255

	logWriter, err := wal.NewWriter()
	if err != nil {
		t.Fatal(err)
	}

	for i = 0; i < n; i++ {
		val := []byte("msg.")
		val = append(val, i)
		if err := <-logWriter.Append(val); err != nil {
			t.Fatal(err)
		}
	}

	if err := <-logWriter.SignalInitWrite(255); err != nil {
		t.Fatal(err)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	wal, needRecovery, err = newTestWal("test.db", false)
	if !needRecovery || err != nil {
		t.Fatal(err)
	}
}

func TestLogApplied(t *testing.T) {
	wal, _, err := newTestWal("test.db", true)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()
	var i byte
	var n uint8 = 255

	var upperSeq uint64
	logWriter, err := wal.NewWriter()
	if err != nil {
		t.Fatal(err)
	}

	for i = 0; i < n; i++ {
		val := []byte("msg.")
		val = append(val, i)
		if err := <-logWriter.Append(val); err != nil {
			t.Fatal(err)
		}
	}

	if err := <-logWriter.SignalInitWrite(255); err != nil {
		t.Fatal(err)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}
	wal, needRecovery, err := newTestWal("test.db", false)
	if !needRecovery || err != nil {
		t.Fatal(err)
	}

	err = wal.Read(func(upSeq uint64, r *Reader) (bool, error) {
		for {
			_, ok := r.Next()
			if !ok {
				break
			}
		}
		upperSeq = upSeq
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := wal.SignalLogApplied(upperSeq); err != nil {
		t.Fatal(err)
	}

	if err := wal.Close(); err != nil {
		t.Fatal(err)
	}

	wal, needRecovery, err = newTestWal("test.db", false)
	if needRecovery || err != nil {
		t.Fatal(err)
	}
}

func TestSimple(t *testing.T) {
	wal, _, err := newTestWal("test.db", true)
	if err != nil {
		t.Fatal(err)
	}
	defer wal.Close()

	var i byte
	var n uint8 = 255

	logWriter, err := wal.NewWriter()
	if err != nil {
		t.Fatal(err)
	}

	for i = 0; i < n; i++ {
		val := []byte("msg.")
		val = append(val, i)
		if err := <-logWriter.Append(val); err != nil {
			t.Fatal(err)
		}
	}

	if err := <-logWriter.SignalInitWrite(255); err != nil {
		t.Fatal(err)
	}
}
