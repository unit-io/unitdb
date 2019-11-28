package tracedb

import (
	"encoding"
	"math/rand"
	"os"

	"github.com/saffat-in/tracedb/fs"
	"github.com/saffat-in/tracedb/memdb"
)

type table struct {
	fs.FileManager
	size int64

	cache   memdb.Cache
	cacheID uint64
}

func newTable(fs fs.FileSystem, name string) (table, error) {
	fileFlag := os.O_CREATE | os.O_RDWR
	// fs := opts.FileSystem
	fileMode := os.FileMode(0666)
	fi, err := fs.OpenFile(name, fileFlag, fileMode)
	t := table{}
	if err != nil {
		return t, err
	}
	t.FileManager = fi
	stat, err := fi.Stat()
	if err != nil {
		return t, err
	}
	t.size = stat.Size()
	return t, err
}

func (t *table) newCache(cache memdb.Cache) {
	t.cache = cache
	t.cacheID = uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
}

func (t *table) extend(size uint32) (int64, error) {
	off := t.size
	if err := t.Truncate(off + int64(size)); err != nil {
		return 0, err
	}
	t.size += int64(size)

	if t.FileManager.Type() == "MemoryMap" {
		return off, t.FileManager.(*fs.OSFile).Mmap(t.size)
	} else {
		return off, nil
	}

}

func (t *table) append(data []byte) (int64, error) {
	off := t.size
	if _, err := t.WriteAt(data, off); err != nil {
		return 0, err
	}
	t.size += int64(len(data))
	if t.FileManager.Type() == "MemoryMap" {
		return off, t.FileManager.(*fs.OSFile).Mmap(t.size)
	} else {
		return off, nil
	}
}

func (t *table) writeMarshalableAt(m encoding.BinaryMarshaler, off int64) error {
	buf, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = t.WriteAt(buf, off)
	return err
}

func (t *table) readUnmarshalableAt(m encoding.BinaryUnmarshaler, size uint32, off int64) error {
	buf := make([]byte, size)
	if _, err := t.ReadAt(buf, off); err != nil {
		return err
	}
	return m.UnmarshalBinary(buf)
}
