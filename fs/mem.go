package fs

import (
	"io"
	"os"
	"time"
)

type memfs struct {
	files map[string]*MemFile
}

// Mem is a file system backed by memory.
var Mem = &memfs{files: map[string]*MemFile{}}

// Open opens table if it is exist or create new memtable
func (fs *memfs) OpenFile(name string, flag int, perm os.FileMode) (FileManager, error) {
	f := fs.files[name]
	if f == nil {
		f = &MemFile{}
		fs.files[name] = f
	} else if !f.closed {
		return nil, os.ErrExist
	} else {
		f.closed = false
	}
	return f, nil
}

// CreateLockFile to create lock file for DB
func (fs *memfs) CreateLockFile(name string, perm os.FileMode) (LockFile, bool, error) {
	f, err := fs.OpenFile(name, 0, perm)
	if err != nil {
		return nil, false, err
	}
	return &memlockfile{f, name}, false, nil
}

// State provides DB state and size of file
func (fs *memfs) Stat(name string) (os.FileInfo, error) {
	if f, ok := fs.files[name]; ok {
		return f, nil
	}
	return nil, os.ErrNotExist
}

// Remove removes the file
func (fs *memfs) Remove(name string) error {
	if _, ok := fs.files[name]; ok {
		delete(fs.files, name)
		return nil
	}
	return os.ErrNotExist
}

type memlockfile struct {
	File
	name string
}

// Unlock unlocks DB lock file while closing DB
func (f *memlockfile) Unlock() error {
	if err := f.Close(); err != nil {
		return err
	}
	return Mem.Remove(f.name)
}

// MemFile mem file is used to write buffer to memory store
type MemFile struct {
	buf    []byte
	size   int64
	offset int64
	closed bool
}

// Type indicate type of filesystem
func (m *MemFile) Type() string {
	return "Mem"
}

// Close closes memtable on DB close
func (m *MemFile) Close() error {
	if m.closed {
		return os.ErrClosed
	}
	m.closed = true
	return nil
}

// ReadAt reads data from memtable at offset
func (m *MemFile) ReadAt(p []byte, off int64) (int, error) {
	if m.closed {
		return 0, os.ErrClosed
	}
	n := len(p)
	if int64(n) > m.size-off {
		return 0, io.EOF
	}
	copy(p, m.buf[off:off+int64(n)])
	return n, nil
}

// WriteAt writes data to memtable at the given offset
func (m *MemFile) WriteAt(p []byte, off int64) (int, error) {
	if m.closed {
		return 0, os.ErrClosed
	}
	n := len(p)
	if off == m.size {
		m.buf = append(m.buf, p...)
		m.size += int64(n)
	} else if off+int64(n) > m.size {
		panic("trying to write past EOF - undefined behavior")
	} else {
		copy(m.buf[off:off+int64(n)], p)
	}
	return n, nil
}

// Stat provides DB state and size of memtable
func (m *MemFile) Stat() (os.FileInfo, error) {
	if m.closed {
		return m, os.ErrClosed
	}
	return m, nil
}

// Sync flush the changes to memtable
func (m *MemFile) Sync() error {
	if m.closed {
		return os.ErrClosed
	}
	return nil
}

// Truncate resize the memtable and shrink or extend the memtable
func (m *MemFile) Truncate(size int64) error {
	if m.closed {
		return os.ErrClosed
	}
	if size > m.size {
		diff := int(size - m.size)
		m.buf = append(m.buf, make([]byte, diff)...)
	} else {
		m.buf = m.buf[:m.size]
	}
	m.size = size
	return nil
}

// Name name of the FileSystem
func (m *MemFile) Name() string {
	return ""
}

// Size provides size of the memtable in bytes
func (m *MemFile) Size() int64 {
	return m.size
}

// Mode mode of FileSystem
func (m *MemFile) Mode() os.FileMode {
	return os.FileMode(0)
}

// ModTime modtime for memtable
func (m *MemFile) ModTime() time.Time {
	return time.Now()
}

// IsDir indicates if the path is directory
func (m *MemFile) IsDir() bool {
	return false
}

// Sys empty interface
func (m *MemFile) Sys() interface{} {
	return nil
}

// Slice provide the data for start and end offset
func (m *MemFile) Slice(start int64, end int64) ([]byte, error) {
	if m.closed {
		return nil, os.ErrClosed
	}
	return m.buf[start:end], nil
}
