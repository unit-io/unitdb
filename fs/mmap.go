package fs

import (
	"errors"
	"os"
)

const (
	initialMmapSize = 1024 << 20
)

var (
	// ErrUnmappedMemory is returned when a function is called on unmapped memory
	ErrUnmappedMemory = errors.New("unmapped memory")

	// ErrIndexOutOfBound is returned when given offset out of mapped region
	ErrIndexOutOfBound = errors.New("offset out of mapped region")
)

type OSFile struct {
	*os.File
	data     []byte
	mmapSize int64
}

type osfs struct{}

// OS is a file system backed by the os package.
var MemoryMap = &osfs{}

// Open opens file is exist or create new file
func (fs *osfs) OpenFile(name string, flag int, perm os.FileMode) (FileManager, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	mf := &OSFile{f, nil, 0}
	if stat.Size() > 0 {
		if err := mf.Mmap(stat.Size()); err != nil {
			return nil, err
		}
	}
	return mf, err
}

// CreateLockFile to create lock file for db
func (fs *osfs) CreateLockFile(name string, perm os.FileMode) (LockFile, bool, error) {
	return createLockFile(name, perm)
}

// State provides db state and size of file
func (fs *osfs) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

// Remove removes the file
func (fs *osfs) Remove(name string) error {
	return os.Remove(name)
}

type oslockfile struct {
	File
	path string
}

// Unlock unlocks db lock file while closing db
func (f *oslockfile) Unlock() error {
	if err := os.Remove(f.path); err != nil {
		return err
	}
	return f.Close()
}

// Type indicate type of filesystem
func (f *OSFile) Type() string {
	return "MemoryMap"
}

// Slice provide the data for start and end offset
func (f *OSFile) Slice(start int64, end int64) ([]byte, error) {
	if f.data == nil {
		return nil, os.ErrClosed
	}
	return f.data[start:end], nil
}

//Close deletes the memory mapped region, flushes any remaining changes
func (f *OSFile) Close() error {
	if f.data != nil {
		if err := munmap(f.data); err != nil {
			return nil
		}
		f.data = nil
	}
	return f.File.Close()
}

// Mmap memory maps file
func (f *OSFile) Mmap(fileSize int64) error {
	mmapSize := f.mmapSize

	if fileSize != 0 && mmapSize >= fileSize {
		return nil
	}

	if mmapSize == 0 {
		mmapSize = initialMmapSize
		if mmapSize < fileSize {
			mmapSize = fileSize
		}
	} else {
		if err := munmap(f.data); err != nil {
			return err
		}
		mmapSize *= 2
	}

	data, mappedSize, err := mmap(f.File, fileSize, mmapSize)
	if err != nil {
		return err
	}

	madviceRandom(data)

	f.data = data
	f.mmapSize = mappedSize
	return nil
}
