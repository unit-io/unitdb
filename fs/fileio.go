package fs

import (
	"os"
)

// IOFile is file system based stor for db
type IOFile struct {
	*os.File
}

type iofs struct{}

// FileIO is a file system backed by the io package.
var FileIO = &iofs{}

// Open opens file is exist or create new file
func (fs *iofs) OpenFile(name string, flag int, perm os.FileMode) (FileManager, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	iof := &IOFile{f}
	return iof, err
}

// CreateLockFile to create lock file for db
func (fs *iofs) CreateLockFile(name string, perm os.FileMode) (LockFile, bool, error) {
	return createLockFile(name, perm)
}

// State provides db state and size of file
func (fs *iofs) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

// Remove removes the file
func (fs *iofs) Remove(name string) error {
	return os.Remove(name)
}

type fslockfile struct {
	File
	name string
}

// Unlock unlocks db lock file while closing db
func (f *fslockfile) Unlock() error {
	if err := f.Close(); err != nil {
		return err
	}
	return FileIO.Remove(f.name)
}

// Type indicate type of filesystem
func (f *IOFile) Type() string {
	return "FileIO"
}

// Slice provide the data for start and end offset
func (f *IOFile) Slice(start int64, end int64) ([]byte, error) {
	buf := make([]byte, end-start)
	_, err := f.ReadAt(buf, start)
	return buf, err
}

// Close closes file on db close
func (f *IOFile) Close() error {
	return f.File.Close()
}

// ReatAt reads data from file at offset
func (f *IOFile) ReadAt(p []byte, off int64) (int, error) {
	return f.File.ReadAt(p, off)
}

// WriteAt writes data to file at the given offset
func (f *IOFile) WriteAt(p []byte, off int64) (int, error) {
	return f.File.WriteAt(p, off)
}

// Sync flush the changes to file to disk
func (f *IOFile) Sync() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	return nil
}
