package fs

import (
	"os"
)

type IOFile struct {
	*os.File
}

type iofs struct{}

// FileIO is a file system backed by the io package.
var FileIO = &iofs{}

func (fs *iofs) OpenFile(name string, flag int, perm os.FileMode) (FileManager, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	iof := &IOFile{f}
	return iof, err
}

func (fs *iofs) CreateLockFile(name string, perm os.FileMode) (LockFile, bool, error) {
	return createLockFile(name, perm)
}

func (fs *iofs) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (fs *iofs) Remove(name string) error {
	return os.Remove(name)
}

type fslockfile struct {
	File
	name string
}

func (f *fslockfile) Unlock() error {
	if err := f.Close(); err != nil {
		return err
	}
	return FileIO.Remove(f.name)
}

func (fs *IOFile) Type() string {
	return "FileIO"
}

func (f *IOFile) Slice(start int64, end int64) ([]byte, error) {
	buf := make([]byte, end-start)
	_, err := f.ReadAt(buf, start)
	return buf, err
}

func (f *IOFile) Close() error {
	return f.File.Close()
}

func (f *IOFile) ReadAt(p []byte, off int64) (int, error) {
	return f.File.ReadAt(p, off)
}

func (f *IOFile) WriteAt(p []byte, off int64) (int, error) {
	return f.File.WriteAt(p, off)
}

func (f *IOFile) Sync() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	return nil
}
