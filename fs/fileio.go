/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fs

import (
	"fmt"
	"io"
	"os"
)

// IOFile is file system based store.
type IOFile struct {
	*os.File
}

type _IOFs struct{}

// FileIO is a file system backed by the io package.
var FileIO = &_IOFs{}

// Open opens file is exist or create new file.
func (fs *_IOFs) OpenFile(name string, flag int, perm os.FileMode) (FileManager, error) {
	f, err := os.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	iof := &IOFile{f}
	return iof, err
}

// CreateLockFile to create lock file.
func (fs *_IOFs) CreateLockFile(name string) (LockFile, error) {
	return newLockFile(name)
}

// State provides state and size of the file.
func (fs *_IOFs) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

// Remove removes the file.
func (fs *_IOFs) Remove(name string) error {
	return os.Remove(name)
}

// Type indicate type of filesystem.
func (f *IOFile) Type() string {
	return "FileIO"
}

// Slice provide the data for start and end offset.
func (f *IOFile) Slice(start int64, end int64) ([]byte, error) {
	buf := make([]byte, end-start)
	_, err := f.ReadAt(buf, start)
	return buf, err
}

// Close closes file.
func (f *IOFile) Close() error {
	return f.File.Close()
}

// ReadAt reads data from file at offset.
func (f *IOFile) ReadAt(p []byte, off int64) (int, error) {
	return f.File.ReadAt(p, off)
}

// WriteAt writes data to file at the given offset.
func (f *IOFile) WriteAt(p []byte, off int64) (int, error) {
	return f.File.WriteAt(p, off)
}

// Sync flush the changes from file to disk.
func (f *IOFile) Sync() error {
	if err := f.File.Sync(); err != nil {
		return err
	}
	return nil
}

// Copy copies the file to a new file.
func (f *IOFile) Copy() (int64, error) {
	if err := f.File.Sync(); err != nil {
		return 0, err
	}
	if stat, err := f.File.Stat(); err != nil || stat.Size() == int64(0) {
		return 0, err
	}
	newName := fmt.Sprintf("%s.%d", f.File.Name(), f.File.Fd())
	newFile, err := os.OpenFile(newName, os.O_CREATE|os.O_RDWR, os.FileMode(0666))
	if err != nil {
		return 0, err
	}

	buf := make([]byte, 4096)
	size := int64(0)
	for {
		n, err := f.File.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		if n == 0 {
			break
		}

		if _, err := newFile.Write(buf[:n]); err != nil {
			return 0, err
		}
		size += int64(n)
	}
	return size, err
}
