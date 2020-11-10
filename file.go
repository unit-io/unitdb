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

package unitdb

import (
	"encoding"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"

	"github.com/unit-io/unitdb/fs"
)

// FileType represent a file type.
type FileType int

// File types.
const (
	TypeInfo FileType = iota
	TypeTimeWindow
	TypeIndex
	TypeData
	TypeLease
	TypeFilter

	TypeAll = TypeInfo | TypeTimeWindow | TypeIndex | TypeData | TypeLease | TypeFilter

	indexDir = "index"
	dataDir  = "data"
	winDir   = "window"
)

// FileDesc is a 'file descriptor'.
type FileDesc struct {
	Type FileType
	Num  int16
	fd   uintptr
}

func filePath(prefix string, fd FileDesc) string {
	name := fmt.Sprintf("%#x-%d", fd.Type, fd.Num)
	if err := ensureDir(indexDir); err != nil {
		return name
	}
	if err := ensureDir(dataDir); err != nil {
		return name
	}
	if err := ensureDir(winDir); err != nil {
		return name
	}
	switch fd.Type {
	case TypeInfo:
		suffix := fmt.Sprintf("%s.info", prefix)
		return suffix
	case TypeTimeWindow:
		suffix := fmt.Sprintf("%s%04d.win", prefix, fd.Num)
		return path.Join(winDir, suffix)
	case TypeIndex:
		suffix := fmt.Sprintf("%s%04d.index", prefix, fd.Num)
		return path.Join(indexDir, suffix)
	case TypeData:
		suffix := fmt.Sprintf("%s%04d.data", prefix, fd.Num)
		return path.Join(dataDir, suffix)
	case TypeLease:
		suffix := fmt.Sprintf("%s.lease", prefix)
		return suffix
	case TypeFilter:
		suffix := fmt.Sprintf("%s.filter", prefix)
		return suffix
	default:
		return fmt.Sprintf("%#x-%d", fd.Type, fd.Num)
	}
}

type (
	_File struct {
		fs.FileManager
		fd   FileDesc
		size int64
	}
	_FileSet struct {
		mu *sync.RWMutex

		fileMap map[int16]_File
		list    []_FileSet
		*_File
	}
)

func newFile(fsys fs.FileSystem, l int16, name string, fd FileDesc) (_FileSet, error) {
	if l == 0 {
		return _FileSet{}, errors.New("no new file")
	}
	fileFlag := os.O_CREATE | os.O_RDWR
	fileMode := os.FileMode(0666)
	f := _File{}
	fs := _FileSet{mu: new(sync.RWMutex), fileMap: make(map[int16]_File, l)}
	for i := int16(0); i < l; i++ {
		fd.Num = i
		path := filePath(name, fd)
		fi, err := fsys.OpenFile(path, fileFlag, fileMode)
		if err != nil {
			return fs, err
		}
		f.FileManager = fi
		fd.fd = fi.Fd()
		f.fd = fd
		stat, err := fi.Stat()
		if err != nil {
			return fs, err
		}
		f.size = stat.Size()
		fs.fileMap[int16(i)] = f
	}
	// atomic.StorePointer(&fs.file, f)
	fs._File = &f
	return fs, nil
}

func (f *_File) truncate(size int64) error {
	if err := f.Truncate(size); err != nil {
		return err
	}
	f.size = size
	return nil
}

func (f *_File) extend(size uint32) (int64, error) {
	off := f.size
	if err := f.Truncate(off + int64(size)); err != nil {
		return 0, err
	}
	f.size += int64(size)

	return off, nil
}

func (f *_File) write(data []byte) (int, error) {
	off := f.size
	if _, err := f.WriteAt(data, off); err != nil {
		return 0, err
	}
	f.size += int64(len(data))
	return len(data), nil
}

func (f *_File) writeMarshalableAt(m encoding.BinaryMarshaler, off int64) error {
	buf, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = f.WriteAt(buf, off)
	return err
}

func (f *_File) readUnmarshalableAt(m encoding.BinaryUnmarshaler, size uint32, off int64) error {
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, off); err != nil {
		return err
	}
	return m.UnmarshalBinary(buf)
}

func (f *_File) currSize() int64 {
	return f.size
}

func (f *_File) Size() int64 {
	stat, _ := f.Stat()
	return stat.Size()
}

func (fs *_FileSet) getFile(fd FileDesc) (*_File, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for _, fileset := range fs.list {
		if fileset.fd.Type == fd.Type {
			if fileset.fd.Num == fd.Num {
				return fileset._File, nil
			}
			if f, ok := fileset.fileMap[fd.Num]; ok {
				fileset.fileMap[fileset.fd.Num] = *fileset._File // keep current file into map
				fileset._File = &f
				return &f, nil
			}
		}
	}
	return &_File{}, errors.New("file not found")
}

func (fs *_FileSet) sync() error {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	for _, f := range fs.fileMap {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	return nil
}

func (fs *_FileSet) close() error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	for _, files := range fs.list {
		for _, f := range files.fileMap {
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

func ensureDir(dirName string) error {
	err := os.Mkdir(dirName, os.ModeDir)
	if err == nil || os.IsExist(err) {
		return nil
	} else {
		return err
	}
}
