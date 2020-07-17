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
	"io"
	"os"
)

// File is the interface compatible with os.File.
type File interface {
	io.Closer
	io.ReaderAt
	io.WriterAt

	Stat() (os.FileInfo, error)
	Sync() error
	Truncate(size int64) error
	Seek(offset int64, whence int) (ret int64, err error)
}

// FileManager is an interface to support different types of storage such file based storage or a memory-mapped file storage.
type FileManager interface {
	File
	Type() string
	Slice(start int64, end int64) ([]byte, error)
}

// LockFile represents a lock file.
type LockFile interface {
	Unlock() error
}

// FileSystem represents a virtual file system.
type FileSystem interface {
	OpenFile(name string, flag int, perm os.FileMode) (FileManager, error)
	CreateLockFile(name string) (LockFile, error)
	Stat(name string) (os.FileInfo, error)
	Remove(name string) error
}
