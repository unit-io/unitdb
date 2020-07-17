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

package wal

import (
	"encoding"
	"os"

	"github.com/unit-io/unitdb/fs"
)

type (
	freeBlock struct {
		offset int64
		size   int64
	}
	file struct {
		fs.FileManager
		fb         fb
		size       int64
		targetSize int64
	}
)

type fb [3]freeBlock

func openFile(name string, targetSize int64) (file, error) {
	fileFlag := os.O_CREATE | os.O_RDWR
	fileMode := os.FileMode(0666)
	fs := fs.FileIO

	fi, err := fs.OpenFile(name, fileFlag, fileMode)
	f := file{}
	if err != nil {
		return f, err
	}
	f.FileManager = fi

	stat, err := fi.Stat()
	if err != nil {
		return f, err
	}
	f.size = stat.Size()
	f.targetSize = targetSize

	return f, err
}

func newFreeBlock() fb {
	var fb fb
	fb[0] = freeBlock{offset: int64(headerSize), size: 0}
	fb[1] = freeBlock{offset: int64(headerSize), size: 0}
	return fb
}

func (fb *fb) currSize() int64 {
	return fb[1].size
}

func (fb *fb) recoveryOffset(offset int64) int64 {
	if offset == fb[0].offset {
		offset += fb[0].size
	}
	if offset == fb[1].offset {
		offset += fb[1].size
	}
	if offset == fb[2].offset {
		offset += fb[2].size
	}
	return offset
}

func (fb *fb) freeSize(offset int64) int64 {
	if offset == fb[0].offset {
		return fb[0].size
	}
	if offset == fb[1].offset {
		return fb[1].size
	}
	if offset == fb[2].offset {
		return fb[2].size
	}
	return 0
}

func (fb *fb) allocate(size uint32) int64 {
	off := fb[1].offset
	fb[1].size -= int64(size)
	fb[1].offset += int64(size)
	return off
}

func (fb *fb) free(offset, size int64) (ok bool) {
	if fb[1].offset+fb[1].size == offset {
		ok = true
		fb[1].size += size
	} else {
		if fb[0].offset+fb[0].size == offset {
			ok = true
			fb[0].size += size
		}
	}
	return ok
}

func (fb *fb) swap(targetSize int64) error {
	if fb[1].size != 0 && fb[1].offset+fb[1].size == fb[2].offset {
		fb[1].size += fb[2].size
		fb[2].size = 0
	}
	if fb[0].size > targetSize {
		fb[2].offset = fb[1].offset
		fb[2].size = fb[1].size
		fb[1].offset = fb[0].offset
		fb[1].size = fb[0].size
		fb[0].size = 0
	}
	return nil
}

func (f *file) truncate(size int64) error {
	if err := f.Truncate(size); err != nil {
		return err
	}
	f.size = size
	return nil
}

func (f *file) allocate(size uint32) (int64, error) {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	// do not allocate freeblocks until target size has reached of the log to avoid fragmentation
	if f.targetSize > (f.size+int64(size)) || f.fb.currSize() < int64(size) {
		off := f.size
		if err := f.Truncate(off + int64(size)); err != nil {
			return 0, err
		}
		f.size += int64(size)
		return off, nil
	}
	off := f.fb.allocate(size)

	return off, nil
}

func (f *file) readAt(buf []byte, off int64) (int, error) {
	return f.ReadAt(buf, off)
}

func (f *file) writeMarshalableAt(m encoding.BinaryMarshaler, off int64) error {
	buf, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = f.WriteAt(buf, off)
	return err
}

func (f *file) readUnmarshalableAt(m encoding.BinaryUnmarshaler, size uint32, off int64) error {
	buf := make([]byte, size)
	if _, err := f.ReadAt(buf, off); err != nil {
		return err
	}
	return m.UnmarshalBinary(buf)
}

func (f *file) Size() int64 {
	return f.size
}
