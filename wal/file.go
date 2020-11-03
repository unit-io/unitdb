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
	"fmt"
	"os"

	"github.com/unit-io/unitdb/fs"
)

type (
	_Segment struct {
		offset int64
		size   uint32
	}
	_File struct {
		fs.FileManager
		segments   _Segments
		size       int64
		targetSize int64
	}
)

type _Segments [3]_Segment

func openFile(name string, targetSize int64) (_File, error) {
	fileFlag := os.O_CREATE | os.O_RDWR
	fileMode := os.FileMode(0666)
	fs := fs.FileIO

	fi, err := fs.OpenFile(name, fileFlag, fileMode)
	f := _File{}
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

func newSegments() _Segments {
	segments := _Segments{}
	segments[0] = _Segment{offset: int64(headerSize), size: 0}
	segments[1] = _Segment{offset: int64(headerSize), size: 0}
	return segments
}

func (sg *_Segments) currSize() uint32 {
	return sg[1].size
}

func (sg *_Segments) recoveryOffset(offset int64) int64 {
	if offset == sg[0].offset {
		offset += int64(sg[0].size)
	}
	if offset == sg[1].offset {
		offset += int64(sg[1].size)
	}
	if offset == sg[2].offset {
		offset += int64(sg[2].size)
	}
	return offset
}

func (sg *_Segments) freeSize(offset int64) uint32 {
	if offset == sg[0].offset {
		return sg[0].size
	}
	if offset == sg[1].offset {
		return sg[1].size
	}
	if offset == sg[2].offset {
		return sg[2].size
	}
	return 0
}

func (sg *_Segments) allocate(size uint32) int64 {
	off := sg[1].offset
	sg[1].size -= size
	sg[1].offset += int64(size)
	return off
}

func (sg *_Segments) free(offset int64, size uint32) (ok bool) {
	if sg[0].offset+int64(sg[0].size) == offset {
		sg[0].size += size
		return true
	}
	if sg[1].offset+int64(sg[1].size) == offset {
		sg[1].size += size
		return true
	}
	return false
}

func (sg *_Segments) swap(targetSize int64) error {
	if sg[1].size != 0 && sg[1].offset+int64(sg[1].size) == sg[2].offset {
		sg[1].size += sg[2].size
		sg[2].size = 0
	}
	if targetSize < int64(sg[0].size) {
		sg[2].offset = sg[1].offset
		sg[2].size = sg[1].size
		sg[1].offset = sg[0].offset
		sg[1].size = sg[0].size
		sg[0].size = 0
		fmt.Println("wal.Swap: segments ", sg)
	}
	return nil
}

func (f *_File) truncate(size int64) error {
	if err := f.Truncate(size); err != nil {
		return err
	}
	f.size = size
	return nil
}

func (f *_File) reset() error {
	// copy file before reseting.
	if _, err := f.Copy(); err != nil {
		return err
	}
	f.size = 0
	if err := f.truncate(0); err != nil {
		return err
	}
	if _, err := f.Seek(0, 0); err != nil {
		return err
	}
	return nil
}

func (f *_File) allocate(size uint32) (int64, error) {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	// Allocation to free segment happens when log reaches its target size to avoid fragmentation.
	if f.targetSize > (f.size+int64(size)) || f.segments.currSize() < size {
		off := f.size
		if err := f.Truncate(off + int64(size)); err != nil {
			return 0, err
		}
		f.size += int64(size)
		return off, nil
	}
	off := f.segments.allocate(size)

	return off, nil
}

func (f *_File) readAt(buf []byte, off int64) (int, error) {
	return f.ReadAt(buf, off)
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

func (f *_File) Size() int64 {
	return f.size
}
