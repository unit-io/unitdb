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
	"os"

	"github.com/unit-io/unitdb/fs"
)

type _File struct {
	fs.FileManager

	size int64
}

func newFile(fs fs.FileSystem, name string) (_File, error) {
	fileFlag := os.O_CREATE | os.O_RDWR
	fileMode := os.FileMode(0666)
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
	return f, err
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
