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

import "github.com/unit-io/bpool"

type _DataWriter struct {
	dataTable *_DataTable
	buffer    *bpool.Buffer

	leasing       map[int64]uint32 // map[offset]size
	writeComplete bool
}

func newDataWriter(dt *_DataTable, buf *bpool.Buffer) *_DataWriter {
	return &_DataWriter{dataTable: dt, buffer: buf, leasing: make(map[int64]uint32)}
}

func (w *_DataWriter) append(data []byte) (off int64, err error) {
	if len(data) == 0 {
		return 0, nil
	}

	dataLen := len(data)
	off = w.dataTable.lease.allocate(uint32(dataLen))
	if off != -1 {
		buf := make([]byte, dataLen)
		copy(buf, data)
		if _, err = w.dataTable.file.WriteAt(buf, off); err != nil {
			return 0, err
		}
		w.leasing[off] = uint32(dataLen)
		return off, err
	}
	off = w.dataTable.offset
	offset, err := w.buffer.Extend(int64(dataLen))
	if err != nil {
		return 0, err
	}
	w.dataTable.offset += int64(dataLen)
	if _, err := w.buffer.WriteAt(data, offset); err != nil {
		return 0, err
	}
	return off, err
}

func (w *_DataWriter) write() (int, error) {
	n, err := w.dataTable.file.write(w.buffer.Bytes())
	if err != nil {
		return 0, err
	}
	w.writeComplete = true
	return n, err
}

func (w *_DataWriter) rollback() error {
	for off, size := range w.leasing {
		w.dataTable.lease.freeBlock(off, size)
	}
	return nil
}
