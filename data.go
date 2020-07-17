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
	"github.com/unit-io/bpool"
)

type (
	dataTable struct {
		file
		lease *lease

		offset int64
	}

	dataWriter struct {
		*dataTable
		buffer *bpool.Buffer

		leasing       map[int64]uint32 // map[offset]size
		writeComplete bool
	}
)

func newDataWriter(dt *dataTable, buf *bpool.Buffer) *dataWriter {
	return &dataWriter{dataTable: dt, buffer: buf, leasing: make(map[int64]uint32)}
}

func (dt *dataTable) readMessage(e entry) ([]byte, []byte, error) {
	if e.cacheBlock != nil {
		return e.cacheBlock[:idSize], e.cacheBlock[e.topicSize+idSize:], nil
	}
	message, err := dt.Slice(e.msgOffset, e.msgOffset+int64(e.mSize()))
	if err != nil {
		return nil, nil, err
	}
	return message[:idSize], message[e.topicSize+idSize:], nil
}

func (dt *dataTable) readTopic(e entry) ([]byte, error) {
	if e.cacheBlock != nil {
		return e.cacheBlock[idSize : e.topicSize+idSize], nil
	}
	return dt.Slice(e.msgOffset+int64(idSize), e.msgOffset+int64(e.topicSize)+int64(idSize))
}

func (dt *dataTable) extend(size uint32) (int64, error) {
	off := dt.offset
	if _, err := dt.file.extend(size); err != nil {
		return 0, err
	}
	dt.offset += int64(size)

	return off, nil
}

func (dw *dataWriter) append(data []byte) (off int64, err error) {
	if len(data) == 0 {
		return 0, nil
	}

	dataLen := len(data)
	off = dw.lease.allocate(uint32(dataLen))
	if off != -1 {
		buf := make([]byte, dataLen)
		copy(buf, data)
		if _, err = dw.file.WriteAt(buf, off); err != nil {
			return 0, err
		}
		dw.leasing[off] = uint32(dataLen)
		return off, err
	}
	off = dw.offset
	offset, err := dw.buffer.Extend(int64(dataLen))
	if err != nil {
		return 0, err
	}
	dw.offset += int64(dataLen)
	if _, err := dw.buffer.WriteAt(data, offset); err != nil {
		return 0, err
	}
	return off, err
}

func (dw *dataWriter) write() (int, error) {
	n, err := dw.file.write(dw.buffer.Bytes())
	if err != nil {
		return 0, err
	}
	dw.writeComplete = true
	return n, err
}

func (dw *dataWriter) rollback() error {
	for off, size := range dw.leasing {
		dw.lease.freeBlock(off, size)
	}
	return nil
}
