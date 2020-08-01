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
	"encoding/binary"
	"errors"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/uid"
)

// Reader reads logs from WAL.
// Reader reader is a simple iterator over log data
type Reader struct {
	Id          uid.LID
	logData     []byte
	blockOffset int64

	entryCount uint32

	buffer *bpool.Buffer

	wal *WAL
}

// NewReader returns new log reader to read logs from WAL
func (wal *WAL) NewReader() (*Reader, error) {
	if err := wal.ok(); err != nil {
		return &Reader{wal: wal}, err
	}
	r := &Reader{
		Id:  uid.NewLID(),
		wal: wal,
	}

	r.buffer = wal.bufPool.Get()
	return r, nil
}

// Read reads log written to the WAL but fully applied. It returns Reader iterator
func (r *Reader) Read(f func(bool) (bool, error)) (err error) {
	// release and merge logs before read and after read
	r.wal.releaseLogs()
	r.wal.mu.RLock()
	defer func() {
		r.wal.mu.RUnlock()
		r.wal.releaseLogs()
		r.wal.bufPool.Put(r.buffer)
	}()
	readCount := uint32(0)
	idx := 0
	l := len(r.wal.logs)
	if l == 0 {
		return nil
	}
	fileOff := r.wal.logs[0].offset
	size := r.wal.logFile.Size() - fileOff
	if size > r.wal.opts.BufferSize {
		size = r.wal.opts.BufferSize
	}
	for {
		r.buffer.Reset()
		offset := int64(0)

		if _, err := r.buffer.Extend(size); err != nil {
			return err
		}
		if _, err := r.wal.logFile.readAt(r.buffer.Internal(), fileOff); err != nil {
			return err
		}
		for i := idx; i < l; i++ {
			ul := r.wal.logs[i]
			if ul.entryCount == 0 || ul.status != logStatusWritten {
				idx++
				continue
			}
			if size < ul.size {
				size = ul.size
				break
			}
			if size-offset < ul.size {
				fileOff = ul.offset
				size = r.wal.logFile.Size() - ul.offset
				break
			}
			data, err := r.buffer.Slice(offset+int64(logHeaderSize), offset+ul.size)
			if err != nil {
				return err
			}
			readCount += ul.entryCount
			r.entryCount = ul.entryCount
			r.logData = data
			r.blockOffset = 0
			if stop, err := f(idx == l-1); stop || err != nil {
				return err
			}
			offset += ul.size
			offset += r.wal.logFile.segments.freeSize(ul.offset + ul.size)
			if r.wal.logSeqApplied < ul.seq {
				r.wal.logSeqApplied = ul.seq
			}
			idx++
		}
		if idx == l {
			break
		}
	}
	// fmt.Println("wal.Read: readCount ", readCount)
	return nil
}

// Count returns entry count in the current reader
func (r *Reader) Count() uint32 {
	return r.entryCount
}

// Next returns next record from the log data iterator or false if iteration is done
func (r *Reader) Next() ([]byte, bool, error) {
	if r.entryCount == 0 {
		return nil, false, nil
	}
	r.entryCount--
	logData := r.logData[r.blockOffset:]
	dataLen := binary.LittleEndian.Uint32(logData[0:4])
	if uint32(len(logData)) < dataLen {
		return nil, false, errors.New("logData error")
	}
	r.blockOffset += int64(dataLen)
	return logData[4:dataLen], true, nil
}
