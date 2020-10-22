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

package memdb

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/wal"
)

type (
	tinyBatch struct {
		sync.RWMutex
		ID int64

		entryCount uint32
		buffer     *bpool.Buffer
	}
)

func (db *DB) newTinyBatch() *tinyBatch {
	tinyBatch := &tinyBatch{ID: int64(newTimeID(db.opts.tinyBatchWriteInterval)), buffer: db.bufPool.Get()}
	return tinyBatch
}

func (b *tinyBatch) reset(timeID int64) {
	b.Lock()
	defer b.Unlock()
	atomic.StoreInt64(&b.ID, timeID)
	atomic.StoreUint32(&b.entryCount, 0)
	b.buffer.Reset()
}

func (b *tinyBatch) timeID() int64 {
	return atomic.LoadInt64(&b.ID)
}

func (b *tinyBatch) len() uint32 {
	return atomic.LoadUint32(&b.entryCount)
}

func (b *tinyBatch) incount() uint32 {
	return atomic.AddUint32(&b.entryCount, 1)
}

// append appends message to tinyBatch for writing to log file.
func (db *DB) append(delFlag bool, k uint64, data []byte) error {
	var dBit uint8
	if delFlag {
		dBit = 1
	}

	db.writeLockC <- struct{}{}
	defer func() {
		<-db.writeLockC
	}()

	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(data)+8+4+1))

	if _, err := db.tinyBatch.buffer.Write(scratch[:]); err != nil {
		return err
	}

	// key with flag bit
	var key [9]byte
	key[0] = dBit
	binary.LittleEndian.PutUint64(key[1:], k)
	if _, err := db.tinyBatch.buffer.Write(key[:]); err != nil {
		return err
	}
	if data != nil {
		if _, err := db.tinyBatch.buffer.Write(data); err != nil {
			return err
		}
	}

	db.tinyBatch.incount()
	return nil
}

// recovery recovers pending messages from log file.
func (db *DB) recovery(reset bool) (map[uint64][]byte, error) {
	m := make(map[uint64][]byte) // map[key]msg

	// Make sure we have a directory
	if err := os.MkdirAll(db.opts.logFilePath, 0777); err != nil {
		return m, errors.New("db.Open, Unable to create db dir")
	}

	logOpts := wal.Options{Path: db.opts.logFilePath + "/" + logPostfix, TargetSize: db.opts.logSize, BufferSize: db.opts.bufferSize}
	wal, needLogRecovery, err := wal.New(logOpts)
	if err != nil {
		wal.Close()
		return m, err
	}

	db.closer = wal
	db.wal = wal
	if !needLogRecovery || reset {
		return m, nil
	}

	// start log recovery
	r, err := wal.NewReader()
	if err != nil {
		return m, err
	}
	err = r.Read(func(timeID int64) (ok bool, err error) {
		l := r.Count()
		for i := uint32(0); i < l; i++ {
			logData, ok, err := r.Next()
			if err != nil {
				return false, err
			}
			if !ok {
				break
			}
			dBit := logData[0]
			key := binary.LittleEndian.Uint64(logData[1:9])
			msg := logData[9:]
			if dBit == 1 {
				if _, exists := m[key]; exists {
					delete(m, key)
				}
			}
			m[key] = msg
		}
		return false, nil
	})

	return m, err
}

// write write tiny batch to log file
func (db *DB) writeLog() error {
	db.closeW.Add(1)
	defer func() {
		db.closeW.Done()
	}()

	if db.tinyBatch.len() == 0 {
		return nil
	}

	logWriter, err := db.wal.NewWriter()
	if err != nil {
		return err
	}
	// commit writes batches into write ahead log. The write happen synchronously.
	db.writeLockC <- struct{}{}
	defer func() {
		db.tinyBatch.reset(int64(newTimeID(db.opts.tinyBatchWriteInterval)))
		<-db.writeLockC
	}()
	offset := uint32(0)
	buf := db.tinyBatch.buffer.Bytes()
	for i := uint32(0); i < db.tinyBatch.len(); i++ {
		dataLen := binary.LittleEndian.Uint32(buf[offset : offset+4])
		data := buf[offset+4 : offset+dataLen]
		if err := <-logWriter.Append(data); err != nil {
			return err
		}
		offset += dataLen
	}

	if err := <-logWriter.SignalInitWrite(db.tinyBatch.timeID()); err != nil {
		return err
	}

	return nil
}

func (db *DB) signalLogApplied(timeID int64) error {
	// signal log applied for older messages those are acknowledged or timed out.
	return db.wal.SignalLogApplied(timeID)
}

func newTimeID(dur time.Duration) timeID {
	return timeID(time.Now().UTC().Truncate(dur).Round(time.Millisecond).Unix())
}
