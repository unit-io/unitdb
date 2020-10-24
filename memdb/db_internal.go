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
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/unitdb/wal"
)

const (
	dbVersion = 1.0

	logFileName = "data.log"

	nBlocks = 27

	nPoolSize = 27

	// maxMemSize value to limit maximum memory for the mem store.
	defaultMemSize = (int64(1) << 34) - 1

	// maxBufferSize sets Size of buffer to use for pooling.
	defaultBufferSize = 1 << 30 // maximum size of a buffer to use in bufferpool (1GB).

	// logSize sets Size of write ahead log.
	defaultLogSize = 1 << 32 // maximum size of log to grow before allocating free segments (4GB).
)

type (
	blockKey struct {
		blockID uint16
		key     uint64
	}
	block struct {
		data         dataTable
		m            map[uint64]int64 // map[key]offset
		sync.RWMutex                  // Read Write mutex, guards access to internal map.
	}
)

// db represents mem store.
type db struct {
	writeLockC chan struct{}

	// time mark to manage timeIDs
	*timeMark

	// tiny Batch
	tinyBatchLockC chan struct{}
	*tinyBatch
	*batchPool

	// Write ahead log
	wal *wal.WAL

	// close
	closeW sync.WaitGroup
	closeC chan struct{}
	closed uint32
	closer io.Closer
}

func (db *DB) initDb() error {
	db.internal.writeLockC = make(chan struct{}, 1)
	db.internal.timeMark = newTimeMark()
	db.internal.tinyBatchLockC = make(chan struct{}, 1)
	db.internal.tinyBatch = &tinyBatch{ID: db.internal.timeMark.newID(), doneChan: make(chan struct{})}
	db.internal.batchPool = db.newBatchPool(nPoolSize)

	// Close
	db.internal.closeC = make(chan struct{})

	go db.tinyBatchLoop(db.opts.tinyBatchWriteInterval)

	return nil
}

func (db *DB) newTinyBatch() *tinyBatch {
	tinyBatch := &tinyBatch{ID: db.internal.timeMark.newID(), doneChan: make(chan struct{})}
	return tinyBatch
}

func (db *DB) newBatchPool(maxBatches int) *batchPool {
	// There must be at least one batch.
	if maxBatches < 1 {
		maxBatches = 1
	}

	pool := &batchPool{
		db:          db,
		maxBatches:  maxBatches,
		writeQueue:  make(chan *tinyBatch, 1),
		batchQueue:  make(chan *tinyBatch),
		stoppedChan: make(chan struct{}),
	}

	// start the batch dispatcher
	go pool.dispatch()

	return pool
}

func (db *DB) close() error {
	if !db.setClosed() {
		return errClosed
	}

	// Signal all goroutines.
	time.Sleep(db.opts.tinyBatchWriteInterval)
	close(db.internal.closeC)
	db.internal.batchPool.stopWait()

	// Acquire lock.
	db.internal.writeLockC <- struct{}{}

	// Wait for all goroutines to exit.
	db.internal.closeW.Wait()

	var err error
	if db.internal.closer != nil {
		if err1 := db.internal.closer.Close(); err == nil {
			err = err1
		}
		db.internal.closer = nil
	}

	return err
}

// tinyWrite writes tiny batch to DB WAL.
func (db *DB) tinyWrite(tinyBatch *tinyBatch) error {
	// Backoff to limit excess memroy usage
	// db.blockCache.Backoff()

	logWriter, err := db.internal.wal.NewWriter()
	if err != nil {
		return err
	}

	block, ok := db.blockCache[timeID(tinyBatch.timeID())]
	if !ok {
		return nil
	}
	block.RLock()
	defer block.RUnlock()
	for _, off := range block.m {
		scratch, err := block.data.readRaw(off, 4) // read data length.
		if err != nil {
			return err
		}
		dataLen := binary.LittleEndian.Uint32(scratch[:4])
		if data, err := block.data.readRaw(off, dataLen); err == nil {
			if err := <-logWriter.Append(data[4:]); err != nil {
				return err
			}
			data = nil
		}
	}

	if err := <-logWriter.SignalInitWrite(tinyBatch.timeID()); err != nil {
		return err
	}

	return nil
}

// tinyCommit commits tiny batch to DB.
func (db *DB) tinyCommit(tinyBatch *tinyBatch) error {
	db.internal.closeW.Add(1)
	defer func() {
		tinyBatch.abort()
		db.internal.closeW.Done()
	}()

	// commit writes batches into write ahead log. The write happen synchronously.
	db.internal.writeLockC <- struct{}{}
	defer func() {
		<-db.internal.writeLockC
	}()

	if tinyBatch.len() == 0 {
		return nil
	}

	if err := db.tinyWrite(tinyBatch); err != nil {
		return err
	}

	if !tinyBatch.managed {
		db.internal.timeMark.release(tinyBatch.timeID())
	}

	return nil
}

func (db *DB) recovery(reset bool) error {
	// acquire write lock on recovery.
	db.internal.writeLockC <- struct{}{}
	defer func() {
		<-db.internal.writeLockC
	}()

	m, err := db.recoveryLog(reset)
	if err != nil {
		return err
	}
	for k, msg := range m {
		if err := db.Set(k, msg); err != nil {
			return err
		}
	}

	// reset log on successful recovery.
	return db.internal.wal.Reset()
}

// recovery recovers pending messages from log file.
func (db *DB) recoveryLog(reset bool) (map[uint64][]byte, error) {
	m := make(map[uint64][]byte) // map[key]msg

	// Make sure we have a directory
	if err := os.MkdirAll(db.opts.logFilePath, 0777); err != nil {
		return m, errors.New("db.Open, Unable to create db dir")
	}

	logOpts := wal.Options{Path: db.opts.logFilePath + "/" + logFileName, TargetSize: db.opts.logSize, BufferSize: db.opts.bufferSize}
	wal, needLogRecovery, err := wal.New(logOpts)
	if err != nil {
		wal.Close()
		return m, err
	}

	db.internal.closer = wal
	db.internal.wal = wal
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

func (db *DB) signalLogApplied(timeID int64) error {
	// signal log applied for older messages those are acknowledged or timed out.
	return db.internal.wal.SignalLogApplied(timeID)
}

// tinyBatchLoop handles tiny batches.
func (db *DB) tinyBatchLoop(interval time.Duration) {
	db.internal.closeW.Add(1)
	defer db.internal.closeW.Done()
	tinyBatchTicker := time.NewTicker(interval)
	for {
		select {
		case <-db.internal.closeC:
			tinyBatchTicker.Stop()
			return
		case <-tinyBatchTicker.C:
			if db.internal.tinyBatch.len() != 0 {
				db.internal.tinyBatchLockC <- struct{}{}
				db.internal.batchPool.write(db.internal.tinyBatch)
				db.internal.tinyBatch = db.newTinyBatch()
				<-db.internal.tinyBatchLockC
			}
		}
	}
}

// setClosed flag; return true if not already closed.
func (db *DB) setClosed() bool {
	return atomic.CompareAndSwapUint32(&db.internal.closed, 0, 1)
}

// isClosed checks whether DB was closed.
func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.internal.closed) != 0
}

// ok checks read ok status.
func (db *DB) ok() error {
	if db.isClosed() {
		return errors.New("db is closed")
	}
	return nil
}
