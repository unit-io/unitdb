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
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/wal"
)

const (
	dbVersion = 1.0

	logDir = "logs"

	nPoolSize = 27

	nBlocks = 27

	// nLocks sets maximum concurent timeLocks.
	nLocks = 100000

	// defaultMemdbSize sets maximum memory usage limit for the DB.
	defaultMemdbSize = (int64(1) << 34) - 1

	// defaultBufferSize sets Size of buffer to use for pooling.
	defaultBufferSize = 1 << 30 // maximum size of a buffer to use in bufferpool (1GB).

	// defaultLogSize sets Size of write ahead log.
	defaultLogSize = 1 << 32 // maximum size of log to grow before allocating from free segments (4GB).
)

// _DB represents a mem store.
type _DB struct {
	// The db start time.
	start time.Time

	// The metrics to measure timeseries on DB events.
	meter *Meter

	// time mark to manage time records written to WAL.
	timeMark *_TimeMark
	timeLock _TimeLock

	// tiny Log
	timeRef    _TimeID
	logManager *_TinyLogManager

	// buffer pool
	buffer *bpool.BufferPool

	// Write ahead log
	wal *wal.WAL

	// close
	closed uint32
	closer io.Closer
}

func (db *DB) close() error {
	if !db.setClosed() {
		return errClosed
	}

	db.internal.logManager.closeWait()

	var err error
	if db.internal.closer != nil {
		if err1 := db.internal.closer.Close(); err1 != nil {
			err = err1
		}
		db.internal.closer = nil
	}
	// Stop the pool's drain goroutine, or every DB opened leaks one.
	db.internal.buffer.Done()

	db.internal.meter.UnregisterAll()

	return err
}

// newTimeLock set timeRef and returns timeLock.
func (db *DB) newTimeLock(timeRef _TimeID) _Internal {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.internal.timeRef = timeRef
	return db.internal.timeLock.getTimeLock(timeRef)
}

// timeLock returns timeLock for the current timeRef.
func (db *DB) timeLock() _Internal {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.internal.timeLock.getTimeLock(db.internal.timeRef)
}

func (db *DB) timeID() _TimeID {
	return db.internal.logManager.timeID()
}

// blockKey gets blockKey for the Key using consistent hashing.
func (db *DB) blockKey(key uint64) _BlockKey {
	return _BlockKey(db.consistent.FindBlock(key))
}

func (db *DB) cap() float64 {
	return db.internal.buffer.Capacity()
}

func (db *DB) addTimeBlock(timeID _TimeID) (ok bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.timeBlocks[timeID]; !ok {
		db.timeBlocks[timeID] = &_Block{data: db.internal.buffer.Get(), records: make(map[_Key]int64)}
		return true
	}

	return false
}

func (db *DB) timeBlock(timeID _TimeID) (*_Block, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if b, ok := db.timeBlocks[timeID]; ok {
		return b, true
	}

	return nil, false
}

// blocks returns a snapshot of the time blocks so callers can lock each block
// without holding db.mu. Put holds a block lock while acquiring db.mu, so
// holding db.mu while acquiring a block lock would deadlock.
func (db *DB) blocks() []*_Block {
	db.mu.RLock()
	defer db.mu.RUnlock()
	blocks := make([]*_Block, 0, len(db.timeBlocks))
	for _, b := range db.timeBlocks {
		blocks = append(blocks, b)
	}

	return blocks
}

// addTimeFilter records that the time block holds keys of the key's block key.
func (db *DB) addTimeFilter(timeID _TimeID, key uint64) error {
	db.mu.RLock()
	r, ok := db.timeFilters[db.blockKey(key)]
	db.mu.RUnlock()
	if !ok {
		return nil
	}
	r.Lock()
	r.timeRecords[timeID] = struct{}{}
	r.Unlock()

	return nil
}

// removeTimeFilter forgets a released time block, so lookups don't keep
// visiting blocks that are gone.
func (db *DB) removeTimeFilter(timeID _TimeID) {
	for _, r := range db.timeFilters {
		r.Lock()
		delete(r.timeRecords, timeID)
		r.Unlock()
	}
}

// move moves the entry to the new block
func (db *DB) move(timeID _TimeID, key uint64) error {
	newTimeID := db.timeID()
	// add deleted key to new time block to persist deleted entry to the WAL.
	dkey := iKey(true, key)
	newBlock, ok := db.timeBlock(newTimeID)
	if !ok {
		return errForbidden
	}
	newBlock.Lock()
	defer newBlock.Unlock()

	rawTimeID := make([]byte, 8)
	binary.LittleEndian.PutUint64(rawTimeID[:8], uint64(timeID))

	newBlock.records[dkey] = int64(newTimeID)
	return newBlock.put(dkey, rawTimeID)
}

// tinyWrite writes tiny log to the WAL.
func (db *DB) tinyWrite(tinyLog *_TinyLog) error {
	timeLock := db.newTimeLock(tinyLog.ID())
	timeLock.Lock()
	defer timeLock.Unlock()
	block, ok := db.timeBlock(tinyLog.timeID())
	if !ok {
		// all records has already deleted and nothing to write.
		return nil
	}
	block.RLock()
	blockSize := block.size()
	if blockSize == 0 {
		// freed; nothing to write.
		block.RUnlock()
		return nil
	}
	log, err := block.data.Slice(block.lastOffset, blockSize)
	block.RUnlock()
	if err != nil {
		return err
	}
	if len(log) == 0 {
		// nothing to write
		return nil
	}
	logWriter, err := db.internal.wal.NewWriter()
	if err != nil {
		return err
	}

	if err := <-logWriter.Append(log); err != nil {
		return err
	}
	if err := <-logWriter.SignalInitWrite(int64(tinyLog.ID())); err != nil {
		return err
	}

	block.Lock()
	defer block.Unlock()
	block.lastOffset = blockSize
	block.timeRefs = append(block.timeRefs, tinyLog.ID())

	return nil
}

// tinyCommit commits tiny log to DB.
func (db *DB) tinyCommit(tinyLog *_TinyLog) error {
	defer tinyLog.abort()

	if err := db.tinyWrite(tinyLog); err != nil {
		return err
	}

	if !tinyLog.managed {
		db.internal.timeMark.release(tinyLog.timeID())
	}

	return nil
}

func (db *DB) releaseLog(timeID _TimeID) error {
	db.mu.RLock()
	block, ok := db.timeBlocks[timeID]
	db.mu.RUnlock()
	if !ok {
		return errEntryDoesNotExist
	}

	block.RLock()
	timeRefs := block.timeRefs
	block.RUnlock()
	for _, timeRef := range timeRefs {
		if err := db.internal.wal.SignalLogApplied(int64(timeRef)); err != nil {
			return err
		}
	}

	db.mu.Lock()
	delete(db.timeBlocks, _TimeID(timeID))
	db.internal.timeMark.timeUnref(timeID)
	db.mu.Unlock()

	// Free under the block's write lock so it waits for readers of the buffer.
	block.Lock()
	block.free(db.internal.buffer)
	block.Unlock()

	// Prune after the block is gone, and without db.mu: addTimeFilter takes the
	// filter lock after db.mu is released, so holding both here could deadlock.
	db.removeTimeFilter(timeID)

	return nil
}

// setClosed flag; return true if DB is not already closed.
func (db *DB) setClosed() bool {
	return atomic.CompareAndSwapUint32(&db.internal.closed, 0, 1)
}

// isClosed checks whether DB was closed.
func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.internal.closed) != 0
}

// ok checks DB status.
func (db *DB) ok() error {
	if db.isClosed() {
		return errors.New("db is closed")
	}
	return nil
}
