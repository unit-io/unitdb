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
	"errors"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/filter"
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

	// tiny Log
	timeID _TimeID
	// writeLockC chan struct{}
	logPool *_LogPool

	// buffer pool
	buffer *bpool.BufferPool

	// Write ahead log
	wal *wal.WAL

	// query
	queryPlan *_LogicalPlan

	// close
	closed uint32
	closer io.Closer
}

func (db *DB) close() error {
	if !db.setClosed() {
		return errClosed
	}

	db.internal.logPool.closeWait()

	var err error
	if db.internal.closer != nil {
		if err1 := db.internal.closer.Close(); err1 != nil {
			err = err1
		}
		db.internal.closer = nil
	}

	db.internal.meter.UnregisterAll()

	return err
}

func (db *DB) timeID() _TimeID {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.internal.timeID
}

// blockKey gets blockKey for the Key using consistent hashing.
func (db *DB) blockKey(key uint64) _BlockKey {
	return _BlockKey(db.consistent.FindBlock(key))
}

func (db *DB) cap() float64 {
	return db.internal.buffer.Capacity()
}

func (db *DB) getOrCreateTimeBlock(timeID _TimeID) *_Block {
	db.mu.Lock()
	defer db.mu.Unlock()
	newBlock, ok := db.timeBlocks[timeID]
	if !ok {
		newBlock = &_Block{data: db.internal.buffer.Get(), records: make(map[_Key]int64)}
		db.internal.timeID = timeID
		db.timeBlocks[timeID] = newBlock
	}

	return newBlock
}

// addTimeFilter adds unique time block to the set.
func (db *DB) addTimeFilter(timeID _TimeID, key uint64) error {
	blockKey := db.blockKey(key)
	db.mu.RLock()
	r, ok := db.timeFilters[blockKey]
	db.mu.RUnlock()
	r.Lock()
	defer r.Unlock()
	if ok {
		if _, ok := r.timeRecords[timeID]; !ok {
			db.mu.Lock()
			r.timeRecords[timeID] = filter.NewFilterBlock(r.filter.Bytes())
			db.mu.Unlock()
		}

		// Append key to bloom filter
		r.filter.Append(key)
	}

	return nil
}

func (db *DB) newQueryPlan() {
	queryPlan := &_LogicalPlan{timeBlocks: make(map[_TimeID]*_Block), timeFilters: make(map[_BlockKey]*_TimeFilter)}
	for i := 0; i < nBlocks; i++ {
		queryPlan.timeFilters[_BlockKey(i)] = &_TimeFilter{timeRecords: make(map[_TimeID]*filter.Block), filter: filter.NewFilterGenerator()}
	}

	db.internal.queryPlan = queryPlan
}

// seek finds timeRecords and timeBlock for the provided key and cutoff duration and caches those for query.
func (db *DB) seek(key uint64, cutoff int64) error {
	if err := db.ok(); err != nil {
		return err
	}

	db.mu.RLock()
	// Get time block
	blockKey := db.blockKey(key)
	r, ok := db.timeFilters[blockKey]
	db.mu.RUnlock()
	if !ok {
		return errEntryDoesNotExist
	}

	var timeIDs []_TimeID
	r.RLock()
	for timeID := range r.timeRecords {
		timeIDs = append(timeIDs, timeID)
	}
	r.RUnlock()
	sort.Slice(timeIDs[:], func(i, j int) bool {
		return timeIDs[i] > timeIDs[j]
	})
	for _, timeID := range timeIDs {
		db.mu.RLock()
		block, ok := db.timeBlocks[timeID]
		db.mu.RUnlock()
		if ok {
			block.RLock()
			_, ok := block.records[iKey(false, key)]
			block.RUnlock()
			if ok {
				b, ok := db.internal.queryPlan.timeFilters[blockKey]
				if ok {
					b.timeRecords[timeID] = filter.NewFilterBlock(r.filter.Bytes())
				}
				db.internal.queryPlan.timeBlocks[timeID] = block
				if cutoff != 0 {
					db.internal.queryPlan.cutoff = _TimeID(cutoff)
				}
				return nil
			}
			r.RLock()
			fltr := r.timeRecords[timeID]
			r.RUnlock()
			if !fltr.Test(key) {
				return errEntryDoesNotExist
			}
		}
	}

	return errEntryDoesNotExist
}

// tinyWrite writes tiny log to the WAL.
func (db *DB) tinyWrite(tinyLog *_TinyLog) error {
	block := db.getOrCreateTimeBlock(tinyLog.timeID())
	block.RLock()
	blockSize := block.data.Size()
	log, err := block.data.Slice(block.offset, blockSize)
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
	block.offset = blockSize
	block.logs = append(block.logs, tinyLog)
	block.Unlock()

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

	for _, tinyLog := range block.logs {
		if err := db.internal.wal.SignalLogApplied(int64(tinyLog.ID())); err != nil {
			return err
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	delete(db.timeBlocks, _TimeID(timeID))

	db.internal.buffer.Put(block.data)

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
