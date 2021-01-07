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
	"sort"
	"sync"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/filter"
	"github.com/unit-io/unitdb/hash"
	"github.com/unit-io/unitdb/wal"
)

// DB represents an SSD-optimized mem store.
type DB struct {
	mu sync.RWMutex

	version int
	opts    *_Options

	// timeBlock
	internal    *_DB
	consistent  *hash.Consistent
	timeBlocks  _TimeBlocks
	timeFilters map[_BlockKey]*_TimeFilter
}

// Open initializes database.
func Open(opts ...Options) (*DB, error) {
	options := &_Options{}
	WithDefaultOptions().set(options)
	for _, opt := range opts {
		if opt != nil {
			opt.set(options)
		}
	}

	// Make sure we have a directory.
	if err := os.MkdirAll(options.logFilePath, 0777); err != nil {
		return nil, errors.New("DB.Open, Unable to create db dir")
	}

	bufPool := bpool.NewBufferPool(options.memdbSize, &bpool.Options{MaxElapsedTime: 1 * time.Second})
	internal := &_DB{
		start:    time.Now(),
		meter:    NewMeter(),
		timeMark: newTimeMark(),
		timeLock: newTimeLock(),

		// buffer pool
		buffer: bufPool,
	}
	logOpts := wal.Options{Path: options.logFilePath + "/" + logDir, BufferSize: options.bufferSize, Reset: options.logResetFlag}
	wal, err := wal.New(logOpts)
	if err != nil {
		wal.Close()
		return nil, err
	}

	internal.closer = wal
	internal.wal = wal

	db := &DB{
		opts:        options,
		internal:    internal,
		consistent:  hash.InitConsistent(nBlocks, nBlocks),
		timeBlocks:  make(map[_TimeID]*_Block),
		timeFilters: make(map[_BlockKey]*_TimeFilter),
	}

	for i := 0; i < nBlocks; i++ {
		db.timeFilters[_BlockKey(i)] = &_TimeFilter{timeRecords: make(map[_TimeID]*filter.Block), filter: filter.NewFilterGenerator()}
	}

	if !options.logResetFlag {
		if err := db.startRecovery(); err != nil {
			return nil, err
		}
	}

	// Query plan
	db.newQueryPlan()

	// Log pool
	db.newLogPool(&_LogOptions{poolCapacity: nPoolSize, writeInterval: options.logInterval, blockDuration: options.timeBlockDuration})

	return db, nil
}

// Close closes the underlying database.
func (db *DB) Close() error {
	if err := db.close(); err != nil {
		return err
	}

	if db.timeBlocks != nil {
		db.timeBlocks = nil
		db.version = -1

	}

	return nil
}

// Keys gets all keys from DB.
func (db *DB) Keys() []uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var keys []uint64

	for _, block := range db.timeBlocks {
		block.RLock()
		for ik := range block.records {
			if ik.delFlag == 0 {
				keys = append(keys, ik.key)
			}
		}
		block.RUnlock()
	}

	return keys
}

// Lookup gets data for the provided key and timeID.
func (db *DB) Lookup(timeID int64, key uint64) ([]byte, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}

	db.mu.RLock()
	block, ok := db.timeBlocks[_TimeID(timeID)]
	db.mu.RUnlock()
	if !ok {
		return nil, errEntryDoesNotExist
	}

	block.RLock()
	defer block.RUnlock()
	// Get item from block.
	off, ok := block.records[iKey(false, key)]
	if !ok {
		return nil, errEntryDoesNotExist
	}
	scratch, err := block.data.Slice(off, off+4) // read data length.
	if err != nil {
		return nil, err
	}
	dataLen := int64(binary.LittleEndian.Uint32(scratch[:4]))
	data, err := block.data.Slice(off, off+dataLen)
	if err != nil {
		return nil, err
	}
	db.internal.meter.Gets.Inc(1)

	return data[8+1+4:], nil
}

// Get gets data from most recent time ID for the provided key.
func (db *DB) Get(key uint64) ([]byte, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}

	db.mu.RLock()
	// Get time block
	blockKey := db.blockKey(key)
	db.mu.RUnlock()

	// first execute query plan
	if len(db.internal.queryPlan.timeFilters[blockKey].timeRecords) == 0 {
		if err := db.seek(key, 0); err != nil {
			return nil, err
		}
	}

	// Lookup key first for the current timeRecord.
	block, ok := db.internal.queryPlan.timeBlocks[db.internal.queryPlan.timeRcord]
	if ok {
		block.RLock()
		off, ok := block.records[iKey(false, key)]
		block.RUnlock()
		if ok {
			block.RLock()
			defer block.RUnlock()

			db.internal.meter.Gets.Inc(1)

			return block.get(off)
		}
	}

	db.mu.RLock()
	// Get time block
	r, ok := db.internal.queryPlan.timeFilters[blockKey]
	db.mu.RUnlock()
	if !ok {
		return nil, errEntryDoesNotExist
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
		block, ok := db.internal.queryPlan.timeBlocks[timeID]
		if ok {
			block.RLock()
			off, ok := block.records[iKey(false, key)]
			block.RUnlock()
			if ok {
				block.RLock()
				defer block.RUnlock()
				db.internal.meter.Gets.Inc(1)
				db.internal.queryPlan.timeRcord = timeID

				return block.get(off)
			}
			r.RLock()
			fltr := r.timeRecords[timeID]
			r.RUnlock()
			if !fltr.Test(key) {
				break
			}
		}
	}

	// reset timeBlock and start over
	db.internal.queryPlan.timeFilters[blockKey] = &_TimeFilter{timeRecords: make(map[_TimeID]*filter.Block), filter: filter.NewFilterGenerator()}

	return db.Get(key)
}

// ForEachBlock gets all keys from DB committed to WAL.
func (db *DB) ForEachBlock(f func(timeID int64, keys []uint64) (bool, error)) (err error) {
	// Get timeBlocks successfully committed to WAL.
	timeIDs := db.internal.timeMark.timeRefs(db.timeID())
	for _, timeID := range timeIDs {
		db.mu.RLock()
		block, ok := db.timeBlocks[timeID]
		db.mu.RUnlock()
		if !ok {
			continue
		}
		var keys []uint64
		block.RLock()
		for ik := range block.records {
			if ik.delFlag == 0 {
				keys = append(keys, ik.key)
			}
		}
		block.RUnlock()
		if len(keys) == 0 {
			continue
		}
		if stop, err := f(int64(timeID), keys); stop || err != nil {
			return err
		}
	}

	return nil
}

// Delete deletes entry from the DB.
// It writes deleted key into new time block to persist record into the WAL.
// If all entries are deleted from a time block then the time block is released from the WAL.
func (db *DB) Delete(key uint64) error {
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
	ikey := iKey(false, key)
	for _, timeID := range timeIDs {
		db.mu.RLock()
		block, ok := db.timeBlocks[timeID]
		db.mu.RUnlock()
		if ok {
			block.RLock()
			_, ok := block.records[ikey]
			block.RUnlock()
			if !ok {
				r.RLock()
				fltr := r.timeRecords[timeID]
				r.RUnlock()
				if !fltr.Test(key) {
					return errEntryDoesNotExist
				}
				continue
			}

			timeLock := db.timeLock()
			timeLock.RLock()
			defer timeLock.RUnlock()

			block.Lock()
			block.delete(key)
			db.internal.meter.Dels.Inc(1)
			if block.count == 0 {
				// all entries are deleted from the block,
				// now check if timeIDs for deleted entries are released.
				for ikey, timeID := range block.records {
					db.mu.RLock()
					if _, ok := db.timeBlocks[_TimeID(timeID)]; ok {
						db.move(_TimeID(timeID), ikey.key)
					}
					db.mu.RUnlock()
					delete(block.records, ikey)
				}
				// released timeblock from the WAL if all records are deleted.
				if len(block.records) == 0 && timeID < db.timeID() {
					block.Unlock()
					return db.releaseLog(timeID)
				}
			}
			block.Unlock()

			return db.move(timeID, key)
		}
	}

	return errEntryDoesNotExist
}

// Put inserts a new key-value pair to the DB.
func (db *DB) Put(key uint64, data []byte) (int64, error) {
	if err := db.ok(); err != nil {
		return 0, err
	}

	timeID := db.timeID()
	db.mu.RLock()
	block, ok := db.timeBlocks[timeID]
	db.mu.RUnlock()
	if !ok {
		return 0, errForbidden
	}

	block.Lock()
	defer block.Unlock()
	ikey := iKey(false, key)
	if err := block.put(ikey, data); err != nil {
		return int64(timeID), err
	}
	db.addTimeFilter(timeID, key)

	db.internal.meter.Puts.Inc(1)

	return int64(timeID), nil
}

// NewBatch returns unmanaged Batch so caller can perform Put, Write, Commit, Abort to the Batch.
func (db *DB) NewBatch() *Batch {
	return db.batch()
}

// Batch executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is written.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the write is
// returned from the Batch() method.
//
// Attempting to manually commit or rollback within the function will cause a panic.
func (db *DB) Batch(fn func(*Batch, <-chan struct{}) error) error {
	b := db.batch()

	b.setManaged()
	// If an error is returned from the function then rollback and return error.
	if err := fn(b, b.commitComplete); err != nil {
		b.Abort()
		close(b.commitComplete)
		return err
	}
	b.unsetManaged()

	return b.Commit()
}

// Free frees time block from DB for a provided time ID and releases block from WAL.
func (db *DB) Free(timeID int64) error {
	return db.releaseLog(_TimeID(timeID))
}

// Size returns the total number of entries in DB.
func (db *DB) Size() int64 {
	size := int64(0)
	db.mu.RLock()
	defer db.mu.RUnlock()

	for _, block := range db.timeBlocks {
		block.RLock()
		size += block.count
		block.RUnlock()
	}

	return size
}
