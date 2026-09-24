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
	"os"
	"sort"
	"sync"
	"time"

	"github.com/unit-io/bpool"
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
		db.timeFilters[_BlockKey(i)] = &_TimeFilter{timeRecords: make(map[_TimeID]struct{})}
	}

	if !options.logResetFlag {
		if err := db.startRecovery(); err != nil {
			wal.Close()
			return nil, err
		}
	}

	// Log Manager
	db.newLogManager(&_TinyLogOptions{poolCapacity: nPoolSize, writeInterval: options.logInterval, blockDuration: options.timeBlockDuration})

	return db, nil
}

// Close closes the underlying database.
func (db *DB) Close() error {
	if err := db.close(); err != nil {
		return err
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.timeBlocks != nil {
		db.timeBlocks = nil
		db.version = -1

	}

	return nil
}

// Keys gets all keys from DB.
func (db *DB) Keys() []uint64 {
	var keys []uint64

	for _, block := range db.blocks() {
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
	data, err := block.get(off)
	if err != nil {
		return nil, err
	}
	db.internal.meter.Gets.Inc(1)

	return data, nil
}

// Get gets data from most recent time ID for the provided key.
func (db *DB) Get(key uint64) ([]byte, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}

	// timeFilters is only written in Open, so it is read without db.mu.
	r, ok := db.timeFilters[db.blockKey(key)]
	if !ok {
		return nil, errEntryDoesNotExist
	}

	// Look in the newest time block first so the latest value wins. There are
	// only a few live time blocks, so insertion sort into a stack buffer.
	var buf [16]_TimeID
	timeIDs := buf[:0]
	r.RLock()
	for timeID := range r.timeRecords {
		timeIDs = append(timeIDs, timeID)
		for i := len(timeIDs) - 1; i > 0 && timeIDs[i] > timeIDs[i-1]; i-- {
			timeIDs[i], timeIDs[i-1] = timeIDs[i-1], timeIDs[i]
		}
	}
	r.RUnlock()

	// Resolve all candidate blocks under one db.mu read lock; taking it per
	// block makes its reader count a hot spot under parallel Gets.
	var blockBuf [16]*_Block
	blocks := blockBuf[:0]
	db.mu.RLock()
	for _, timeID := range timeIDs {
		blocks = append(blocks, db.timeBlocks[timeID])
	}
	db.mu.RUnlock()

	ikey := iKey(false, key)
	for _, block := range blocks {
		if block == nil {
			continue
		}
		block.RLock()
		off, ok := block.records[ikey]
		if !ok {
			block.RUnlock()
			continue
		}
		data, err := block.get(off)
		block.RUnlock()
		db.internal.meter.Gets.Inc(1)

		return data, err
	}

	return nil, errEntryDoesNotExist
}

// BlockIterator iterates all time blocks from DB committed to the WAL.
func (db *DB) BlockIterator(f func(timeID int64, keys []uint64) (bool, error)) (err error) {
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
				// Don't stop early on a filter miss: filters are snapshots taken when a
				// time block is first used, and older time blocks (e.g. concurrent
				// batches) can receive writes after newer ones, so a miss is not proof.
				continue
			}

			timeLock := db.timeLock()
			timeLock.RLock()
			defer timeLock.RUnlock()

			block.Lock()
			// Re-check under the write lock; a concurrent Delete may have removed the key.
			if _, ok := block.records[ikey]; !ok {
				block.Unlock()
				return errEntryDoesNotExist
			}
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

	db.internal.logManager.rotateMu.RLock()
	defer db.internal.logManager.rotateMu.RUnlock()
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
		b.unsetManaged()
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

	for _, block := range db.blocks() {
		block.RLock()
		size += block.count
		block.RUnlock()
	}

	return size
}
