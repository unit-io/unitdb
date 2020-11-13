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

	// block cache
	internal   *_DB
	consistent *hash.Consistent
	blockCache _BlockCache
	timeBlocks map[_BlockKey]*_TimeBlock
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

	internal := &_DB{
		start:      time.Now(),
		meter:      NewMeter(),
		timeMark:   newTimeMark(options.timeRecordInterval),
		timeLock:   newTimeLock(),
		writeLockC: make(chan struct{}, 1),

		// buffer pool
		bufPool: bpool.NewBufferPool(options.memdbSize, nil),

		// Close
		closeC: make(chan struct{}),
	}
	internal.tinyBatch = &_TinyBatch{ID: int64(internal.timeMark.newTimeID()), doneChan: make(chan struct{})}
	logOpts := wal.Options{Path: options.logFilePath + "/" + logFileName, TargetSize: options.logSize, BufferSize: options.bufferSize, Reset: options.logResetFlag}
	wal, needLogRecovery, err := wal.New(logOpts)
	if err != nil {
		wal.Close()
		return nil, err
	}

	internal.closer = wal
	internal.wal = wal

	db := &DB{
		opts:       options,
		internal:   internal,
		consistent: hash.InitConsistent(nBlocks, nBlocks),
		blockCache: make(map[_TimeID]*_Block),
		timeBlocks: make(map[_BlockKey]*_TimeBlock),
	}

	for i := 0; i < nBlocks; i++ {
		db.timeBlocks[_BlockKey(i)] = &_TimeBlock{timeRecords: make(map[_TimeID]*filter.Block), filter: filter.NewFilterGenerator()}
	}

	db.internal.batchPool = db.newBatchPool(nPoolSize)

	go db.tinyBatchLoop(db.opts.timeRecordInterval)

	if needLogRecovery || !options.logResetFlag {
		if err := db.startRecovery(); err != nil {
			return nil, err
		}
	}

	return db, nil
}

// Close closes the underlying database.
func (db *DB) Close() error {
	db.mu.Lock()
	db.mu.Unlock()

	if err := db.close(); err != nil {
		return err
	}

	if db.blockCache != nil {
		db.blockCache = nil
		db.version = -1

	}

	return nil
}

// IsOpen returns true if connection to the database has been established. It does not check if
// connection is actually live.
func (db *DB) IsOpen() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.blockCache != nil
}

// Keys gets all keys from DB.
func (db *DB) Keys() []uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var keys []uint64

	for _, block := range db.blockCache {
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
	block, ok := db.blockCache[_TimeID(timeID)]
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
	blockKey := db.blockID(key)
	r, ok := db.timeBlocks[blockKey]
	db.mu.RUnlock()
	if !ok {
		return nil, errEntryDoesNotExist
	}

	// var timeID _TimeID
	var timeRec []_TimeID
	r.RLock()
	for tmID := range r.timeRecords {
		timeRec = append(timeRec, tmID)
	}
	r.RUnlock()
	sort.Slice(timeRec[:], func(i, j int) bool {
		return timeRec[i] > timeRec[j]
	})
	for _, timeID := range timeRec {
		db.mu.RLock()
		block, ok := db.blockCache[timeID]
		db.mu.RUnlock()
		if ok {
			block.RLock()
			_, ok := block.records[iKey(false, key)]
			block.RUnlock()
			if !ok {
				r.RLock()
				fltr := r.timeRecords[timeID]
				r.RUnlock()
				if !fltr.Test(key) {
					break
				}
				continue
			}
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
	}

	return nil, errEntryDoesNotExist
}

// ForEachBlock gets all keys from DB.
func (db *DB) ForEachBlock(timeMarkExpDur time.Duration, f func(timeID int64, keys []uint64) (bool, error)) (err error) {
	var timeIDs []_TimeID
	db.mu.RLock()
	db.internal.timeMark.timeRecord = _TimeRecord{lastUnref: _TimeID(time.Now().UTC().UnixNano())}
	blocks := db.blockCache
	db.mu.RUnlock()

	for timeID := range blocks {
		if db.internal.timeMark.isReleased(timeID) {
			timeIDs = append(timeIDs, timeID)
		}
	}

	sort.Slice(timeIDs[:], func(i, j int) bool {
		return timeIDs[i] < timeIDs[j]
	})

	for _, timeID := range timeIDs {
		block := blocks[timeID]
		var keys []uint64
		block.RLock()
		for ik := range block.records {
			if ik.delFlag == 0 {
				keys = append(keys, ik.key)
			}
		}
		block.RUnlock()
		sort.Slice(keys[:], func(i, j int) bool {
			return keys[i] < keys[j]
		})
		if stop, err := f(int64(timeID), keys); stop || err != nil {
			return err
		}
	}

	go db.internal.timeMark.startReleaser(timeMarkExpDur)

	return nil
}

// Delete deletes entry from DB.
func (db *DB) Delete(key uint64) error {
	if err := db.ok(); err != nil {
		return err
	}

	db.mu.RLock()
	// Get time block
	blockKey := db.blockID(key)
	r, ok := db.timeBlocks[blockKey]
	db.mu.RUnlock()
	if !ok {
		return errEntryDoesNotExist
	}

	var timeRec []_TimeID
	r.RLock()
	for tmID := range r.timeRecords {
		timeRec = append(timeRec, tmID)
	}
	r.RUnlock()
	sort.Slice(timeRec[:], func(i, j int) bool {
		return timeRec[i] > timeRec[j]
	})
	for _, timeID := range timeRec {
		db.mu.RLock()
		block, ok := db.blockCache[timeID]
		db.mu.RUnlock()
		if ok {
			block.RLock()
			_, ok := block.records[iKey(false, key)]
			block.RUnlock()
			if !ok {
				r.RLock()
				fltr := r.timeRecords[timeID]
				r.RUnlock()
				if !fltr.Test(key) {
					break
				}
				continue
			}
			block.Lock()
			ikey := iKey(false, key)
			delete(block.records, ikey)
			block.count--
			count := block.count
			block.Unlock()

			db.delete(key)
			db.internal.meter.Dels.Inc(1)

			if count == 0 {
				return db.releaseLog(timeID)
			}
			return nil
		}
	}

	return errEntryDoesNotExist
}

// Put sets a new key-value pait to the DB.
func (db *DB) Put(key uint64, data []byte) (int64, error) {
	if err := db.ok(); err != nil {
		return 0, err
	}

	db.internal.writeLockC <- struct{}{}
	defer func() {
		<-db.internal.writeLockC
	}()

	timeID := db.internal.tinyBatch.timeID()

	db.mu.Lock()
	block, ok := db.blockCache[timeID]
	if !ok {
		block = &_Block{data: db.internal.bufPool.Get(), records: make(map[_Key]int64)}
		db.blockCache[timeID] = block
	}
	db.mu.Unlock()

	block.Lock()
	defer block.Unlock()
	ikey := iKey(false, key)
	if err := block.put(ikey, data); err != nil {
		return int64(timeID), err
	}

	db.addTimeBlock(timeID, key)

	db.internal.tinyBatch.incount()
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

	for _, block := range db.blockCache {
		block.RLock()
		for ik := range block.records {
			if ik.delFlag == 0 {
				size++
			}
		}
		block.RUnlock()
	}

	return size
}
