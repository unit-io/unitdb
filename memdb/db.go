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

	"github.com/unit-io/unitdb/filter"
	"github.com/unit-io/unitdb/hash"
)

// DB represents an SSD-optimized store.
type DB struct {
	mu sync.RWMutex

	version int
	opts    *_Options

	// blockcache
	internal   *_DB
	consistent *hash.Consistent
	blockCache _BlockCache
	timeBlocks map[_BlockKey]*_BlockRecord
}

// Open initializes database connection.
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
		writeLockC:     make(chan struct{}, 1),
		tinyBatchLockC: make(chan struct{}, 1),
		timeMark:       newTimeMark(options.tinyBatchWriteInterval),

		// Close
		closeC: make(chan struct{}),
	}
	internal.tinyBatch = &_TinyBatch{ID: int64(internal.timeMark.newTimeID()), doneChan: make(chan struct{})}

	db := &DB{
		opts:       options,
		internal:   internal,
		consistent: hash.InitConsistent(nBlocks, nBlocks),
		blockCache: make(map[_TimeID]*_Block),
		timeBlocks: make(map[_BlockKey]*_BlockRecord),
	}

	for i := 0; i < nBlocks; i++ {
		db.timeBlocks[_BlockKey(i)] = &_BlockRecord{timeRecords: make(map[_TimeID]*filter.Block), filter: filter.NewFilterGenerator()}
	}

	db.internal.batchPool = db.newBatchPool(nPoolSize)

	go db.tinyBatchLoop(db.opts.tinyBatchWriteInterval)

	if err := db.startRecover(options.resetFlag); err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	db.mu.Lock()
	db.mu.Unlock()
	if db.blockCache != nil {
		db.blockCache = nil
		db.version = -1

	}
	return db.close()
}

// IsOpen returns true if connection to database has been established. It does not check if
// connection is actually live.
func (db *DB) IsOpen() bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.blockCache != nil
}

// Keys gets all keys from mem store.
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

// TimeID returns new timeID.
func (db *DB) TimeID() int64 {
	return int64(db.internal.tinyBatch.timeID())
}

// TimeMark sets TimeRecord and returns TimeMark.
func (db *DB) TimeMark() *TimeMark {
	db.internal.timeMark.timeRecord = _TimeRecord{lastUnref: _TimeID(time.Now().UTC().UnixNano())}
	return db.internal.timeMark
}

// Get gets data for the provided key.
func (db *DB) Get(key uint64) ([]byte, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}

	db.mu.RLock()
	// Get timeBlock
	blockKey := db.blockID(key)
	r, ok := db.timeBlocks[blockKey]
	if !ok {
		db.mu.RUnlock()
		return nil, errEntryDoesNotExist
	}

	var timeID _TimeID
	var timeRec []_TimeID
	for tmID := range r.timeRecords {
		timeRec = append(timeRec, tmID)
	}
	sort.Slice(timeRec[:], func(i, j int) bool {
		return timeRec[i] > timeRec[j]
	})
	for _, tmID := range timeRec {
		if b, ok := db.blockCache[tmID]; ok {
			b.RLock()
			if _, ok := b.records[iKey(false, key)]; ok {
				timeID = tmID
				b.RUnlock()
				break
			}
			fltr := r.timeRecords[tmID]
			if fltr.Test(key) {
				// fmt.Println("db.Get: filter test for blockKey, key ", blockKey, key, timeRec)
				b.RUnlock()
				break
			}
			b.RUnlock()
		}
	}

	block, ok := db.blockCache[timeID]
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
	scratch, err := block.data.readRaw(off, 4) // read data length.
	if err != nil {
		return nil, err
	}
	dataLen := binary.LittleEndian.Uint32(scratch[:4])
	data, err := block.data.readRaw(off, dataLen)
	if err != nil {
		return nil, err
	}
	return data[8+1+4:], nil
}

// ForEachBlock gets all keys from mem store. This method is not thread safe.
func (db *DB) ForEachBlock(f func(timeID int64, keys []uint64) (bool, error)) (err error) {
	var timeIDs []_TimeID
	blocks := db.blockCache

	for timeID := range blocks {
		timeIDs = append(timeIDs, timeID)
	}

	sort.Slice(timeIDs[:], func(i, j int) bool {
		return timeIDs[i] < timeIDs[j]
	})

	for _, timeID := range timeIDs {
		block := blocks[timeID]
		var keys []uint64
		for ik := range block.records {
			if ik.delFlag == 0 {
				keys = append(keys, ik.key)
			}
		}
		if stop, err := f(int64(timeID), keys); stop || err != nil {
			return err
		}
	}

	return nil
}

// Delete deletes entry from mem store.
func (db *DB) Delete(key uint64) error {
	if err := db.ok(); err != nil {
		return err
	}

	db.mu.RLock()
	// Get timeBlock
	blockKey := db.blockID(key)
	r, ok := db.timeBlocks[blockKey]
	if !ok {
		db.mu.RUnlock()
		return errEntryDoesNotExist
	}

	var timeID _TimeID
	var timeRec []_TimeID
	for tmID := range r.timeRecords {
		timeRec = append(timeRec, tmID)
	}
	sort.Slice(timeRec[:], func(i, j int) bool {
		return timeRec[i] > timeRec[j]
	})
	for _, tmID := range timeRec {
		if b, ok := db.blockCache[tmID]; ok {
			b.RLock()
			if _, ok := b.records[iKey(false, key)]; ok {
				timeID = tmID
				b.RUnlock()
				break
			}
			fltr := r.timeRecords[tmID]
			if fltr.Test(key) {
				// fmt.Println("db.Delete: filter test for blockKey, key ", blockKey, key, timeRec)
				b.RUnlock()
				break
			}
			b.RUnlock()
		}
	}

	block, ok := db.blockCache[timeID]
	db.mu.RUnlock()
	if !ok {
		return errEntryDoesNotExist
	}

	block.Lock()
	ikey := iKey(false, key)
	delete(block.records, ikey)
	block.count--
	count := block.count
	block.Unlock()

	db.delete(key)

	if count == 0 {
		return db.releaseLog(timeID)
	}

	return nil
}

// Put sets the value for the given key-value.
func (db *DB) Put(key uint64, data []byte) (int64, error) {
	if err := db.ok(); err != nil {
		return 0, err
	}

	db.internal.tinyBatchLockC <- struct{}{}
	defer func() {
		<-db.internal.tinyBatchLockC
	}()

	timeID := db.internal.tinyBatch.timeID()
	ikey := iKey(false, key)
	db.mu.Lock()
	block, ok := db.blockCache[timeID]
	if !ok {
		block = newBlock()
		db.blockCache[timeID] = block
	}
	db.mu.Unlock()

	block.Lock()
	defer block.Unlock()
	if err := block.put(ikey, data); err != nil {
		return int64(timeID), err
	}

	db.addTimeBlock(timeID, key)

	db.internal.tinyBatch.incount()

	// fmt.Println("db.Set: timeID, key ", timeID, key)
	return int64(timeID), nil
}

// NewBatch returns unmanaged Batch so caller can perform Append, Write, Commit to the Batch.
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

// Free frees datatable from mem store for a timeID and release log from WAL.
func (db *DB) Free(timeID int64) error {
	return db.releaseLog(_TimeID(timeID))
}

// Size returns the total number of keys in memstore.
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
