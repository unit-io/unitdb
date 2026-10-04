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
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/wal"
)

// DB represents an SSD-optimized mem store.
type DB struct {
	mu rwMutex[dbRank]

	version int
	opts    *_Options

	// timeBlock
	internal   *_DB
	timeBlocks _TimeBlocks
	// index gives the block holding each key's value (index.go).
	index *_Index
	// recovered holds the blocks recovered on open, oldest first, for All.
	recovered []_TimeID
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
	if err := os.MkdirAll(options.logFilePath, 0750); err != nil {
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
		opts:       options,
		internal:   internal,
		timeBlocks: make(map[_TimeID]*_Block),
		index:      newIndex(),
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

// Keys returns the keys the DB holds a value of.
func (db *DB) Keys() []uint64 {
	var keys []uint64
	for i := range db.index.shards {
		sh := &db.index.shards[i]
		sh.RLock()
		for key := range sh.keys {
			keys = append(keys, key)
		}
		sh.RUnlock()
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

// Get returns the key's value.
func (db *DB) Get(key uint64) ([]byte, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}

	sh := db.index.shard(key)
	sh.RLock()
	defer sh.RUnlock()
	loc, ok := sh.keys[key]
	if !ok {
		return nil, errEntryDoesNotExist
	}
	loc.block.RLock()
	defer loc.block.RUnlock()
	off, ok := loc.block.records[iKey(false, key)]
	if !ok {
		// Released since: synced by the engine.
		return nil, errEntryDoesNotExist
	}
	db.internal.meter.Gets.Inc(1)
	return loc.block.get(off)
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

// Delete deletes the key's value. The delete is written to the WAL in the
// current block, and the block that held the value is released once it
// holds no other.
func (db *DB) Delete(key uint64) error {
	if err := db.ok(); err != nil {
		return err
	}

	db.internal.logManager.rotateMu.RLock()
	defer db.internal.logManager.rotateMu.RUnlock()
	sh := db.index.shard(key)
	sh.Lock()
	loc, ok := sh.keys[key]
	if !ok {
		sh.Unlock()
		return errEntryDoesNotExist
	}
	emptied, err := db.replace(loc, key)
	if err == nil {
		delete(sh.keys, key)
	}
	sh.Unlock()
	if err != nil {
		return err
	}
	return db.releaseEmptied(emptied, loc.timeID)
}

// replace deletes the key's value at loc, writing the delete to the current
// block. The caller holds the key's shard, and the rotation lock for
// reading: the delete goes to the current block, which rotation would
// otherwise leave, and releaseEmpty free, between. It reports whether the
// block at loc was emptied.
func (db *DB) replace(loc _Loc, key uint64) (bool, error) {
	timeLock := db.timeLock()
	timeLock.RLock()
	defer timeLock.RUnlock()
	cur, ok := db.timeBlock(db.timeID())
	if !ok {
		return false, errForbidden
	}
	// The delete goes to the WAL even if it empties the block: the block's
	// logs stay there until the delete's go (applyLogs).
	return db.deleteEntry(loc.block, loc.timeID, cur, key)
}

// releaseEmptied releases a block a delete emptied, unless writes may still
// go to it, as they do to the current block; one with more to write is
// released once it is (releaseEmpty). A batch's block, written, takes no
// more writes, though its time ID may be past the current block's: it was
// left, emptied, for good. The caller holds no index shard: releasing takes
// them.
func (db *DB) releaseEmptied(emptied bool, timeID _TimeID) error {
	if !emptied || timeID == db.timeID() {
		return nil
	}
	if err := db.releaseLog(timeID); err != nil && err != errEntryDoesNotExist {
		return err
	}
	return nil
}

// Put puts data as the key's value, in place of the value it had.
func (db *DB) Put(key uint64, data []byte) (int64, error) {
	if err := db.ok(); err != nil {
		return 0, err
	}

	db.internal.logManager.rotateMu.RLock()
	defer db.internal.logManager.rotateMu.RUnlock()
	timeID := db.timeID()
	block, ok := db.timeBlock(timeID)
	if !ok {
		return 0, errForbidden
	}
	sh := db.index.shard(key)
	sh.Lock()
	// A value in another block is deleted, and the delete written to the WAL
	// before the put: one in this block is replaced.
	loc, had := sh.keys[key]
	var emptied bool
	if had && loc.block != block {
		var err error
		if emptied, err = db.replace(loc, key); err != nil {
			sh.Unlock()
			return int64(timeID), err
		}
		delete(sh.keys, key)
	}
	err := db.putEntry(block, key, data)
	if err == nil {
		sh.keys[key] = _Loc{timeID: timeID, block: block}
	}
	sh.Unlock()
	if err != nil {
		return int64(timeID), err
	}
	return int64(timeID), db.releaseEmptied(emptied, loc.timeID)
}

// Replace is Put, which replaces a key's value: it deleted the newest of a
// key's versions, and put data, in one log.
func (db *DB) Replace(key uint64, data []byte) (int64, error) {
	return db.Put(key, data)
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

// Flush writes the entries put so far to the WAL, and returns once they are
// written: until then, an entry put is lost if the process stops.
func (db *DB) Flush() error {
	if err := db.ok(); err != nil {
		return err
	}
	return db.internal.logManager.flush()
}

// Free frees time block from DB for a provided time ID and releases block from WAL.
func (db *DB) Free(timeID int64) error {
	return db.releaseLog(_TimeID(timeID))
}

// Size returns the number of keys the DB holds a value of.
func (db *DB) Size() int64 {
	var size int64
	for i := range db.index.shards {
		sh := &db.index.shards[i]
		sh.RLock()
		size += int64(len(sh.keys))
		sh.RUnlock()
	}
	return size
}
