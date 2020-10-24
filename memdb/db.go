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

	"github.com/unit-io/unitdb/hash"
)

// To avoid lock bottlenecks block cache is divided into several (nShards) shards.
type (
	timeID     int64
	blockCache map[timeID]*block
)

// DB represents an SSD-optimized store.
type DB struct {
	mu sync.RWMutex

	version int
	opts    *options

	// blockcache
	internal   *db
	consistent *hash.Consistent
	blockCache
	timeBlocks map[blockKey]timeID
}

// Open initializes database connection.
func Open(opts ...Options) (*DB, error) {
	options := &options{}
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

	db := &DB{
		opts:       options,
		internal:   &db{},
		consistent: hash.InitConsistent(options.maxBlocks, options.maxBlocks),
		blockCache: make(map[timeID]*block),
		timeBlocks: make(map[blockKey]timeID),
	}

	db.initDb()

	if err := db.recovery(false); err != nil {
		return nil, err
	}

	return db, nil
}

// Close closes the underlying database connection.
func (db *DB) Close() error {
	if db.blockCache != nil {
		db.blockCache = nil
		db.version = -1

	}
	return db.close()
}

// IsOpen returns true if connection to database has been established. It does not check if
// connection is actually live.
func (db *DB) IsOpen() bool {
	return db.blockCache != nil
}

// Get gets data for the provided key under a blockID.
func (db *DB) Get(key uint64) ([]byte, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}

	// Get timeBlock
	blockID := db.consistent.FindBlock(key)

	db.mu.RLock()
	timeID, ok := db.timeBlocks[blockKey{blockID: blockID, key: key}]
	if !ok {
		db.mu.RUnlock()
		return nil, errEntryDoesNotExist
	}

	block, ok := db.blockCache[timeID]
	db.mu.RUnlock()
	if !ok {
		return nil, errEntryDeleted
	}

	block.RLock()
	defer block.RUnlock()
	// Get item from block.
	off, ok := block.m[key]
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
	return data[+8+1+4:], nil
}

// Delete deletes entry from mem store.
func (db *DB) Delete(key uint64) error {
	if err := db.ok(); err != nil {
		return err
	}
	// Get timeBlock
	blockID := db.consistent.FindBlock(key)

	db.mu.RLock()
	blockKey := blockKey{blockID: blockID, key: key}
	timeID, ok := db.timeBlocks[blockKey]
	if !ok {
		db.mu.RUnlock()
		return errEntryDoesNotExist
	}

	block := db.blockCache[timeID]
	db.mu.RUnlock()

	block.Lock()
	defer block.Unlock()
	delete(block.m, key)

	if len(block.m) == 0 {
		db.mu.Lock()
		delete(db.timeBlocks, blockKey)
		block.data.reset()
		delete(db.blockCache, timeID)
		db.mu.Unlock()

		db.signalLogApplied(int64(timeID))
	}

	return nil
}

// Set sets the value for the given entry for a blockID.
func (db *DB) Set(key uint64, data []byte) error {
	if err := db.ok(); err != nil {
		return err
	}

	db.internal.tinyBatchLockC <- struct{}{}
	defer func() {
		<-db.internal.tinyBatchLockC
	}()

	db.mu.Lock()
	timeID := timeID(db.internal.timeID())
	b, ok := db.blockCache[timeID]
	if !ok {
		b = &block{data: dataTable{}, m: make(map[uint64]int64)}
		db.blockCache[timeID] = b
	}
	db.mu.Unlock()
	b.Lock()
	defer b.Unlock()
	dataLen := uint32(len(data) + 8 + 1 + 4) // data len+key len+flag bit+scratch len
	off, err := b.data.allocate(dataLen)
	if err != nil {
		return err
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], dataLen)
	if _, err := b.data.writeAt(scratch[:], off); err != nil {
		return err
	}
	// k with flag bit
	var k [9]byte
	k[0] = 0
	binary.LittleEndian.PutUint64(k[1:], key)
	if _, err := b.data.writeAt(k[:], off+4); err != nil {
		return err
	}
	if _, err := b.data.writeAt(data, off+8+1+4); err != nil {
		return err
	}
	b.m[key] = off

	// Get timeBlock
	blockID := db.consistent.FindBlock(key)
	blockKey := blockKey{
		blockID: blockID,
		key:     key,
	}
	db.mu.Lock()
	db.timeBlocks[blockKey] = timeID
	db.mu.Unlock()

	db.internal.tinyBatch.incount()

	return nil
}
