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

// DB represents an SSD-optimized store.
type DB struct {
	mu sync.RWMutex

	version int
	opts    *options

	// blockcache
	internal   *db
	consistent *hash.Consistent
	blockCache
	timeBlocks map[uint16]timeID // map[blockID]timeID
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
		timeBlocks: make(map[uint16]timeID),
	}

	db.initDb()

	if err := db.startRecover(false); err != nil {
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

	db.mu.RLock()
	// Get timeBlock
	blockID := db.blockID(key)
	timeID, ok := db.timeBlocks[blockID]
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
	return data[+8+1+4:], nil
}

// Delete deletes entry from mem store.
func (db *DB) Delete(key uint64) error {
	if err := db.ok(); err != nil {
		return err
	}

	db.mu.RLock()
	// Get timeBlock
	blockID := db.blockID(key)
	timeID, ok := db.timeBlocks[blockID]
	if !ok {
		db.mu.RUnlock()
		return errEntryDoesNotExist
	}

	block := db.blockCache[timeID]
	db.mu.RUnlock()

	block.Lock()
	defer block.Unlock()
	iK := iKey(false, key)
	delete(block.records, iK)
	block.count--
	// fmt.Println("db.Delete: timeID, count, records ", timeID, block.count, block.records)
	if block.count == 0 {
		db.mu.Lock()
		delete(db.timeBlocks, blockID)
		block.data.reset()
		delete(db.blockCache, timeID)
		db.mu.Unlock()

		db.signalLogApplied(int64(timeID))
	}

	// set deleted key
	db.set(true, key, nil)

	return nil
}

// Set sets the value for the given entry for a blockID.
func (db *DB) Set(key uint64, data []byte) error {
	if err := db.ok(); err != nil {
		return err
	}

	timeID, err := db.set(false, key, data)
	if err != nil {
		return err
	}
	// Get timeBlock
	blockID := db.blockID(key)
	db.mu.Lock()
	db.timeBlocks[blockID] = timeID
	db.mu.Unlock()

	return nil
}
