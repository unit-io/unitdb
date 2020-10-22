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
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/hash"
	"github.com/unit-io/unitdb/wal"
)

const (
	dbVersion = 1.0

	logPostfix = ".log"

	nBlocks = 27

	// maxMemSize value to limit maximum memory for the mem store.
	defaultMemSize = (int64(1) << 34) - 1

	// maxBufferSize sets Size of buffer to use for pooling.
	defaultBufferSize = 1 << 30 // maximum size of a buffer to use in bufferpool (1GB).

	// logSize sets Size of write ahead log.
	defaultLogSize = 1 << 32 // maximum size of log to grow before allocating free segments (4GB).
)

// To avoid lock bottlenecks block cache is divided into several (nShards) shards.
type (
	timeID     int64
	blockCache map[timeID]*block
)

type (
	blockKey struct {
		blockID uint16
		key     uint64
	}
	block struct {
		data         dataTable
		freeOffset   int64            // mem cache keep lowest offset that can be free.
		m            map[uint64]int64 // map[key]offset
		sync.RWMutex                  // Read Write mutex, guards access to internal map.
	}
)

// DB represents an SSD-optimized store.
type DB struct {
	mu         sync.RWMutex
	writeLockC chan struct{}

	version int
	opts    *options
	bufPool *bpool.BufferPool

	// tiny Batch
	*tinyBatch

	// blockcache
	consistent *hash.Consistent
	blockCache
	timeBlocks map[blockKey]timeID

	// Write ahead log
	wal *wal.WAL

	// close
	closeW sync.WaitGroup
	closeC chan struct{}
	closer io.Closer
}

// Open initializes database connection
func Open(opts ...Options) (*DB, error) {
	options := &options{}
	WithDefaultOptions().set(options)
	for _, opt := range opts {
		if opt != nil {
			opt.set(options)
		}
	}

	// Make sure we have a directory
	if err := os.MkdirAll(options.logFilePath, 0777); err != nil {
		return nil, errors.New("DB.Open, Unable to create db dir")
	}

	db := &DB{
		bufPool:    bpool.NewBufferPool(options.memdbSize, nil),
		consistent: hash.InitConsistent(options.MaxBlocks, options.MaxBlocks),
		blockCache: make(map[timeID]*block),
		timeBlocks: make(map[blockKey]timeID),

		// Close
		closeC: make(chan struct{}),
	}

	db.tinyBatch = db.newTinyBatch()

	if err := db.Recovery(false); err != nil {
		return nil, err
	}

	TinyBatchWriteInterval(options.tinyBatchWriteInterval)

	return db, nil
}

// Close closes the underlying database connection
func (db *DB) Close() error {
	var err error
	if db.blockCache != nil {
		db.blockCache = nil
		db.version = -1

		var err error
		if db.closer != nil {
			if err1 := db.closer.Close(); err == nil {
				err = err1
			}
			db.closer = nil
		}
	}
	return err
}

// IsOpen returns true if connection to database has been established. It does not check if
// connection is actually live.
func (db *DB) IsOpen() bool {
	return db.blockCache != nil
}

func (db *DB) Recovery(reset bool) error {
	m, err := db.recovery(reset)
	if err != nil {
		return err
	}
	for k, msg := range m {
		if err := db.Set(k, msg); err != nil {
			return err
		}
	}
	return nil
}

// Get gets data for the provided key under a blockID.
func (db *DB) Get(key uint64) ([]byte, error) {
	// Get timeBlock
	blockID := db.consistent.FindBlock(key)

	db.mu.RLock()
	timeID, ok := db.timeBlocks[blockKey{blockID: blockID, key: key}]
	if !ok {
		db.mu.RUnlock()
		return nil, errors.New("entry not found")
	}
	db.mu.RUnlock()

	block := db.blockCache[timeID]
	block.RLock()
	defer block.RUnlock()
	// Get item from block.
	off, ok := block.m[key]
	if off == -1 {
		return nil, errors.New("entry deleted")
	}
	if !ok {
		return nil, nil
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
	return data[4:], nil
}

// Remove sets data offset to -1 for the key under a blockID.
func (db *DB) Delete(key uint64) error {
	// Get timeBlock
	blockID := db.consistent.FindBlock(key)

	db.mu.RLock()
	blockKey := blockKey{blockID: blockID, key: key}
	timeID, ok := db.timeBlocks[blockKey]
	if !ok {
		db.mu.RUnlock()
		return errors.New("entry not found")
	}

	block := db.blockCache[timeID]
	db.mu.RUnlock()

	block.Lock()
	defer block.Unlock()
	delete(block.m, key)

	// append deleted entry to WAL.
	db.append(true, key, nil)

	if len(block.m) == 0 {
		db.mu.Lock()
		defer db.mu.Unlock()
		delete(db.timeBlocks, blockKey)
		block.data.reset()
		delete(db.blockCache, timeID)

		db.signalLogApplied(int64(timeID))
	}

	return nil
}

// Set sets the value for the given entry for a blockID.
func (db *DB) Set(key uint64, data []byte) error {
	// Get timeBlock
	blockID := db.consistent.FindBlock(key)

	db.mu.RLock()
	timeID, ok := db.timeBlocks[blockKey{blockID: blockID, key: key}]
	if !ok {
		blockKey := blockKey{
			blockID: blockID,
			key:     key,
		}
		timeID = newTimeID(db.opts.tinyBatchWriteInterval)
		db.timeBlocks[blockKey] = timeID

		b := &block{data: dataTable{}, m: make(map[uint64]int64)}
		db.blockCache[timeID] = b
	}
	db.mu.RUnlock()

	block := db.blockCache[timeID]
	block.Lock()
	defer block.Unlock()
	dataLen := uint32(len(data) + 4)
	off, err := block.data.allocate(dataLen)
	if err != nil {
		return err
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], dataLen)

	if _, err := block.data.writeAt(scratch[:], off); err != nil {
		return err
	}
	if _, err := block.data.writeAt(data, off+4); err != nil {
		return err
	}
	block.m[key] = off

	// append entry to WAL for persistence.
	return db.append(false, key, data)
}

// tinyBatchLoop handles tiny batches.
func (db *DB) tinyBatchLoop(interval time.Duration) {
	db.closeW.Add(1)
	defer db.closeW.Done()
	tinyBatchTicker := time.NewTicker(interval)
	for {
		select {
		case <-db.closeC:
			tinyBatchTicker.Stop()
			return
		case <-tinyBatchTicker.C:
			if db.tinyBatch.len() != 0 {
				if err := db.writeLog(); err != nil {
					fmt.Println("Error committing tinyBatch")
				}
			}
		}
	}
}

// // Keys gets all keys from block cache for the provided blockID.
// func (db *DB) Keys(blockID uint64) []uint64 {
// 	// Get block
// 	block := db.getBlock(blockID)
// 	block.RLock()
// 	defer block.RUnlock()
// 	// Get keys from  block.
// 	keys := make([]uint64, 0, len(block.m))
// 	for k := range block.m {
// 		keys = append(keys, k)
// 	}
// 	return keys
// }
