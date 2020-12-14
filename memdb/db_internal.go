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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/filter"
	"github.com/unit-io/unitdb/wal"
)

const (
	dbVersion = 1.0

	logDir = "logs"

	nBlocks = 27

	nPoolSize = 10

	// nLocks sets maximum concurent timeLocks.
	nLocks = 100000

	// defaultMemdbSize sets maximum memory usage limit for the DB.
	defaultMemdbSize = (int64(1) << 34) - 1

	// defaultBufferSize sets Size of buffer to use for pooling.
	defaultBufferSize = 1 << 30 // maximum size of a buffer to use in bufferpool (1GB).

	// defaultLogSize sets Size of write ahead log.
	defaultLogSize = 1 << 32 // maximum size of log to grow before allocating from free segments (4GB).
)

// To avoid lock bottlenecks block cache is divided into several (nShards) shards.
type (
	_TimeID     int64
	_TimeFilter struct {
		timeRecords map[_TimeID]*filter.Block
		// bloom filter adds keys to the filter for all entries in a time block.
		// filter is checked during get or delete operation
		// to indicate key definitely not exist in the time block.
		filter       *filter.Generator
		sync.RWMutex // Read Write mutex, guards access to internal map.
	}
	_TimeBlocks map[_TimeID]*_Block
)

type (
	// _Key is an internal key that includes deleted flag for the key.
	_Key struct {
		delFlag uint8 // deleted flag
		key     uint64
	}
	_BlockKey uint16
	_Block    struct {
		count        int64
		data         *bpool.Buffer
		timeRefs     []_TimeID
		records      map[_Key]int64 // map[key]offset
		sync.RWMutex                // Read Write mutex, guards access to internal map.
	}
)

// _DB represents a mem store.
type _DB struct {
	// The db start time.
	start time.Time

	// The metrics to measure timeseries on DB events.
	meter *Meter

	// time mark to manage time records
	timeMark *_TimeMark
	timeLock _TimeLock

	// tiny Log
	writeLockC chan struct{}
	tinyLog    *_TinyLog
	workerPool *_WorkerPool

	// buffer pool
	bufPool *bpool.BufferPool

	// Write ahead log
	wal *wal.WAL

	// query
	queryPlan *_LogicalPlan

	// close
	closeW sync.WaitGroup
	closeC chan struct{}
	closed uint32
	closer io.Closer
}

// blockKey gets blockKey for the Key using consistent hashing.
func (db *DB) blockKey(key uint64) _BlockKey {
	return _BlockKey(db.consistent.FindBlock(key))
}

// iKey an internal key includes deleted flag.
func iKey(delFlag bool, k uint64) _Key {
	dFlag := uint8(0)
	if delFlag {
		dFlag = 1
	}
	return _Key{delFlag: dFlag, key: k}
}

// addTimeFilter adds unique time block to the set.
func (db *DB) addTimeFilter(timeID _TimeID, key uint64) error {
	blockKey := db.blockKey(key)
	r, ok := db.timeFilters[blockKey]
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

func (db *DB) newTinyLog() *_TinyLog {
	tinyLog := &_TinyLog{doneChan: make(chan struct{})}
	tinyLog.setID(db.internal.timeMark.timeNow())
	db.internal.tinyLog = tinyLog

	return tinyLog
}

func (db *DB) newWorkerPool(size int) *_WorkerPool {
	// There must be at least one job.
	if size < 1 {
		size = 1
	}

	pool := &_WorkerPool{
		db:          db,
		maxSize:     size,
		writeQueue:  make(chan *_TinyLog, 1),
		logQueue:    make(chan *_TinyLog),
		stoppedChan: make(chan struct{}),
	}

	// start the log dispatcher
	go pool.dispatch()

	return pool
}

func (db *DB) close() error {
	if !db.setClosed() {
		return errClosed
	}

	// Signal all goroutines.
	time.Sleep(db.opts.logInterval)
	close(db.internal.closeC)

	db.internal.workerPool.stopWait()

	// Wait for all goroutines to exit.
	db.internal.closeW.Wait()

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

// batch starts a new batch.
func (db *DB) batch() *Batch {
	b := &Batch{db: db, writeLockC: make(chan struct{}, 1), tinyLogGroup: make(map[_TimeID]*_TinyLog)}
	b.newTinyLog()

	return b
}

func (db *DB) newQueryPlan() *_LogicalPlan {
	queryPlan := &_LogicalPlan{timeBlocks: make(map[_TimeID]*_Block), timeFilters: make(map[_BlockKey]*_TimeFilter)}
	for i := 0; i < nBlocks; i++ {
		queryPlan.timeFilters[_BlockKey(i)] = &_TimeFilter{timeRecords: make(map[_TimeID]*filter.Block), filter: filter.NewFilterGenerator()}
	}

	return queryPlan
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

func (b *_Block) get(off int64) ([]byte, error) {
	scratch, err := b.data.Slice(off, off+4) // read data length.
	if err != nil {
		return nil, err
	}
	dataLen := int64(binary.LittleEndian.Uint32(scratch[:4]))
	data, err := b.data.Slice(off, off+dataLen)
	if err != nil {
		return nil, err
	}

	return data[8+1+4:], nil
}

func (b *_Block) put(ikey _Key, data []byte) error {
	dataLen := int64(len(data) + 8 + 1 + 4) // data len + key len + flag bit + scratch len
	off, err := b.data.Extend(dataLen)
	if err != nil {
		return err
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(dataLen))
	if _, err := b.data.WriteAt(scratch[:], off); err != nil {
		return err
	}

	// k with flag bit
	var k [9]byte
	k[0] = ikey.delFlag
	binary.LittleEndian.PutUint64(k[1:], ikey.key)
	if _, err := b.data.WriteAt(k[:], off+4); err != nil {
		return err
	}
	if ikey.delFlag == 0 {
		if _, err := b.data.WriteAt(data, off+8+1+4); err != nil {
			return err
		}
		b.count++
	}
	b.records[ikey] = off

	return nil
}

// tinyWrite writes tiny log to the WAL.
func (db *DB) tinyWrite(tinyLog *_TinyLog) error {
	timeID := db.internal.timeMark.timeID(tinyLog.ID())
	logWriter, err := db.internal.wal.NewWriter()
	if err != nil {
		return err
	}
	db.mu.RLock()
	block, ok := db.timeBlocks[timeID]
	db.mu.RUnlock()
	if !ok {
		return nil
	}
	block.RLock()
	defer block.RUnlock()
	for _, off := range block.records {
		scratch, err := block.data.Slice(off, off+4) // read data length.
		if err != nil {
			return err
		}
		dataLen := int64(binary.LittleEndian.Uint32(scratch[:4]))
		if data, err := block.data.Slice(off, off+dataLen); err == nil {
			if err := <-logWriter.Append(data[4:]); err != nil {
				return err
			}
			data = nil
		}
	}

	if err := <-logWriter.SignalInitWrite(int64(tinyLog.ID())); err != nil {
		return err
	}
	// Add timeID to the block later it is used for releasing logs.
	block.timeRefs = append(block.timeRefs, tinyLog.ID())
	db.internal.meter.Syncs.Inc(int64(len(block.records)))

	return nil
}

// tinyCommit commits tiny log to DB.
func (db *DB) tinyCommit(tinyLog *_TinyLog) error {
	db.internal.closeW.Add(1)
	defer func() {
		tinyLog.abort()
		db.internal.closeW.Done()
	}()

	if err := db.tinyWrite(tinyLog); err != nil {
		return err
	}

	if !tinyLog.managed {
		db.internal.timeMark.release(tinyLog.ID())
	}

	return nil
}

// startRecovery recovers pending entries from the WAL.
func (db *DB) startRecovery() error {
	// start log recovery
	r, err := db.internal.wal.NewReader()
	if err != nil {
		return err
	}

	log := make(map[uint64][]byte)
	err = r.Read(func(ID int64) (ok bool, err error) {
		l := r.Count()
		for i := uint32(0); i < l; i++ {
			logData, ok, err := r.Next()
			if err != nil {
				return false, err
			}
			if !ok {
				break
			}
			dBit := logData[0]
			key := binary.LittleEndian.Uint64(logData[1:9])
			val := logData[9:]
			if dBit == 1 {
				if _, exists := log[key]; exists {
					delete(log, key)
				}
				continue
			}
			log[key] = val
		}
		return false, nil
	})

	db.internal.wal.Reset()

	for k, val := range log {
		if _, err := db.Put(k, val); err != nil {
			return err
		}
	}

	db.internal.meter.Recovers.Inc(int64(len(log)))

	return nil
}

func (db *DB) releaseLog(timeID _TimeID) error {
	db.mu.RLock()
	block, ok := db.timeBlocks[timeID]
	db.mu.RUnlock()
	if !ok {
		return errEntryDoesNotExist
	}

	for _, ID := range block.timeRefs {
		if err := db.internal.wal.SignalLogApplied(int64(ID)); err != nil {
			return err
		}
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.releaseCount += block.count
	delete(db.timeBlocks, _TimeID(timeID))

	db.internal.bufPool.Put(block.data)

	return nil
}

// tinyLogLoop handles writing tiny logs to the WAL.
func (db *DB) tinyLogLoop(interval time.Duration) {
	logTicker := time.NewTimer(interval)
	for {
		select {
		case <-db.internal.closeC:
			logTicker.Stop()
			return
		case <-logTicker.C:
			db.internal.writeLockC <- struct{}{}
			if db.internal.tinyLog.len() != 0 {
				db.internal.workerPool.write(db.internal.tinyLog)
				db.newTinyLog()
			}
			<-db.internal.writeLockC
			logTicker.Reset(interval)
		}
	}
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
