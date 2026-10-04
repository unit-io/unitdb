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
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/wal"
)

const (
	dbVersion = 1.0

	logDir = "logs"

	nPoolSize = 27

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
	timeLock _TimeLock

	// tiny Log
	timeRef    _TimeID
	logManager *_TinyLogManager
	// lastLogID is the ID of the last log, read and set atomically: see
	// newLogID.
	lastLogID int64

	// buffer pool
	buffer *bpool.BufferPool

	// Write ahead log
	wal *wal.WAL
	// logMu guards the blocks' logs and what keeps them in the WAL. It is
	// taken after a block's lock and before db.mu.
	logMu mutex[logRank]

	// close
	closed uint32
	closer io.Closer
}

func (db *DB) close() error {
	if !db.setClosed() {
		return errClosed
	}

	db.internal.logManager.closeWait()

	var err error
	if db.internal.closer != nil {
		if err1 := db.internal.closer.Close(); err1 != nil {
			err = err1
		}
		db.internal.closer = nil
	}
	// Stop the pool's drain goroutine, or every DB opened leaks one.
	db.internal.buffer.Done()

	db.internal.meter.UnregisterAll()

	return err
}

// newTimeLock set timeRef and returns timeLock.
func (db *DB) newTimeLock(timeRef _TimeID) _Internal {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.internal.timeRef = timeRef
	return db.internal.timeLock.getTimeLock(timeRef)
}

// timeLock returns timeLock for the current timeRef.
func (db *DB) timeLock() _Internal {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.internal.timeLock.getTimeLock(db.internal.timeRef)
}

func (db *DB) timeID() _TimeID {
	return db.internal.logManager.timeID()
}

// newLogID returns the ID of a new log: its time, made later than the last
// log's if it isn't. A log is written to a file named by its ID, so two of
// the same ID, as a clock that ticks in microseconds gives, wrote one file:
// the second replaced the first.
func (db *DB) newLogID() _TimeID {
	for {
		last := atomic.LoadInt64(&db.internal.lastLogID)
		id := time.Now().UTC().UnixNano()
		if id <= last {
			id = last + 1
		}
		if atomic.CompareAndSwapInt64(&db.internal.lastLogID, last, id) {
			return _TimeID(id)
		}
	}
}

func (db *DB) cap() float64 {
	return db.internal.buffer.Capacity()
}

func (db *DB) newBlock() *_Block {
	return &_Block{data: db.internal.buffer.Get(), records: make(map[_Key]int64)}
}

func (db *DB) addTimeBlock(timeID _TimeID) (ok bool) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if _, ok := db.timeBlocks[timeID]; !ok {
		db.timeBlocks[timeID] = db.newBlock()
		return true
	}

	return false
}

func (db *DB) timeBlock(timeID _TimeID) (*_Block, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if b, ok := db.timeBlocks[timeID]; ok {
		return b, true
	}

	return nil, false
}

// blocks returns a snapshot of the time blocks so callers can lock each block
// without holding db.mu. Put holds a block lock while acquiring db.mu, so
// holding db.mu while acquiring a block lock would deadlock.
func (db *DB) blocks() []*_Block {
	db.mu.RLock()
	defer db.mu.RUnlock()
	blocks := make([]*_Block, 0, len(db.timeBlocks))
	for _, b := range db.timeBlocks {
		blocks = append(blocks, b)
	}

	return blocks
}

// deleteFrom records that b holds deletes of versions in from: b's logs
// stay in the WAL as long as from's. The caller holds logMu.
func (b *_Block) deleteFrom(from *_Block) {
	if from == b || from.state == blockGone || b.deletes[from] {
		return
	}
	if b.deletes == nil {
		b.deletes = make(map[*_Block]bool)
	}
	b.deletes[from] = true
	b.waitFor++
	from.waiters = append(from.waiters, b)
}

// applyLogs marks the logs of a released block applied once the logs of
// the blocks it deletes versions from are, and then those of the blocks
// that waited for it. It returns the logs, in the order they may go. The
// caller holds logMu.
func (b *_Block) applyLogs() []_TimeID {
	if b.state != blockReleased || b.waitFor > 0 {
		return nil
	}
	b.setState(blockGone)
	logs := b.timeRefs
	for _, w := range b.waiters {
		w.waitFor--
		logs = append(logs, w.applyLogs()...)
	}
	b.waiters = nil
	b.deletes = nil
	return logs
}

// tinyWrite writes tiny log to the WAL.
func (db *DB) tinyWrite(tinyLog *_TinyLog) error {
	timeLock := db.newTimeLock(tinyLog.ID())
	timeLock.Lock()
	defer timeLock.Unlock()
	block, ok := db.timeBlock(tinyLog.timeID())
	if !ok {
		// all records has already deleted and nothing to write.
		return nil
	}
	block.RLock()
	blockSize := block.size()
	if blockSize == 0 {
		// freed; nothing to write.
		block.RUnlock()
		return nil
	}
	log, err := block.data.Slice(block.lastOffset, blockSize)
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
	logWriter.SetBlockID(int64(tinyLog.timeID()))
	if err := <-logWriter.SignalInitWrite(int64(tinyLog.ID())); err != nil {
		return err
	}

	block.Lock()
	block.lastOffset = blockSize
	block.Unlock()

	// A block released while its log was written: the log goes with the
	// block's others, or at once if they have gone.
	db.internal.logMu.Lock()
	defer db.internal.logMu.Unlock()
	if block.state == blockGone {
		return db.internal.wal.SignalLogApplied(int64(tinyLog.ID()))
	}
	block.timeRefs = append(block.timeRefs, tinyLog.ID())

	return nil
}

// tinyCommit commits tiny log to DB.
func (db *DB) tinyCommit(tinyLog *_TinyLog) error {
	defer tinyLog.abort()

	err := db.tinyWrite(tinyLog)
	if tinyLog.managed {
		tinyLog.err = err
		return err
	}
	// The log is done with, written or not: a block whose log is never
	// counted done is never synced. Its entries are in memory either way.
	db.internal.timeMark.release(tinyLog.timeID())
	if err != nil {
		tinyLog.err = err
		return err
	}
	return db.releaseEmpty(tinyLog.timeID())
}

// releaseEmpty releases a block writes no longer go to once it holds no
// entries and its data is all in the WAL: one whose entries were deleted
// before its last log was written, or that holds deletes only, which
// nothing else releases.
func (db *DB) releaseEmpty(timeID _TimeID) error {
	if timeID >= db.timeID() {
		return nil
	}
	block, ok := db.timeBlock(timeID)
	if !ok {
		return nil
	}
	block.RLock()
	empty := block.data != nil && block.count == 0 && block.lastOffset == block.size()
	block.RUnlock()
	if !empty {
		return nil
	}
	if err := db.releaseLog(timeID); err != nil && err != errEntryDoesNotExist {
		return err
	}
	return nil
}

// releaseLog releases a time block whose entries were written out or
// deleted: its logs are marked applied, and its buffer goes back to the pool.
// A delete that empties a block and a sync that wrote it can release it at
// once: the one that takes it out of timeBlocks releases it, and the other
// finds it gone.
func (db *DB) releaseLog(timeID _TimeID) error {
	// The live tiny log can share the time ID: after a reopen in the second
	// the last writes were made, it writes to the block recovered for them.
	// It then gets an empty block, which writes need until the next rotation.
	current := db.timeID()
	db.internal.logMu.Lock()
	db.mu.Lock()
	block, ok := db.timeBlocks[timeID]
	if !ok {
		db.mu.Unlock()
		db.internal.logMu.Unlock()
		return errEntryDoesNotExist
	}
	if timeID == current {
		db.timeBlocks[timeID] = db.newBlock()
	} else {
		delete(db.timeBlocks, _TimeID(timeID))
	}
	db.internal.timeMark.timeUnref(timeID)
	db.mu.Unlock()

	// The logs go, in order, under logMu: a block deleting from one whose
	// logs are going must find them gone only once they are.
	block.setState(blockReleased)
	err := db.signalApplied(block.applyLogs())
	db.internal.logMu.Unlock()
	if err != nil {
		return err
	}

	// Free under the block's write lock so it waits for readers of the buffer.
	block.Lock()
	keys := block.liveKeys()
	block.free(db.internal.buffer)
	block.Unlock()

	// Its keys have no value here any more: synced by the engine, or a
	// batch aborted. Without the block's lock: shards come before it.
	db.index.forget(block, keys)

	return nil
}

// signalApplied marks logs applied, in order.
func (db *DB) signalApplied(logs []_TimeID) error {
	for _, timeRef := range logs {
		if err := db.internal.wal.SignalLogApplied(int64(timeRef)); err != nil {
			return err
		}
	}
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
