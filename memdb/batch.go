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
	"fmt"
)

// Batch is a write batch.
type Batch struct {
	db      *DB
	opts    *_Options
	managed bool

	//tiny Log
	writeLockC chan struct{}
	tinyLog    *_TinyLog

	tinyLogGroup map[_TimeID]*_TinyLog

	// commitComplete is used to signal if batch commit is complete and batch is fully written to DB.
	commitComplete chan struct{}
}

// TimeID returns time ID for the batch.
func (b *Batch) TimeID() int64 {
	return int64(b.db.internal.timeMark.timeID(b.tinyLog.ID()))
}

// Put adds a new key-value pair to the batch.
func (b *Batch) Put(key uint64, data []byte) error {
	if err := b.db.ok(); err != nil {
		return err
	}

	b.writeLockC <- struct{}{}
	defer func() {
		<-b.writeLockC
	}()

	timeID := b.db.internal.timeMark.timeID(b.tinyLog.ID())
	b.db.mu.RLock()
	block, ok := b.db.timeBlocks[timeID]
	b.db.mu.RUnlock()
	if !ok {
		return errForbidden
	}
	block.Lock()
	defer block.Unlock()
	ikey := iKey(false, key)
	if err := block.put(ikey, data); err != nil {
		return err
	}
	b.db.addTimeFilter(timeID, key)

	b.tinyLog.incount()
	b.db.internal.meter.Puts.Inc(1)

	return nil
}

// Write starts writing entries into DB.
func (b *Batch) Write() error {
	b.writeLockC <- struct{}{}
	b.tinyLogGroup[b.tinyLog.ID()] = b.tinyLog
	b.db.internal.workerPool.write(b.tinyLog)
	b.tinyLog = b.db.newTinyLog()
	<-b.writeLockC

	return nil
}

// Commit commits changes to the DB. In batch operation commit is managed and client is not allowed to call Commit.
// On Commit complete batch operation signal to the caller if the batch is fully committed to DB.
func (b *Batch) Commit() error {
	_assert(!b.managed, "managed batch commit not allowed")
	b.db.internal.closeW.Add(1)
	defer func() {
		close(b.commitComplete)
		b.db.internal.closeW.Done()
		b.Abort()
	}()

	// Write batch entries.
	if err := b.Write(); err != nil {
		return err
	}

	for ID, tinyLog := range b.tinyLogGroup {
		<-tinyLog.doneChan
		b.db.internal.timeMark.release(ID)
	}

	b.tinyLogGroup = make(map[_TimeID]*_TinyLog)

	return nil
}

//Abort aborts batch or perform cleanup operation on batch complete.
func (b *Batch) Abort() error {
	_assert(!b.managed, "managed batch abort not allowed")
	for ID := range b.tinyLogGroup {
		if err := b.db.releaseLog(ID); err != nil {
			return err
		}
	}
	b.db = nil

	return nil
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

// setManaged sets batch managed.
func (b *Batch) setManaged() {
	b.managed = true
}

// unsetManaged sets batch unmanaged.
func (b *Batch) unsetManaged() {
	b.managed = false
}
