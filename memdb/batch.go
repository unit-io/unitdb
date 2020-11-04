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

	//tiny Batch
	tinyBatchLockC chan struct{}
	tinyBatch      *_TinyBatch

	tinyBatchGroup map[_TimeID]*_TinyBatch // map[timeID]*tinyBatch

	// commitComplete is used to signal if batch commit is complete and batch is fully written to DB.
	commitComplete chan struct{}
}

// TimeID returns timeID for the batch.
func (b *Batch) TimeID() int64 {
	return int64(b.tinyBatch.timeID())
}

// Put added key-value to a batch.
func (b *Batch) Put(key uint64, data []byte) error {
	if err := b.db.ok(); err != nil {
		return err
	}

	b.tinyBatchLockC <- struct{}{}
	defer func() {
		<-b.tinyBatchLockC
	}()

	timeID := b.tinyBatch.timeID()
	ikey := iKey(false, key)
	b.db.mu.Lock()
	block, ok := b.db.blockCache[timeID]
	if !ok {
		block = newBlock()
		b.db.blockCache[timeID] = block
	}
	b.db.mu.Unlock()
	block.Lock()
	defer block.Unlock()
	if err := block.put(ikey, data); err != nil {
		return err
	}

	b.db.addTimeBlock(timeID, key)

	b.tinyBatch.incount()
	b.db.internal.meter.Puts.Inc(1)

	return nil
}

func (b *Batch) Write() error {
	b.tinyBatchLockC <- struct{}{}
	b.tinyBatchGroup[b.tinyBatch.timeID()] = b.tinyBatch
	b.db.internal.batchPool.write(b.tinyBatch)
	b.tinyBatch = b.db.newTinyBatch()
	<-b.tinyBatchLockC

	return nil
}

// Commit commits changes to the DB. In batch operation commit is managed and client is not allowed to call Commit.
// On Commit complete batch operation signal to the caller if the batch is fully commited to DB.
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

	for timeID, tinyBatch := range b.tinyBatchGroup {
		<-tinyBatch.doneChan
		b.db.internal.timeMark.release(timeID)
	}

	b.tinyBatchGroup = make(map[_TimeID]*_TinyBatch)

	return nil
}

//Abort abort is a batch cleanup operation on batch complete.
func (b *Batch) Abort() error {
	_assert(!b.managed, "managed batch abort not allowed")
	for timeID := range b.tinyBatchGroup {
		b.db.internal.timeMark.abort(timeID)
		if err := b.db.releaseLog(timeID); err != nil {
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
