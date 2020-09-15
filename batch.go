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

package unitdb

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/unit-io/unitdb/message"
)

// SetOptions sets batch options.
func (b *Batch) SetOptions(opts ...Options) {
	for _, opt := range opts {
		if opt != nil {
			opt.set(b.opts)
		}
	}
}

// Batch is a write batch.
type Batch struct {
	db      *DB
	opts    *options
	managed bool

	//tiny Batch
	tinyBatchLockC chan struct{}
	tinyBatch      *tinyBatch

	tinyBatchGroup map[int64]*tinyBatch // map[timeID]*tinyBatch
	commitW        sync.WaitGroup
	// commitComplete is used to signal if batch commit is complete and batch is fully written to DB.
	commitComplete chan struct{}
}

// Put adds entry to batch for given topic->key/value.
// Client must provide Topic to the BatchOptions.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) Put(topic, payload []byte) error {
	return b.PutEntry(NewEntry(topic, payload).WithContract(b.opts.batchOptions.contract))
}

// PutEntry appends entries to a bacth for given topic->key/value pair.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) PutEntry(e *Entry) error {
	switch {
	case len(e.Topic) == 0:
		return errTopicEmpty
	case len(e.Topic) > maxTopicLength:
		return errTopicTooLarge
	case len(e.Payload) == 0:
		return errValueEmpty
	case len(e.Payload) > maxValueLength:
		return errValueTooLarge
	}
	e.Encryption = e.Encryption || b.opts.batchOptions.encryption
	if err := b.db.setEntry(b.tinyBatch.timeID(), e); err != nil {
		return err
	}

	b.tinyBatchLockC <- struct{}{}
	defer func() {
		<-b.tinyBatchLockC
	}()

	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(e.cache)+4))
	if _, err := b.tinyBatch.buffer.Write(scratch[:]); err != nil {
		return err
	}
	if _, err := b.tinyBatch.buffer.Write(e.cache); err != nil {
		return err
	}

	b.tinyBatch.index = append(b.tinyBatch.index, batchIndex{delFlag: false, offset: b.tinyBatch.size})
	b.tinyBatch.size += int64(len(e.cache) + 4)

	b.tinyBatch.incount()

	// reset message entry
	e.reset()

	return nil
}

// Delete appends delete entry to batch for given key.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) Delete(id, topic []byte) error {
	return b.DeleteEntry(NewEntry(topic, nil).WithID(id))
}

// DeleteEntry appends entry for deletion to a batch for given key.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) DeleteEntry(e *Entry) error {
	switch {
	case b.db.opts.immutable:
		return errImmutable
	case len(e.ID) == 0:
		return errMsgIDEmpty
	case len(e.Topic) == 0:
		return errTopicEmpty
	case len(e.Topic) > maxTopicLength:
		return errTopicTooLarge
	}

	if err := b.db.setEntry(b.tinyBatch.timeID(), e); err != nil {
		return err
	}

	b.tinyBatchLockC <- struct{}{}
	defer func() {
		<-b.tinyBatchLockC
	}()

	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(e.cache)+4))
	if _, err := b.tinyBatch.buffer.Write(scratch[:]); err != nil {
		return err
	}
	if _, err := b.tinyBatch.buffer.Write(e.cache); err != nil {
		return err
	}

	b.tinyBatch.index = append(b.tinyBatch.index, batchIndex{delFlag: true, offset: b.tinyBatch.size})
	b.tinyBatch.size += int64(len(e.cache) + 4)

	b.tinyBatch.incount()

	// reset message entry
	e.reset()

	return nil
}

func (b *Batch) writeInternal(fn func(i int, e entry, data []byte) error) error {
	if err := b.db.ok(); err != nil {
		return err
	}

	// data := b.tinyBatch.buffer.Bytes()
	var e entry

	for i, index := range b.tinyBatch.index {
		off := index.offset
		data, err := b.tinyBatch.buffer.Slice(off, off+4)
		if err != nil {
			return err
		}
		dataLen := int64(binary.LittleEndian.Uint32(data))
		entryData, err := b.tinyBatch.buffer.Slice(off+4, off+entrySize+4)
		if err != nil {
			return err
		}
		if err := e.UnmarshalBinary(entryData); err != nil {
			return err
		}
		if index.delFlag && e.seq != 0 {
			/// Test filter block for presence.
			if !b.db.filter.Test(e.seq) {
				return nil
			}
			b.db.delete(e.topicHash, e.seq)
			continue
		}

		// put packed entry into memdb.
		data = data[:0]
		data, err = b.tinyBatch.buffer.Slice(off+4, off+dataLen)
		if err != nil {
			return err
		}
		if err := fn(i, e, data); err != nil {
			return err
		}
		entryData = nil
		data = nil
	}
	return nil
}

// Write starts writing entries into DB. It returns an error if batch write fails.
func (b *Batch) Write() error {
	if b.tinyBatch.len() == 0 {
		return nil
	}

	defer b.db.bufPool.Put(b.tinyBatch.buffer)

	topics := make(map[uint64]*message.Topic)
	b.tinyBatchGroup[b.tinyBatch.timeID()] = b.tinyBatch

	b.writeInternal(func(i int, e entry, data []byte) error {
		if e.topicSize != 0 {
			t, ok := topics[e.topicHash]
			if !ok {
				t = new(message.Topic)
				rawTopic := data[entrySize+idSize : entrySize+idSize+e.topicSize]
				t.Unmarshal(rawTopic)
				topics[e.topicHash] = t
			}
			b.db.trie.add(newTopic(e.topicHash, 0), t.Parts, t.Depth)
		}
		blockID := startBlockIndex(e.seq)
		memseq := b.db.cacheID ^ e.seq
		if err := b.db.mem.Set(uint64(blockID), memseq, data); err != nil {
			return err
		}
		if ok := b.db.timeWindow.add(b.tinyBatch.timeID(), e.topicHash, newWinEntry(e.seq, e.expiresAt)); !ok {
			return errForbidden
		}
		b.tinyBatch.entries = append(b.tinyBatch.entries, e.seq)
		return nil
	})

	b.tinyBatchLockC <- struct{}{}
	b.db.batchPool.write(b.tinyBatch)
	b.tinyBatch = b.db.newTinyBatch()
	<-b.tinyBatchLockC

	return nil
}

// batchWriteLoop handles batch partial writes using tiny batches.
func (b *Batch) writeLoop(interval time.Duration) {
	b.db.closeW.Add(1)
	defer b.db.closeW.Done()
	tinyBatchTicker := time.NewTicker(interval)
	for {
		select {
		case <-b.commitComplete:
			tinyBatchTicker.Stop()
			return
		case <-b.db.closeC:
			tinyBatchTicker.Stop()
			return
		case <-tinyBatchTicker.C:
			if err := b.Write(); err != nil {
				logger.Error().Err(err).Str("context", "batchWriteLoop").Msgf("Error writing tinyBatch")
			}
		}
	}
}

// Commit commits changes to the DB. In batch operation commit is managed and client is not allowed to call Commit.
// On Commit complete batch operation signal to the caller if the batch is fully commited to DB.
func (b *Batch) Commit() error {
	_assert(!b.managed, "managed batch commit not allowed")
	b.db.closeW.Add(1)
	defer func() {
		close(b.commitComplete)
		b.db.closeW.Done()
		b.Abort()
	}()

	// Write if any pending entries in batch
	if err := b.Write(); err != nil {
		return err
	}
	for timeID, tinyBatch := range b.tinyBatchGroup {
		<-tinyBatch.doneChan
		b.db.releaseTimeID(timeID)
	}

	b.tinyBatchGroup = make(map[int64]*tinyBatch)
	return nil
}

//Abort abort is a batch cleanup operation on batch complete.
func (b *Batch) Abort() {
	_assert(!b.managed, "managed batch abort not allowed")
	for _, tinyBatch := range b.tinyBatchGroup {
		b.db.rollback(tinyBatch)
	}
	// abort time window entries
	b.db.abort()
	b.db = nil
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
