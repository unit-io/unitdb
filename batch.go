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

	// commitComplete is used to signal if batch commit is complete and batch is fully written to DB.
	commmitTimeIDs []int64
	commitW        sync.WaitGroup
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
	if err := b.db.setEntry(e); err != nil {
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

	if err := b.db.setEntry(e); err != nil {
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

	data := b.tinyBatch.buffer.Bytes()
	var e entry

	for i, index := range b.tinyBatch.index {
		off := index.offset
		dataLen := binary.LittleEndian.Uint32(data[off : off+4])
		entryData := data[off+4 : off+entrySize+4]
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
		if err := fn(i, e, data[off+4:off+int64(dataLen)]); err != nil {
			return err
		}
	}
	return nil
}

// Write starts writing entries into DB. It returns an error if batch write fails.
func (b *Batch) Write() error {
	if b.tinyBatch.len() == 0 {
		return nil
	}

	topics := make(map[uint64]*message.Topic)

	b.writeInternal(func(i int, e entry, data []byte) error {
		blockID := startBlockIndex(e.seq)
		memseq := b.db.cacheID ^ e.seq
		if err := b.db.mem.Set(uint64(blockID), memseq, data); err != nil {
			return err
		}
		if err := b.db.timeWindow.add(b.tinyBatch.timeID(), e.topicHash, newWinEntry(e.seq, e.expiresAt)); err != nil {
			return nil
		}
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
		return nil
	})

	b.commmitTimeIDs = append(b.commmitTimeIDs, b.tinyBatch.timeID())

	b.tinyBatchLockC <- struct{}{}
	b.db.batchPool.write(b.tinyBatch)
	b.tinyBatch = &tinyBatch{ID: b.db.timeID(), managed: true, buffer: b.db.bufPool.Get(), doneChan: make(chan struct{})}
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

	defer func() {
		close(b.commitComplete)
	}()

	// Write if any pending entries in batch
	if err := b.Write(); err != nil {
		return err
	}
	for _, timeID := range b.commmitTimeIDs {
		b.db.releaseTimeID(timeID)
	}

	return nil
}

//Abort abort is a batch cleanup operation on batch complete.
func (b *Batch) Abort() {
	_assert(!b.managed, "managed batch abort not allowed")
	b.tinyBatch.abort()
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
