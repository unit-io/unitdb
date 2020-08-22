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

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/message"
)

// SetOptions sets batch options
func (b *Batch) SetOptions(opts ...Options) {
	for _, opt := range opts {
		if opt != nil {
			opt.set(b.opts)
		}
	}
}

type (
	batchIndex struct {
		delFlag bool
		offset  int64
	}

	// Batch is a write batch.
	Batch struct {
		ID      int64
		db      *DB
		opts    *options
		managed bool
		grouped bool
		order   int8
		buffer  *bpool.Buffer

		index []batchIndex
		size  int64
		// commitComplete is used to signal if batch commit is complete and batch is fully written to write ahead log
		commitW        sync.WaitGroup
		commitComplete chan struct{}
	}
)

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

	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(e.cacheEntry)+4))
	if _, err := b.buffer.Write(scratch[:]); err != nil {
		return err
	}
	if _, err := b.buffer.Write(e.cacheEntry); err != nil {
		return err
	}

	b.index = append(b.index, batchIndex{delFlag: false, offset: b.size})
	b.size += int64(len(e.cacheEntry) + 4)

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

	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(e.cacheEntry)+4))
	if _, err := b.buffer.Write(scratch[:]); err != nil {
		return err
	}
	if _, err := b.buffer.Write(e.cacheEntry); err != nil {
		return err
	}

	b.index = append(b.index, batchIndex{delFlag: true, offset: b.size})
	b.size += int64(len(e.cacheEntry) + 4)

	// reset message entry
	e.reset()

	return nil
}

func (b *Batch) writeInternal(fn func(i int, e entry, data []byte) error) error {
	if err := b.db.ok(); err != nil {
		return err
	}
	// // CPU profiling by default
	// defer profile.Start().Stop()
	// start := time.Now()
	// defer logger.Debug().Str("context", "batch.writeInternal").Dur("duration", time.Since(start)).Msg("")

	data := b.buffer.Bytes()
	var e entry

	for i, index := range b.index {
		off := index.offset
		dataLen := binary.LittleEndian.Uint32(data[off : off+4])
		entryData := data[off+4 : off+entrySize+4]
		if err := e.UnmarshalBinary(entryData); err != nil {
			return err
		}
		if index.delFlag && e.seq != 0 {
			/// Test filter block for presence
			if !b.db.filter.Test(e.seq) {
				return nil
			}
			b.db.delete(e.topicHash, e.seq)
			continue
		}

		// put packed entry without topic hash into memdb
		if err := fn(i, e, data[off+4:off+int64(dataLen)]); err != nil {
			return err
		}
	}
	return nil
}

// commit starts writing entries into DB. It returns an error if batch write fails.
func (b *Batch) commit() error {
	// The write happen synchronously.
	b.db.writeLockC <- struct{}{}
	defer func() {
		<-b.db.writeLockC
	}()
	if b.grouped {
		// append batch to batchgroup
		b.db.batchQueue <- b
		return nil
	}

	batchTicker := time.NewTicker(slotDur)
	topics := make(map[uint64]*message.Topic)

	return b.writeInternal(func(i int, e entry, data []byte) error {
		select {
		case <-batchTicker.C:
			b.ID = b.db.timeID()
		default:
		}
		blockID := startBlockIndex(e.seq)
		memseq := b.db.cacheID ^ e.seq
		if err := b.db.mem.Set(uint64(blockID), memseq, data); err != nil {
			return err
		}
		if err := b.db.timeWindow.add(b.timeID(), e.topicHash, winEntry{seq: e.seq, expiresAt: e.expiresAt}); err != nil {
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
			b.db.trie.add(topic{hash: e.topicHash}, t.Parts, t.Depth)
		}
		return nil
	})
}

// Commit commits changes to the DB. In batch operation commit is managed and client is not allowed to call Commit.
// On Commit complete batch operation signal to the cliend if the batch is fully commmited to DB.
func (b *Batch) Commit() error {
	_assert(!b.managed, "managed tx commit not allowed")
	if b.len() == 0 || b.buffer.Size() == 0 {
		b.Abort()
		return nil
	}
	defer func() {
		close(b.commitComplete)
	}()
	if err := b.commit(); err != nil {
		return err
	}
	if err := b.db.commit(b.timeID(), b.len(), b.buffer); err != nil {
		logger.Error().Err(err).Str("context", "commit").Msgf("Error committing batch")
	}

	return nil
}

//Abort abort is a batch cleanup operation on batch complete
func (b *Batch) Abort() {
	_assert(!b.managed, "managed tx abort not allowed")
	b.Reset()
	b.db.bufPool.Put(b.buffer)
	b.db = nil
}

// Reset resets the batch.
func (b *Batch) Reset() {
	b.size = 0
	b.index = b.index[:0]
}

func (b *Batch) append(new *Batch) {
	if new.len() == 0 {
		return
	}
	off := b.size
	for _, idx := range new.index {
		idx.offset = idx.offset + int64(off)
		b.index = append(b.index, idx)
	}
	b.size += new.size
	b.buffer.Write(new.buffer.Bytes())
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

// timeID returns timeID of the batch.
func (b *Batch) timeID() int64 {
	return b.ID
}

// len returns number of records in the batch.
func (b *Batch) len() int {
	return len(b.index)
}

// setManaged sets batch managed.
func (b *Batch) setManaged() {
	b.managed = true
}

// unsetManaged sets batch unmanaged.
func (b *Batch) unsetManaged() {
	b.managed = false
}

// setGrouped set grouping of multiple batches.
func (b *Batch) setGrouped(g *BatchGroup) {
	b.grouped = true
}

// unsetGrouped unset grouping.
func (b *Batch) unsetGrouped() {
	b.grouped = false
}

func (b *Batch) setOrder(order int8) {
	b.order = order
}
