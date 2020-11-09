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

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/memdb"
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
type (
	_BatchIndex struct {
		delFlag bool
		offset  int64
	}

	Batch struct {
		db      *DB
		mem     *memdb.Batch
		opts    *_Options
		managed bool

		index  []_BatchIndex
		buffer *bpool.Buffer
		size   int64

		// commitComplete is used to signal if batch commit is complete and batch is fully written to DB.
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
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(e.entry.cache)+4))
	if _, err := b.buffer.Write(scratch[:]); err != nil {
		return err
	}
	if _, err := b.buffer.Write(e.entry.cache); err != nil {
		return err
	}

	b.index = append(b.index, _BatchIndex{delFlag: false, offset: b.size})
	b.size += int64(len(e.entry.cache) + 4)

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
	case b.db.opts.flags.immutable:
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
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(e.entry.cache)+4))
	if _, err := b.buffer.Write(scratch[:]); err != nil {
		return err
	}
	if _, err := b.buffer.Write(e.entry.cache); err != nil {
		return err
	}

	b.index = append(b.index, _BatchIndex{delFlag: true, offset: b.size})
	b.size += int64(len(e.entry.cache) + 4)

	// reset message entry
	e.reset()

	return nil
}

func (b *Batch) writeInternal(fn func(i int, e _Entry, data []byte) error) error {
	if err := b.db.ok(); err != nil {
		return err
	}

	var e _Entry

	for i, index := range b.index {
		off := index.offset
		data, err := b.buffer.Slice(off, off+4)
		if err != nil {
			return err
		}
		dataLen := int64(binary.LittleEndian.Uint32(data))
		entryData, err := b.buffer.Slice(off+4, off+entrySize+4)
		if err != nil {
			return err
		}
		if err := e.UnmarshalBinary(entryData); err != nil {
			return err
		}
		if index.delFlag && e.seq != 0 {
			/// Test filter block for presence.
			if !b.db.internal.filter.Test(e.seq) {
				return nil
			}
			b.db.delete(e.topicHash, e.seq)
			continue
		}

		// put packed entry into memdb.
		data = data[:0]
		data, err = b.buffer.Slice(off+4, off+dataLen)
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
	if b.len() == 0 {
		return nil
	}

	topics := make(map[uint64]*message.Topic)
	timeID := b.mem.TimeID()

	b.writeInternal(func(i int, e _Entry, data []byte) error {
		if e.topicSize != 0 {
			t, ok := topics[e.topicHash]
			if !ok {
				t = new(message.Topic)
				rawTopic := data[entrySize+idSize : entrySize+idSize+e.topicSize]
				t.Unmarshal(rawTopic)
				topics[e.topicHash] = t
			}
			b.db.internal.trie.add(newTopic(e.topicHash, 0), t.Parts, t.Depth)
		}
		if err := b.mem.Put(e.seq, data); err != nil {
			return err
		}
		if ok := b.db.internal.timeWindow.add(timeID, e.topicHash, newWinEntry(e.seq, e.expiresAt)); !ok {
			return errForbidden
		}
		return nil
	})

	b.mem.Write()
	b.reset()

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

	// Write if any pending entries in batch.
	if err := b.Write(); err != nil {
		return err
	}

	// Commit batch to database.
	if err := b.mem.Commit(); err != nil {
		return err
	}

	return nil
}

func (b *Batch) reset() {
	b.index = b.index[:0]
	b.size = 0
	b.buffer.Reset()
}

//Abort abort is a batch cleanup operation on batch complete.
func (b *Batch) Abort() {
	_assert(!b.managed, "managed batch abort not allowed")

	b.reset()
	b.mem.Abort()
	b.db.internal.bufPool.Put(b.buffer)
	b.db = nil
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

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
