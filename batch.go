package unitdb

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/hash"
	"github.com/unit-io/unitdb/message"
	"github.com/unit-io/unitdb/uid"
)

// BatchOptions is used to set options when using batch operation
type BatchOptions struct {
	// In concurrent batch writes order determines how to handle conflicts
	Order           int8
	Topic           []byte
	Contract        uint32
	Encryption      bool
	AllowDuplicates bool
}

// DefaultBatchOptions contains default options when writing batches to unitdb key-value store.
var DefaultBatchOptions = &BatchOptions{
	Order:           0,
	Topic:           nil,
	Contract:        message.MasterContract,
	Encryption:      false,
	AllowDuplicates: false,
}

func (index batchIndex) message(data []byte) (id, topic []byte) {
	return data[:idSize], data[idSize : idSize+index.topicSize]
}

// SetOptions sets batch options to defer default option and use options specified by client program
func (b *Batch) SetOptions(opts *BatchOptions) {
	b.opts = opts
}

type (
	batchInfo struct {
		entryCount int
	}

	batchIndex struct {
		delFlag   bool
		key       uint32 // key is local id unique in batch and used to removed duplicate entry from bacth before writing records into DB
		topicSize uint16
		offset    int64
	}

	// Batch is a write batch.
	Batch struct {
		batchId uid.LID
		opts    *BatchOptions
		managed bool
		grouped bool
		order   int8
		batchInfo
		buffer *bpool.Buffer
		size   int64

		db            *DB
		index         []batchIndex
		pendingWrites []batchIndex

		// commitComplete is used to signal if batch commit is complete and batch is fully written to write ahead log
		commitW        sync.WaitGroup
		commitComplete chan struct{}
	}
)

// Put adds entry to batch for given topic->key/value.
// Client must provide Topic to the BatchOptions.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) Put(value []byte) error {
	switch {
	case len(b.opts.Topic) == 0:
		return errTopicEmpty
	case len(b.opts.Topic) > MaxTopicLength:
		return errTopicTooLarge
	}
	return b.PutEntry(&Entry{Topic: b.opts.Topic, Payload: value, Contract: b.opts.Contract})
}

// PutEntry appends entries to a bacth for given topic->key/value pair.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) PutEntry(e *Entry) error {
	switch {
	case len(e.Topic) == 0:
		return errTopicEmpty
	case len(e.Topic) > MaxTopicLength:
		return errTopicTooLarge
	case len(e.Payload) == 0:
		return errValueEmpty
	case len(e.Payload) > MaxValueLength:
		return errValueTooLarge
	}
	if e.Contract == 0 {
		e.Contract = message.MasterContract
	}
	topic, ttl, err := b.db.parseTopic(e)
	if err != nil {
		return err
	}
	if ttl > 0 {
		e.ExpiresAt = uint32(time.Now().Add(time.Duration(ttl)).Unix())
	}
	topic.AddContract(e.Contract)
	e.topic.data = topic.Marshal()
	e.topic.size = uint16(len(e.topic.data))
	e.contract = message.Contract(topic.Parts)
	e.encryption = b.opts.Encryption
	if err := b.db.setEntry(e, ttl); err != nil {
		return err
	}
	var key uint32
	if !b.opts.AllowDuplicates {
		key = hash.WithSalt(e.val, topic.GetHashCode())
	}
	// Encryption.
	if e.encryption {
		e.val = b.db.mac.Encrypt(nil, e.val)
	}

	data, err := b.db.packEntry(e)
	if err != nil {
		return err
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(data)+4))

	if _, err := b.buffer.Write(scratch[:]); err != nil {
		return err
	}
	if _, err := b.buffer.Write(data); err != nil {
		return err
	}
	b.index = append(b.index, batchIndex{delFlag: false, key: key, topicSize: e.topic.size, offset: b.size})
	b.size += int64(len(data) + 4)
	b.entryCount++

	return nil
}

// Delete appends delete entry to batch for given key.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) Delete(id, topic []byte) error {
	return b.DeleteEntry(&Entry{ID: id, Topic: topic})
}

// DeleteEntry appends entry for deletion to a batch for given key.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) DeleteEntry(e *Entry) error {
	switch {
	case len(e.ID) == 0:
		return errMsgIdEmpty
	case len(e.Topic) == 0:
		return errTopicEmpty
	case len(e.Topic) > MaxTopicLength:
		return errTopicTooLarge
	}
	topic, _, err := b.db.parseTopic(e)
	if err != nil {
		return err
	}
	if e.Contract == 0 {
		e.Contract = message.MasterContract
	}
	topic.AddContract(e.Contract)
	e.topic.data = topic.Marshal()
	e.topic.size = uint16(len(e.topic.data))
	e.contract = message.Contract(topic.Parts)
	id := message.ID(e.ID)
	id.AddContract(e.contract)
	e.id = id
	e.seq = id.Seq()
	key := topic.GetHashCode()
	data, err := b.db.packEntry(e)
	if err != nil {
		return err
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(data)+4))

	if _, err := b.buffer.Write(scratch[:]); err != nil {
		return err
	}
	if _, err := b.buffer.Write(data); err != nil {
		return err
	}
	b.index = append(b.index, batchIndex{delFlag: true, key: key, topicSize: e.topic.size, offset: b.size})
	b.size += int64(len(data) + 4)
	b.entryCount++
	return nil
}

func (b *Batch) writeInternal(fn func(i int, contract uint64, memseq uint64, data []byte) error) error {
	if err := b.db.ok(); err != nil {
		return err
	} // // CPU profiling by default
	// defer profile.Start().Stop()
	// start := time.Now()
	// defer logger.Debug().Str("context", "batch.writeInternal").Dur("duration", time.Since(start)).Msg("")

	buf := b.buffer.Bytes()
	topics := make(map[uint64]*message.Topic)
	for i, index := range b.pendingWrites {
		dataLen := binary.LittleEndian.Uint32(buf[index.offset : index.offset+4])
		data := buf[index.offset+4 : index.offset+int64(dataLen)]
		id, rawTopic := index.message(data[entrySize:])

		ID := message.ID(id)
		seq := ID.Seq()
		contract := ID.Contract()
		if _, ok := topics[contract]; !ok {
			t := new(message.Topic)
			t.Unmarshal(rawTopic)
			topics[contract] = t
			if ok := b.db.trie.add(t.GetHash(contract), t.Parts, t.Depth); !ok {
				return errBadRequest
			}
		}
		topic := topics[contract]
		topicHash := topic.GetHash(contract)
		we := winEntry{
			contract: contract,
			seq:      seq,
		}
		if index.delFlag {
			/// Test filter block for presence
			if !b.db.filter.Test(seq) {
				return nil
			}
			b.db.delete(contract, seq)
			continue
		}
		b.db.timeWindow.add(topicHash, we)

		memseq := b.db.cacheID ^ seq
		if err := fn(i, contract, memseq, data); err != nil {
			return err
		}
	}
	return nil
}

// Write starts writing entries into DB. It returns an error if batch write fails.
func (b *Batch) Write() error {
	// The write happen synchronously.
	b.db.writeLockC <- struct{}{}
	defer func() {
		<-b.db.writeLockC
	}()
	b.uniq()
	if b.grouped {
		// append batch to batchgroup
		b.db.batchQueue <- b
		return nil
	}
	err := b.writeInternal(func(i int, contract uint64, memseq uint64, data []byte) error {
		return b.db.mem.Set(contract, memseq, data)
	})
	if err != nil {
		return err
	}
	return nil
}

// Commit commits changes to the DB. In batch operation commit is managed and client is not allowed to call Commit.
// On Commit complete batch operation signal to the cliend if the batch is fully commmited to DB.
func (b *Batch) Commit() error {
	_assert(!b.managed, "managed tx commit not allowed")
	if len(b.pendingWrites) == 0 || b.buffer.Size() == 0 {
		return nil
	}
	defer func() {
		close(b.commitComplete)
		b.Abort()
	}()
	if err := b.db.commit(b.Len(), b.buffer); err != nil {
		logger.Error().Err(err).Str("context", "commit").Msgf("Error committing batch")
	}
	return nil
}

//Abort abort is a batch cleanup operation on batch complete
func (b *Batch) Abort() {
	_assert(!b.managed, "managed tx abort not allowed")
	// b.Reset()
	b.db.bufPool.Put(b.buffer)
	b.db = nil
}

// // Reset resets the batch.
// func (b *Batch) Reset() {
// 	b.entryCount = 0
// 	b.size = 0
// 	b.index = b.index[:0]
// 	b.pendingWrites = b.pendingWrites[:0]
// 	// b.buffer.Reset()
// }

func (b *Batch) uniq() []batchIndex {
	if b.opts.AllowDuplicates {
		b.pendingWrites = append(make([]batchIndex, 0, len(b.index)), b.index...)
		return b.pendingWrites
	}
	type indices struct {
		idx    int
		newidx int
	}
	uniqueSet := make(map[uint32]indices, len(b.index))
	i := 0
	for idx := len(b.index) - 1; idx >= 0; idx-- {
		if _, ok := uniqueSet[b.index[idx].key]; !ok {
			uniqueSet[b.index[idx].key] = indices{idx, i}
			i++
		}
	}

	b.pendingWrites = make([]batchIndex, len(uniqueSet))
	for _, i := range uniqueSet {
		b.pendingWrites[len(uniqueSet)-i.newidx-1] = b.index[i.idx]
	}
	return b.pendingWrites
}

func (b *Batch) append(bnew *Batch) {
	if bnew.Len() == 0 {
		return
	}
	off := b.size
	for _, idx := range bnew.index {
		idx.offset = idx.offset + int64(off)
		b.index = append(b.index, idx)
	}
	b.size += bnew.size
	b.buffer.Write(bnew.buffer.Bytes())
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

// Len returns number of records in the batch.
func (b *Batch) Len() int {
	return len(b.pendingWrites)
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
