package tracedb

import (
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/unit-io/tracedb/bpool"
	"github.com/unit-io/tracedb/hash"
	"github.com/unit-io/tracedb/message"
	"github.com/unit-io/tracedb/uid"
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

// DefaultBatchOptions contains default options when writing batches to Tracedb key-value store.
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
		key       uint32 // key is local id unique in batch and used to removed duplicate entry from bacth before writing records to db
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
	case len(e.Payload) > MaxValueLength:
		return errValueTooLarge
	}
	topic, ttl, err := b.db.parseTopic(e)
	if err != nil {
		return err
	}
	if ttl > 0 {
		e.ExpiresAt = uint32(time.Now().Add(time.Duration(ttl)).Unix())
	}
	e.topic = topic.Marshal()
	e.contract = message.Contract(topic.Parts)
	e.topicHash = topic.GetHash(e.contract)
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
	b.index = append(b.index, batchIndex{delFlag: false, key: key, topicSize: uint16(len(e.topic)), offset: b.size})
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
	case len(e.Payload) > MaxValueLength:
		return errValueTooLarge
	}
	topic, _, err := b.db.parseTopic(e)
	if err != nil {
		return err
	}
	e.topic = topic.Marshal()
	e.contract = message.Contract(topic.Parts)
	e.topicHash = topic.GetHash(e.contract)
	id := message.ID(e.ID)
	id.AddContract(e.contract)
	e.id = id
	e.seq = id.Seq()
	key := topic.GetHashCode()
	e.topicOffset, _ = b.db.trie.getOffset(topic.GetHash(e.contract))
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
	b.index = append(b.index, batchIndex{delFlag: true, key: key, topicSize: uint16(len(e.topic)), offset: b.size})
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
		id, ptopic := index.message(data[entrySize:])

		ID := message.ID(id)
		seq := ID.Seq()
		contract := ID.Contract()
		if _, ok := topics[contract]; !ok {
			t := new(message.Topic)
			t.Unmarshal(ptopic)
			topics[contract] = t
			if ok := b.db.trie.addTopic(contract, t.GetHash(contract), t.Parts, t.Depth); !ok {
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
			if ok := b.db.trie.remove(topicHash, we); !ok {
				return errBadRequest
			}
			b.db.delete(seq)
			continue
		}
		// if ok := b.db.trie.addTopic(contract, topicHash, topic.Parts, topic.Depth); !ok {
		// 	return errBadRequest
		// }
		b.db.timeWindow.add(topicHash, we)

		memseq := b.db.cacheID ^ seq
		if err := fn(i, contract, memseq, data); err != nil {
			return err
		}
	}
	return nil
}

// Write starts writing entries into db. it returns an error to the batch if any
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
	if len(b.pendingWrites) == 0 || b.buffer.Size() == 0 {
		return nil
	}
	buf := b.db.bufPool.Get()
	buf.Write(b.buffer.Bytes())
	go func(l int, data []byte) error {
		defer b.db.bufPool.Put(buf)
		return b.commit(l, data)
	}(b.Len(), buf.Bytes())
	b.Reset()
	return nil
}

func (b *Batch) commit(l int, data []byte) error {
	b.db.writeLockC <- struct{}{}
	defer func() {
		<-b.db.writeLockC
	}()
	err := b.db.commit(l, data)
	if err1 := <-err; err1 != nil {
		logger.Error().Err(err1).Str("context", "commit").Msgf("Error committing batch")
	}
	return nil
}

// Commit commits changes to the db. In batch operation commit is manages and client progress is not allowed to call commit.
// On Commit complete batch operation signal to the cliend program if the batch is fully commmited to db.
func (b *Batch) Commit() error {
	_assert(!b.managed, "managed tx commit not allowed")
	defer close(b.commitComplete)
	if len(b.pendingWrites) == 0 || b.buffer.Size() == 0 {
		return nil
	}
	buf := b.db.bufPool.Get()
	buf.Write(b.buffer.Bytes())
	return b.commit(b.Len(), buf.Bytes())
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
	b.entryCount = 0
	b.size = 0
	b.index = b.index[:0]
	b.pendingWrites = b.pendingWrites[:0]
	b.buffer.Reset()
}

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
