package tracedb

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/unit-io/tracedb/bpool"
	"github.com/unit-io/tracedb/hash"
	"github.com/unit-io/tracedb/message"
)

const (
	batchHeaderLen = 8 + 4
	batchGrowRec   = 3000
)

// BatchOptions is used to set options when using batch operation
type BatchOptions struct {
	// In concurrent batch writes order determines how to handle conflicts
	Order           int8
	Size            int64
	Topic           []byte
	Contract        uint32
	Encryption      bool
	AllowDuplicates bool
}

// DefaultBatchOptions contains default options when writing batches to Tracedb key-value store.
var DefaultBatchOptions = &BatchOptions{
	Order:           0,
	Size:            1 << 33,
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
		entryCount uint16
	}

	batchIndex struct {
		delFlag   bool
		key       uint32 // key is local id unique in batch and used to removed duplicate entry from bacth before writing records to db
		topicSize uint16
		offset    int64
	}

	// Batch is a write batch.
	Batch struct {
		batchId uint64
		opts    *BatchOptions
		managed bool
		grouped bool
		order   int8
		batchInfo
		buffer *bpool.Buffer
		size   int64
		logs   []log
		mu     sync.Mutex

		db            *DB
		index         []batchIndex
		pendingWrites []batchIndex

		// commitComplete is used to signal if batch commit is complete and batch is fully written to write ahead log
		commitComplete chan struct{}
	}
)

// Put appends 'put operation' of the given topic->key/value pair to the batch.
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

// PutEntry appends 'put operation' of the given key/value pair to the batch.
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
	topic, err := b.db.parseTopic(e)
	if err != nil {
		return err
	}
	e.topic = topic.Marshal()
	e.contract = message.Contract(topic.Parts)
	b.db.setEntry(e)
	var key uint32
	if !b.opts.AllowDuplicates {
		key = hash.WithSalt(e.val, topic.GetHashCode())
	}
	// Encryption.
	if b.opts.Encryption == true {
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
	// b.appendRec(false, e.contract, e.seq, key, e.id, e.topic, e.val, e.ExpiresAt)

	return nil
}

// Delete appends 'delete operation' of the given key to the batch.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) Delete(id, topic []byte) error {
	return b.DeleteEntry(&Entry{ID: id, Topic: topic})
}

// DeleteEntry appends 'delete operation' of the given key to the batch.
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
	topic, err := b.db.parseTopic(e)
	if err != nil {
		return err
	}
	e.topic = topic.Marshal()
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
	b.index = append(b.index, batchIndex{delFlag: true, key: key, topicSize: uint16(len(e.topic)), offset: b.size})
	b.size += int64(len(data) + 4)
	b.entryCount++
	// b.appendRec(true, e.contract, e.seq, key, e.id, e.topic, nil, 0)
	return nil
}

func (b *Batch) writeInternal(fn func(i int, contract uint64, memseq uint64, data []byte) error) error {
	// // CPU profiling by default
	// defer profile.Start().Stop()
	// start := time.Now()
	// defer logger.Debug().Str("context", "batch.writeInternal").Dur("duration", time.Since(start)).Msg("")

	buff := b.buffer.Bytes()
	topics := make(map[uint64]*message.Topic)
	for i, index := range b.pendingWrites {
		dataLen := binary.LittleEndian.Uint32(buff[index.offset : index.offset+4])
		data := buff[index.offset+4 : index.offset+int64(dataLen)]
		id, topic := index.message(data[entrySize:])

		ID := message.ID(id)
		seq := ID.Seq()
		contract := ID.Contract()
		if _, ok := topics[contract]; !ok {
			t := new(message.Topic)
			t.Unmarshal(topic)
			topics[contract] = t
		}
		itopic := topics[contract]
		if index.delFlag {
			/// Test filter block for presence
			if !b.db.filter.Test(seq) {
				return nil
			}
			if ok := b.db.trie.Remove(contract, itopic.Parts, seq); !ok {
				return errBadRequest
			}
			b.db.delete(seq)
			continue
		}
		if ok := b.db.trie.Add(contract, itopic.Parts, itopic.Depth, seq); !ok {
			return errBadRequest
		}
		// contract := message.contract(itopic.Parts)
		memseq := b.db.cacheID ^ seq
		if err := fn(i, contract, memseq, data); err != nil {
			return err
		}
		b.logs = append(b.logs, log{contract: contract, seq: memseq})
	}
	b.db.meter.Puts.Inc(int64(len(b.pendingWrites)))
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

	return err
}

// Commit commits changes to the db. In batch operation commit is manages and client progress is not allowed to call commit.
// On Commit complete batch operation signal to the cliend program if the batch is fully commmited to db.
func (b *Batch) Commit() error {
	// defer bufPool.Put(b.tinyBatch.buffer)
	_assert(!b.managed, "managed tx commit not allowed")
	if b.db.mem == nil || b.db.mem.getref() == 0 {
		return nil
	}
	if len(b.pendingWrites) == 0 {
		return nil
	}

	b.db.commitQueue <- b
	return nil
}

//Abort abort is a batch cleanup operation on batch complete
func (b *Batch) Abort() {
	_assert(!b.managed, "managed tx abort not allowed")
	b.Reset()
	b.db = nil
}

// Reset resets the batch.
func (b *Batch) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.entryCount = 0
	b.db.bufPool.Put(b.batchId)
	// b.buffer.Reset()
	b.index = b.index[:0]
	b.pendingWrites = b.pendingWrites[:0]
}

func (b *Batch) uniq() []batchIndex {
	if b.opts.AllowDuplicates {
		b.pendingWrites = make([]batchIndex, len(b.index))
		copy(b.pendingWrites, b.index)
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
	off := b.size
	for _, idx := range bnew.index {
		idx.offset = idx.offset + int64(off)
		b.index = append(b.index, idx)
	}
	// b.buffer.Write(bnew.buffer.Bytes())
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

// Seqs returns Seqs in active batch.
func (b *Batch) Logs() []log {
	return b.logs
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
