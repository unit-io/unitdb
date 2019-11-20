package tracedb

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/saffat-in/tracedb/hash"
	"github.com/saffat-in/tracedb/message"
)

const (
	batchHeaderLen = 8 + 4
	batchGrowRec   = 3000
	// batchBufioSize = 16
)

type batchIndex struct {
	delFlag   bool
	valHash   uint32 // valHash is local id unique in batch and used to removed duplicate entry from bacth before writing records to db
	topicSize uint16
	valueSize uint32
	expiresAt uint32
	kvOffset  int
}

func (index batchIndex) k(data []byte) []byte {
	return data[index.kvOffset : index.kvOffset+keySize]
}

func (index batchIndex) kvSize() uint32 {
	return keySize + uint32(index.topicSize) + index.valueSize
}

func (index batchIndex) kv(data []byte) (topic, key, value []byte) {
	keyValue := data[index.kvOffset : index.kvOffset+int(index.kvSize())]
	return keyValue[keySize : index.topicSize+keySize], keyValue[:keySize], keyValue[keySize+index.topicSize:]

}

// BatchOptions is used to set options when using batch operation
type BatchOptions struct {
	// In concurrent batch writes order determines how to handle conflicts
	Order      int8
	Encryption bool
}

var wg sync.WaitGroup

// DefaultBatchOptions contains default options when writing batches to Tracedb key-value store.
var DefaultBatchOptions = &BatchOptions{
	Order:      0,
	Encryption: false,
}

func (b *Batch) SetOptions(opts *BatchOptions) {
	b.opts = opts
}

// Batch is a write batch.
type Batch struct {
	opts          *BatchOptions
	managed       bool
	grouped       bool
	order         int8
	seq           uint64
	db            *DB
	data          []byte
	index         []batchIndex
	pendingWrites []batchIndex
	firstKeyHash  uint32
	valHashs      []uint32

	// internalLen is sums of key/value pair length plus 8-bytes internal key.
	// internalLen uint32
}

func (b *Batch) grow(n int) {
	o := len(b.data)
	if cap(b.data)-o < n {
		div := 1
		if len(b.index) > batchGrowRec {
			div = len(b.index) / batchGrowRec
		}
		ndata := make([]byte, o, o+n+o/div)
		copy(ndata, b.data)
		b.data = ndata
	}
}

func (b *Batch) appendRec(dFlag bool, valHash uint32, topic, key, value []byte, expiresAt uint32) {
	n := 1 + len(key)
	n += len(topic)
	if !dFlag {
		n += len(value)
	}
	b.grow(n)
	index := batchIndex{delFlag: dFlag, valHash: valHash, topicSize: uint16(len(topic))}
	o := len(b.data)
	data := b.data[:o+n]
	if dFlag {
		data[o] = 1
	} else {
		data[o] = 0
	}
	o++
	index.kvOffset = o
	o += copy(data[o:], key)
	o += copy(data[o:], topic)
	if !dFlag {
		index.valueSize = uint32(len(value))
		o += copy(data[o:], value)
	}
	b.data = data[:o]
	index.expiresAt = expiresAt
	b.index = append(b.index, index)
}

func (b *Batch) mput(dFlag bool, valHash uint32, topic, key, value []byte, expiresAt uint32) error {
	switch {
	case len(key) == 0:
		return errKeyEmpty
	case len(key) > MaxKeyLength:
		return errKeyTooLarge
	case len(value) > MaxValueLength:
		return errValueTooLarge
	}
	if b.hasWriteConflict(valHash) {
		return errWriteConflict
	}
	if err := b.db.mem.Put(topic, b.seq+1, dFlag, key, value, expiresAt); err != nil {
		return err
	}
	if b.firstKeyHash == 0 {
		b.firstKeyHash = b.db.hash(key)
	}
	b.seq++
	return nil
}

// Put appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) Put(key, value []byte) error {
	return b.PutEntry(message.NewEntry(key, value))
}

// PutEntry appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) PutEntry(e *message.Entry) error {
	topic := new(message.Topic)
	if e.Contract == 0 {
		e.Contract = message.Contract
	}
	//Parse the Key
	topic.ParseKey(e.Topic)
	e.Topic = topic.Topic
	// Parse the topic
	topic.Parse(e.Contract, true)
	if topic.TopicType == message.TopicInvalid {
		return errBadRequest
	}
	// // Put should only have static topic strings
	// if topic.TopicType != message.TopicStatic {
	// 	return errForbidden
	// }

	// In case of ttl, add ttl to the msg and store to the db
	if ttl, ok := topic.TTL(); ok {
		//1410065408 10 sec
		e.ExpiresAt = uint32(time.Now().Add(time.Duration(ttl)).Unix())
	}
	topic.AddContract(e.Contract)
	if e.ID != nil {
		e.ID.SetContract(topic.Parts)
	} else {
		e.ID = message.NewID(topic.Parts)
	}
	m, err := e.Marshal()
	if err != nil {
		return err
	}
	val := snappy.Encode(nil, m)
	valHash := hash.WithSalt(val, topic.GetHashCode())
	// Encryption.
	if b.opts.Encryption == true {
		e.ID.SetEncryption()
		val = b.db.mac.Encrypt(nil, val)
	}

	b.appendRec(false, valHash, topic.Marshal(), e.ID, val, e.ExpiresAt)

	return nil
}

// Delete appends 'delete operation' of the given key to the batch.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) Delete(key []byte) error {
	return b.DeleteEntry(message.NewEntry(key, nil))
}

// Delete appends 'delete operation' of the given key to the batch.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) DeleteEntry(e *message.Entry) error {
	if e.ID == nil {
		return errKeyEmpty
	}
	topic := new(message.Topic)
	if e.Contract == 0 {
		e.Contract = message.Contract
	}
	//Parse the Key
	topic.ParseKey(e.Topic)
	e.Topic = topic.Topic
	// Parse the topic
	topic.Parse(e.Contract, true)
	if topic.TopicType == message.TopicInvalid {
		return errBadRequest
	}

	topic.AddContract(e.Contract)
	e.ID.SetContract(topic.Parts)
	valHash := topic.GetHashCode()
	b.appendRec(true, valHash, topic.Marshal(), e.ID, nil, 0)
	return nil
}

func (b *Batch) hasWriteConflict(valHash uint32) bool {
	for _, batch := range b.db.activeBatches {
		for _, hash := range batch {
			if hash == valHash {
				return true
			}
		}
	}
	return false
}

func (b *Batch) writeInternal(fn func(i int, dFlag bool, valHash uint32, topic, k, v []byte, expiresAt uint32) error) error {
	start := time.Now()
	defer logger.Debug().Str("context", "batch.writeInternal").Dur("duration", time.Since(start)).Msg("")
	for i, index := range b.pendingWrites {
		topic, key, val := index.kv(b.data)
		if err := fn(i, index.delFlag, index.valHash, topic, key, val, index.expiresAt); err != nil {
			return err
		}
	}
	return nil
}

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

	b.seq = b.db.mem.getSeq()
	err := b.writeInternal(func(i int, dFlag bool, valHash uint32, topic, k, v []byte, expiresAt uint32) error {
		return b.mput(dFlag, valHash, topic, k, v, expiresAt)
	})

	if err == nil {
		b.db.activeBatches[b.seq] = b.ValHashs()
		b.db.mem.setSeq(b.seq)
	}

	return err
}

func (b *Batch) commit() error {
	if len(b.pendingWrites) == 0 {
		return nil
	}

	it := b.db.mem.Items(b.seq-uint64(b.Len()), b.seq)
	for it.First(); it.Valid(); it.Next() {
		err := it.Error()
		if err != nil {
			logger.Error().Err(err).Str("context", "batch.commit").Int8("order", b.order).Int("Length", b.Len())
			return err
		}
		keyHash := b.db.hash(it.Item().Key())
		if it.Item().DeleteFlag() {
			/// Test filter block for presence
			if !b.db.filter.Test(uint64(keyHash)) {
				return nil
			}
			b.db.delete(it.Item().Key())
			itopic := new(message.Topic)
			itopic.Unmarshal(it.Item().Topic())
			if ok := b.db.trie.Remove(itopic.Parts, keyHash); ok {
			}
		} else {
			b.db.put(it.Item().Topic(), it.Item().Key(), it.Item().Value(), it.Item().ExpiresAt())
			itopic := new(message.Topic)
			itopic.Unmarshal(it.Item().Topic())
			if ok := b.db.trie.Add(itopic.Parts, itopic.Depth, keyHash); ok {
			}
		}
	}
	return nil
}

func (b *Batch) Commit() error {
	_assert(!b.managed, "managed tx commit not allowed")
	if b.db.mem == nil || b.db.mem.getref() == 0 {
		return nil
	}
	err := b.commit()
	if err != nil {
		//remove batch from activeBatches after commit
		delete(b.db.activeBatches, b.seq)
	}

	return err
}

func (b *Batch) Abort() {
	_assert(!b.managed, "managed tx abort not allowed")
	b.Reset()
	b.db = nil
}

// Reset resets the batch.
func (b *Batch) Reset() {
	b.data = b.data[:0]
	b.index = b.index[:0]
	// b.internalLen = 0
}

func (b *Batch) uniq() []batchIndex {
	type indices struct {
		idx    int
		newidx int
	}
	unique_set := make(map[uint32]indices, len(b.index))
	i := 0
	for idx := len(b.index) - 1; idx >= 0; idx-- {
		if _, ok := unique_set[b.index[idx].valHash]; !ok {
			unique_set[b.index[idx].valHash] = indices{idx, i}
			i++
		}
	}

	b.pendingWrites = make([]batchIndex, len(unique_set))
	for k, i := range unique_set {
		b.valHashs = append(b.valHashs, k)
		b.pendingWrites[len(unique_set)-i.newidx-1] = b.index[i.idx]
	}
	return b.pendingWrites
}

func (b *Batch) append(bnew *Batch) {
	off := len(b.data)
	for _, idx := range bnew.index {
		idx.kvOffset = idx.kvOffset + off
		b.index = append(b.index, idx)
	}
	//b.grow(len(bnew.data))
	b.data = append(b.data, bnew.data...)
	// b.internalLen += bnew.internalLen
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

// keys returns keys in active batch.
func (b *Batch) ValHashs() []uint32 {
	return b.valHashs
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
	// b.batchGroup = g
	b.grouped = true
}

// unsetGrouped unset grouping.
func (b *Batch) unsetGrouped() {
	b.grouped = false
}

func (b *Batch) setOrder(order int8) {
	b.order = order
}
