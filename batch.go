package tracedb

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/saffat-in/tracedb/hash"
	"github.com/saffat-in/tracedb/message"
)

const (
	batchHeaderLen = 8 + 4
	batchGrowRec   = 3000
)

type batchIndex struct {
	delFlag   bool
	key       uint32 // key is local id unique in batch and used to removed duplicate entry from bacth before writing records to db
	idSize    uint16
	topicSize uint16
	valueSize uint32
	expiresAt uint32
	tmOffset  int64
}

func (index batchIndex) id(data []byte) []byte {
	return data[index.tmOffset : index.tmOffset+int64(index.idSize)]
}

func (index batchIndex) tmSize() uint32 {
	return uint32(index.idSize) + uint32(index.topicSize) + index.valueSize
}

func (index batchIndex) tm(data []byte) (id, topic, value []byte) {
	keyValue := data[index.tmOffset : index.tmOffset+int64(index.tmSize())]
	return keyValue[:index.idSize], keyValue[index.idSize : index.idSize+index.topicSize], keyValue[index.idSize+index.topicSize:]

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
	keys          []uint32
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

func (b *Batch) appendRec(dFlag bool, key uint32, id, topic, value []byte, expiresAt uint32) {
	n := 1 + len(id)
	n += len(topic)
	if !dFlag {
		n += len(value)
	}
	b.grow(n)
	index := batchIndex{}
	o := len(b.data)
	data := b.data[:o+n]
	if dFlag {
		data[o] = 1
	} else {
		data[o] = 0
	}
	o++
	index.tmOffset = int64(o)
	index.key = key
	index.idSize = uint16(len(id))
	index.topicSize = uint16(len(topic))
	o += copy(data[o:], id)
	o += copy(data[o:], topic)
	if !dFlag {
		index.valueSize = uint32(len(value))
		o += copy(data[o:], value)
	}
	b.data = data[:o]
	index.expiresAt = expiresAt
	b.index = append(b.index, index)
}

func (b *Batch) mput(id, topic, value []byte, offset int64, expiresAt uint32) error {
	hash := b.db.hash(id)
	seq := b.db.cacheID ^ message.ID(id).Seq()
	if err := b.db.mem.Put(seq, hash, id, topic, value, offset, expiresAt); err != nil {
		return err
	}
	b.seq++
	return nil
}

// Put appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) Put(key, value []byte) error {
	return b.PutEntry(NewEntry(key, value))
}

// PutEntry appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) PutEntry(e *Entry) error {
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
	var id message.ID
	seq := b.db.nextSeq()
	if e.ID != nil {
		id = message.ID(e.ID)
		id.AddContract(topic.Parts)
	} else {
		id = message.NewID(seq, b.opts.Encryption)
		id.AddContract(topic.Parts)
	}
	m, err := e.Marshal()
	if err != nil {
		return err
	}
	val := snappy.Encode(nil, m)
	key := hash.WithSalt(val, topic.GetHashCode())
	// Encryption.
	if b.opts.Encryption == true {
		val = b.db.mac.Encrypt(nil, val)
	}

	b.appendRec(false, key, id, topic.Marshal(), val, e.ExpiresAt)

	return nil
}

// Delete appends 'delete operation' of the given key to the batch.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) Delete(key []byte) error {
	return b.DeleteEntry(NewEntry(key, nil))
}

// Delete appends 'delete operation' of the given key to the batch.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) DeleteEntry(e *Entry) error {
	if e.ID == nil {
		return errIdEmpty
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
	id := message.ID(e.ID)
	id.AddContract(topic.Parts)
	key := topic.GetHashCode()
	b.appendRec(true, key, id, topic.Marshal(), nil, 0)
	return nil
}

func (b *Batch) hasWriteConflict(key uint32) bool {
	for _, batch := range b.db.activeBatches {
		for _, hash := range batch {
			if hash == key {
				return true
			}
		}
	}
	return false
}

func (b *Batch) writeInternal(fn func(i int, id, topic, v []byte, offset int64, expiresAt uint32) error) error {
	// start := time.Now()
	// defer logger.Debug().Str("context", "batch.writeInternal").Dur("duration", time.Since(start)).Msg("")
	for i, index := range b.pendingWrites {
		// if b.hasWriteConflict(index.key) {
		// 	return errWriteConflict
		// }
		id, topic, val := index.tm(b.data)
		off, err := b.db.extend(uint32(len(val)))
		if err != nil {
			return err
		}
		if err := fn(i, id, topic, val, off, index.expiresAt); err != nil {
			b.db.freeseq.free(message.ID(id).Seq())
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

	b.seq = b.db.getSeq() + 1
	// b.db.Extend(b.seq + uint64(b.Len()))
	err := b.writeInternal(func(i int, id, topic, v []byte, offset int64, expiresAt uint32) error {
		return b.mput(id, topic, v, offset, expiresAt)
	})

	if err == nil {
		b.db.activeBatches[b.seq] = b.Keys()
	}

	return err
}

func (b *Batch) commit() error {
	if len(b.pendingWrites) == 0 {
		return nil
	}
	// The commit happen synchronously.
	b.db.writeLockC <- struct{}{}
	defer func() {
		<-b.db.writeLockC
	}()

	//precommit steps
	// b.db.extend(0)

	// l := b.Len()
	// for i, r := l-1, 0; i >= 0; i, r = i-1, r+1 {
	// 	index := b.pendingWrites[i]
	// 	id, topic, val := index.tv(b.data)
	// 	hash := b.db.hash(id)
	// 	if index.delFlag {
	// 		/// Test filter block for presence
	// 		if !b.db.filter.Test(uint64(hash)) {
	// 			return nil
	// 		}
	// 		itopic := new(message.Topic)
	// 		itopic.Unmarshal(topic)
	// 		if ok := b.db.trie.Remove(itopic.Parts, message.ID(id)); ok {
	// 			// b.db.delete(key)
	// 		}
	// 	} else {
	// 		itopic := new(message.Topic)
	// 		itopic.Unmarshal(topic)
	// 		if ok := b.db.trie.Add(itopic.Parts, itopic.Depth, message.ID(id)); ok {
	// 			off := db.blockOffset()
	// 			b := &blockHandle{table: db.index, offset: off}
	// 			if err := b.db.put(id, topic, val, index.expiresAt); err != nil {
	// 				log.Println("batch.commit: error ", err)
	// 				continue
	// 			}
	// 		}
	// 	}
	// }

	for _, index := range b.pendingWrites {
		id, topic, val := index.tm(b.data)
		hash := b.db.hash(id)
		if index.delFlag {
			/// Test filter block for presence
			if !b.db.filter.Test(uint64(hash)) {
				return nil
			}
			itopic := new(message.Topic)
			itopic.Unmarshal(topic)
			if ok := b.db.trie.Remove(itopic.Parts, message.ID(id)); ok {
				// b.db.delete(key)
			}
		} else {
			itopic := new(message.Topic)
			itopic.Unmarshal(topic)
			if ok := b.db.trie.Add(itopic.Parts, itopic.Depth, message.ID(id)); ok {
				if err := b.db.put(id, topic, val, index.expiresAt); err != nil {
					log.Println("batch.commit: error ", err)
					continue
				}
			}
		}
	}

	// cleanup memdb blocks after commit is successful to free used blocks
	// append batch seq and length to queue for cleanup
	// q := &batchqueue{startSeq: b.seq - uint64(b.Len()), endSeq: b.seq}
	// b.db.batchCleanupQueue <- q
	return nil
}

func (b *Batch) Commit() error {
	_assert(!b.managed, "managed tx commit not allowed")
	if b.db.mem == nil || b.db.mem.getref() == 0 {
		return nil
	}
	err := b.commit()
	if err == nil {
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
}

func (b *Batch) uniq() []batchIndex {
	type indices struct {
		idx    int
		newidx int
	}
	unique_set := make(map[uint32]indices, len(b.index))
	i := 0
	for idx := len(b.index) - 1; idx >= 0; idx-- {
		if _, ok := unique_set[b.index[idx].key]; !ok {
			unique_set[b.index[idx].key] = indices{idx, i}
			i++
		} else {
			// id := b.index[i].id(b.data)
			// b.db.freeseq.free(message.ID(id).Seq())
		}
	}

	b.pendingWrites = make([]batchIndex, len(unique_set))
	for k, i := range unique_set {
		b.keys = append(b.keys, k)
		b.pendingWrites[len(unique_set)-i.newidx-1] = b.index[i.idx]
	}
	return b.pendingWrites
}

func (b *Batch) append(bnew *Batch) {
	off := len(b.data)
	for _, idx := range bnew.index {
		idx.tmOffset = idx.tmOffset + int64(off)
		b.index = append(b.index, idx)
	}
	b.data = append(b.data, bnew.data...)
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

// keys returns keys in active batch.
func (b *Batch) Keys() []uint32 {
	return b.keys
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
