package tracedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"time"
)

const (
	batchHeaderLen = 8 + 4
	batchGrowRec   = 3000
	// batchBufioSize = 16

	// Maximum value possible for sequence number; the 8-bits are
	// used by value type, so its can packed together in single
	// 64-bit integer.
	keyMaxSeq = (uint64(1) << 56) - 1
	// Maximum value possible for packed sequence number and type.
	keyMaxNum = (keyMaxSeq << 8) | 0
)

type batchIndex struct {
	delFlag   bool
	hash      uint32
	keySize   uint16
	valueSize uint32
	expiresAt uint32
	kvOffset  int
}

func (index batchIndex) k(data []byte) []byte {
	return data[index.kvOffset : index.kvOffset+int(index.keySize)]
}

func (index batchIndex) kvSize() uint32 {
	return uint32(index.keySize) + index.valueSize
}

func (index batchIndex) kv(data []byte) (key, value []byte) {
	keyValue := data[index.kvOffset : index.kvOffset+int(index.kvSize())]
	return keyValue[:index.keySize], keyValue[index.keySize:]

}

type internalKey []byte

func makeInternalKey(dst, ukey []byte, seq uint64, dFlag bool, expiresAt uint32) internalKey {
	if seq > keyMaxSeq {
		Fatal("context: makeInternalKey", "tracedb: invalid sequence number", nil)
	}

	var dBit int8
	if dFlag {
		dBit = 1
	}
	dst = ensureBuffer(dst, len(ukey)+12)
	copy(dst, ukey)
	binary.LittleEndian.PutUint64(dst[len(ukey):len(ukey)+8], (seq<<8)|uint64(dBit))
	binary.LittleEndian.PutUint32(dst[len(ukey)+8:], expiresAt)
	return internalKey(dst)
}

func parseInternalKey(ik []byte) (ukey []byte, seq uint64, dFlag bool, expiresAt uint32, err error) {
	if len(ik) < 12 {
		Error("context: parseInternalKey", "invalid internal key length")
		return
	}
	expiresAt = binary.LittleEndian.Uint32(ik[len(ik)-4:])
	num := binary.LittleEndian.Uint64(ik[len(ik)-12 : len(ik)-4])
	seq, dFlag = uint64(num>>8), num&0xff != 0
	ukey = ik[:len(ik)-12]
	return
}

// BatchOptions is used to set options when using batch operation
type BatchOptions struct {
	// In concurrent batch writes order determines how to handle conflicts
	Order int8
}

var wg sync.WaitGroup

// DefaultBatchOptions contains default options when writing batches to Tracedb key-value store.
var DefaultBatchOptions = &BatchOptions{
	Order: 0,
}

// Batch is a write batch.
type Batch struct {
	managed       bool
	grouped       bool
	order         int8
	seq           uint64
	db            *DB
	data          []byte
	index         []batchIndex
	pendingWrites []batchIndex
	firstKeyHash  uint32
	keys          []uint32

	// internalLen is sums of key/value pair length plus 8-bytes internal key.
	internalLen uint32
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

func (b *Batch) appendRec(dFlag bool, expiresAt uint32, key, value []byte) {
	n := 1 + len(key)
	if !dFlag {
		n += len(value)
	}
	b.grow(n)
	h := b.db.hash(key)
	index := batchIndex{delFlag: dFlag, hash: h, keySize: uint16(len(key))}
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
	if !dFlag {
		index.valueSize = uint32(len(value))
		o += copy(data[o:], value)
	}
	b.data = data[:o]
	index.expiresAt = expiresAt
	b.index = append(b.index, index)
	b.internalLen += uint32(index.keySize) + index.valueSize + 8
}

func (b *Batch) mput(dFlag bool, h uint32, expiresAt uint32, key, value []byte) error {
	switch {
	case len(key) == 0:
		return errKeyEmpty
	case len(key) > MaxKeyLength:
		return errKeyTooLarge
	case len(value) > MaxValueLength:
		return errValueTooLarge
	}
	if b.hasWriteConflict(h) {
		return errWriteConflict
	}
	var k []byte
	k = makeInternalKey(k, key, b.seq+1, dFlag, expiresAt)
	if err := b.db.mem.put(h, k, value, expiresAt); err != nil {
		return err
	}
	if float64(b.db.mem.count)/float64(b.db.mem.nBlocks*entriesPerBlock) > loadFactor {
		if err := b.db.mem.split(); err != nil {
			return err
		}
	}
	if b.firstKeyHash == 0 {
		b.firstKeyHash = h
	}
	b.seq++
	return nil
}

func (b *Batch) put(msg *Message) error {
	if msg.contract == 0 {
		msg.contract = Contract
	}

	topic := new(Topic)
	topic.Topic = msg.Topic
	// Parse the topic
	topic.Parse(msg.contract, false)
	if topic.TopicType == TopicInvalid {
		return errBadRequest
	}

	msg.setContract(topic)
	msg.setSsid(topic.Parts)
	return nil
}

// Put appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) Put(key, value []byte) error {
	return b.PutMessage(NewMessage(key, value))
}

// PutMessage appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) PutMessage(msg *Message) error {
	return b.put(msg)
}

// PutWithTTL appends 'put operation' of the given key/value pair to the batch and add key expiry time.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) PutWithTTL(key, value []byte, ttl time.Duration) {
	var expiresAt uint32
	if ttl != 0 {
		expiresAt = uint32(time.Now().Add(ttl).Unix())
	}
	b.appendRec(false, expiresAt, key, value)
}

// Delete appends 'delete operation' of the given key to the batch.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (b *Batch) Delete(key []byte) {
	var expiresAt uint32
	b.appendRec(true, expiresAt, key, nil)
}

func (b *Batch) hasWriteConflict(h uint32) bool {
	for _, batch := range b.db.activeBatches {
		for _, k := range batch {
			if k == h {
				return true
			}
		}
	}
	return false
}

func (b *Batch) writeInternal(fn func(i int, dFlag bool, h uint32, expiresAt uint32, k, v []byte) error) error {
	start := time.Now()
	defer logger.Debug().Str("context", "batch.writeInternal").Dur("duration", time.Since(start)).Msg("")
	for i, index := range b.pendingWrites {
		key, val := index.kv(b.data)
		if err := fn(i, index.delFlag, index.hash, index.expiresAt, key, val); err != nil {
			return err
		}
		logger.Debug().Str("context", "batch.writeInternal").Str("key", string(key)).Str("value", string(val))
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
	err := b.writeInternal(func(i int, dFlag bool, h uint32, expiresAt uint32, k, v []byte) error {
		return b.mput(dFlag, h, expiresAt, k, v)
	})

	if err == nil {
		b.db.activeBatches[b.seq] = b.Keys()
		b.db.mem.setSeq(b.seq)
	}

	return err
}

func (b *Batch) commit() error {
	if len(b.pendingWrites) == 0 {
		return nil
	}
	var delCount int64 = 0
	var putCount int64 = 0
	var bh *blockHandle
	var originalB *blockHandle
	entryIdx := 0
	logger.Debug().Str("context", "batch.commit").Int8("order", b.order).Int("length", b.Len())
	blockIdx := b.db.mem.blockIndex(b.firstKeyHash)
	for blockIdx < b.db.mem.nBlocks {
		err := b.db.mem.forEachBlock(blockIdx, func(memb blockHandle) (bool, error) {
			for i := 0; i < entriesPerBlock; i++ {
				memsl := memb.entries[i]
				if memsl.kvOffset == 0 {
					return memb.next == 0, nil
				}
				memslKey, value, err := b.db.mem.data.readKeyValue(memsl)
				if err == errKeyExpired {
					continue
				}
				if err != nil {
					return true, err
				}
				key, seq, dFlag, expiresAt, err := parseInternalKey(memslKey)
				if err != nil {
					return true, err
				}
				if seq <= b.seq-uint64(b.Len()) {
					continue
				}
				if seq > b.seq {
					return true, errBatchSeqComplete
				}
				hash := b.db.hash(key)

				if dFlag {
					/// Test filter block for presence
					if !b.db.filter.Test(uint64(hash)) {
						return false, nil
					}
					delCount++
					bh := blockHandle{}
					delentryIdx := -1
					err = b.db.forEachBlock(b.db.blockIndex(hash), func(curb blockHandle) (bool, error) {
						bh = curb
						for i := 0; i < entriesPerBlock; i++ {
							sl := bh.entries[i]
							if sl.kvOffset == 0 {
								return bh.next == 0, nil
							} else if hash == sl.hash && uint16(len(key)) == sl.keySize {
								slKey, err := b.db.data.readKey(sl)
								if err != nil {
									return true, err
								}
								if bytes.Equal(key, slKey) {
									delentryIdx = i
									return true, nil
								}
							}
						}
						return false, nil
					})
					if delentryIdx == -1 || err != nil {
						return false, err
					}
					sl := bh.entries[delentryIdx]
					bh.del(delentryIdx)
					if err := bh.write(); err != nil {
						return false, err
					}
					b.db.data.free(sl.kvSize(), sl.kvOffset)
					b.db.count--
				} else {
					putCount++
					err = b.db.forEachBlock(b.db.blockIndex(hash), func(curb blockHandle) (bool, error) {
						bh = &curb
						for i := 0; i < entriesPerBlock; i++ {
							sl := bh.entries[i]
							entryIdx = i
							if sl.kvOffset == 0 {
								// Found an empty entry.
								return true, nil
							} else if hash == sl.hash && uint16(len(key)) == sl.keySize {
								// Key already exists.
								if slKey, err := b.db.data.readKey(sl); bytes.Equal(key, slKey) || err != nil {
									return true, err
								}
							}
						}
						if bh.next == 0 {
							// Couldn't find free space in the current blockHandle, creating a new overflow blockHandle.
							nextBlock, err := b.db.createOverflowBlock()
							if err != nil {
								return false, err
							}
							bh.next = nextBlock.offset
							originalB = bh
							bh = nextBlock
							entryIdx = 0
							return true, nil
						}
						return false, nil
					})

					if err != nil {
						return false, err
					}
					// Inserting a new item.
					if bh.entries[entryIdx].kvOffset == 0 {
						if b.db.count == MaxKeys {
							return false, errFull
						}
						b.db.count++
					} else {
						defer b.db.data.free(bh.entries[entryIdx].kvSize(), bh.entries[entryIdx].kvOffset)
					}

					bh.entries[entryIdx] = entry{
						hash:      hash,
						keySize:   uint16(len(key)),
						valueSize: uint32(len(value)),
						expiresAt: expiresAt,
					}
					if bh.entries[entryIdx].kvOffset, err = b.db.data.writeKeyValue(key, value); err != nil {
						return false, err
					}
					if err := bh.write(); err != nil {
						return false, err
					}
					if originalB != nil {
						if err := originalB.write(); err != nil {
							return false, err
						}
					}
					b.db.filter.Append(uint64(hash))
				}
			}
			return false, nil
		})
		if err == errBatchSeqComplete {
			break
		}
		if err != nil {
			logger.Err(err).Str("context", "batch.commit").Int8("order", b.order).Int("Length", b.Len())
			return err
		}
		blockIdx++
	}

	//remove batch from activeBatches after commit
	delete(b.db.activeBatches, b.seq)

	b.db.metrics.Dels.Add(delCount)
	b.db.metrics.Puts.Add(putCount)

	if b.db.syncWrites {
		return b.db.sync()
	}

	return nil
}

func (b *Batch) Commit() error {
	_assert(!b.managed, "managed tx commit not allowed")
	if b.db.mem == nil || b.db.mem.getref() == 0 {
		return nil
	}
	return b.commit()
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
	b.internalLen = 0
}

func (b *Batch) uniq() []batchIndex {
	type indices struct {
		idx    int
		newidx int
	}
	unique_set := make(map[uint32]indices, len(b.index))
	i := 0
	for idx := len(b.index) - 1; idx >= 0; idx-- {
		if _, ok := unique_set[b.index[idx].hash]; !ok {
			unique_set[b.index[idx].hash] = indices{idx, i}
			i++
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
		idx.kvOffset = idx.kvOffset + off
		b.index = append(b.index, idx)
	}
	//b.grow(len(bnew.data))
	b.data = append(b.data, bnew.data...)
	b.internalLen += bnew.internalLen
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

// Len returns number of records in the batch.
func (b *Batch) Keys() []uint32 {
	return b.keys
}

// Len returns number of records in the batch.
func (b *Batch) Len() int {
	return len(b.pendingWrites)
}

// Len returns number of records in the batch.
func (b *Batch) setManaged() {
	b.managed = true
}

// Len returns number of records in the batch.
func (b *Batch) unsetManaged() {
	b.managed = false
}

// Len returns number of records in the batch.
func (b *Batch) setGrouped(g *BatchGroup) {
	// b.batchGroup = g
	b.grouped = true
}

// Len returns number of records in the batch.
func (b *Batch) unsetGrouped() {
	b.grouped = false
}

func (b *Batch) setOrder(order int8) {
	b.order = order
}
