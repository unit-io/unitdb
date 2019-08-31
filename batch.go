package tracedb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
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
	delFlag            bool
	expiresAt          uint32
	keyPos, keyLen     int
	valuePos, valueLen int
}

func (index batchIndex) k(data []byte) []byte {
	return data[index.keyPos : index.keyPos+index.keyLen]
}

func (index batchIndex) v(data []byte) []byte {
	if index.valueLen != 0 {
		return data[index.valuePos : index.valuePos+index.valueLen]
	}
	return nil
}

func (index batchIndex) kv(data []byte) (key, value []byte) {
	return index.k(data), index.v(data)
}

type internalKey []byte

func makeInternalKey(dst, ukey []byte, seq uint64, dFlag bool, expiresAt uint32) internalKey {
	if seq > keyMaxSeq {
		panic("tracedb: invalid sequence number")
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
		logger.Print("invalid internal key length")
		return
	}
	expiresAt = binary.LittleEndian.Uint32(ik[len(ik)-4:])
	num := binary.LittleEndian.Uint64(ik[len(ik)-12 : len(ik)-4])
	seq, dFlag = uint64(num>>8), num&0xff != 0
	ukey = ik[:len(ik)-12]
	return
}

// Batch is a write batch.
type Batch struct {
	managed bool
	db      *DB
	data    []byte
	index   []batchIndex

	//Batch memdb
	memMu sync.RWMutex
	mem   *memdb

	// internalLen is sums of key/value pair length plus 8-bytes internal key.
	internalLen int
}

// init initializes the batch.
func (b *Batch) init(db *DB) {
	b.db = db
	if b.db.mem == nil || b.db.mem.getref() == 0 {
		b.db.newmemdb(0)
	}
	b.mem = b.db.mem
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
	n := 1 + binary.MaxVarintLen32 + len(key)
	if !dFlag {
		n += binary.MaxVarintLen32 + len(value)
	}
	b.grow(n)
	index := batchIndex{delFlag: dFlag}
	o := len(b.data)
	data := b.data[:o+n]
	if dFlag {
		data[o] = 1
	} else {
		data[o] = 0
	}
	o++
	o += binary.PutUvarint(data[o:], uint64(len(key)))
	index.keyPos = o
	index.keyLen = len(key)
	o += copy(data[o:], key)
	if !dFlag {
		o += binary.PutUvarint(data[o:], uint64(len(value)))
		index.valuePos = o
		index.valueLen = len(value)
		o += copy(data[o:], value)
	}
	index.expiresAt = expiresAt
	b.data = data[:o]
	b.index = append(b.index, index)
	b.internalLen += index.keyLen + index.valueLen + 8
}

func (b *Batch) mput(dFlag bool, expiresAt uint32, seq uint64, key, value []byte) error {
	switch {
	case len(key) == 0:
		return errKeyEmpty
	case len(key) > MaxKeyLength:
		return errKeyTooLarge
	case len(value) > MaxValueLength:
		return errValueTooLarge
	}
	h := b.mem.hash(key)
	var k []byte
	k = makeInternalKey(k, key, seq, dFlag, expiresAt)
	// b.memMu.Lock()
	// defer b.memMu.Unlock()
	if err := b.mem.put(h, k, value, expiresAt); err != nil {
		return err
	}
	if float64(b.mem.count)/float64(b.mem.nBuckets*entriesPerBucket) > loadFactor {
		if err := b.mem.split(); err != nil {
			return err
		}
	}
	b.mem.seq++
	return nil
}

// Put appends 'put operation' of the given key/value pair to the batch.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (b *Batch) Put(key, value []byte) {
	b.PutWithTTL(key, value, 0)
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

func (b *Batch) writeInternal(fn func(i int, dFlag bool, expiresAt uint32, k, v []byte) error) error {
	for i, index := range b.index {
		if err := fn(i, index.delFlag, index.expiresAt, index.k(b.data), index.v(b.data)); err != nil {
			return err
		}
	}
	return nil
}

// Write apply the given batch to the transaction. The batch will be applied
// sequentially.
// Please note that the transaction is not compacted until committed, so if you
// writes 10 same keys, then those 10 same keys are in the transaction.
//
// It is safe to modify the contents of the arguments after Write returns.
func (b *Batch) Write() error {
	return b.writeInternal(func(i int, dFlag bool, expiresAt uint32, k, v []byte) error {
		return b.mput(dFlag, expiresAt, b.mem.seq+uint64(i), k, v)
	})
}

func (b *Batch) commit() error {
	var delCount int64 = 0
	var putCount int64 = 0
	var bh *bucketHandle
	var originalB *bucketHandle
	var bucketIdx uint32
	entryIdx := 0
	b.mem.mu.RLock()
	b.db.mu.Lock()
	defer func() {
		b.mem.mu.RUnlock()
		b.db.mu.Unlock()
	}()

	for bucketIdx < b.mem.nBuckets {
		err := b.mem.forEachBucket(bucketIdx, func(memb bucketHandle) (bool, error) {
			for i := 0; i < entriesPerBucket; i++ {
				memsl := memb.entries[i]
				if memsl.kvOffset == 0 {
					return memb.next == 0, nil
				}
				memslKey, value, err := b.mem.data.readKeyValue(memsl)
				if err == ErrKeyExpired {
					return false, nil
				}
				if err != nil {
					return true, err
				}
				key, _, dFlag, expiresAt, err := parseInternalKey(memslKey)
				if err != nil {
					return true, err
				}

				hash := b.db.hash(key)

				if dFlag {
					/// Test filter block for presence
					if !b.db.filter.Test(uint64(hash)) {
						return false, nil
					}
					delCount++
					bh := bucketHandle{}
					delentryIdx := -1
					err = b.db.forEachBucket(b.db.bucketIndex(hash), func(curb bucketHandle) (bool, error) {
						bh = curb
						for i := 0; i < entriesPerBucket; i++ {
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
					err = b.db.forEachBucket(b.db.bucketIndex(hash), func(curb bucketHandle) (bool, error) {
						bh = &curb
						for i := 0; i < entriesPerBucket; i++ {
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
							// Couldn't find free space in the current bucketHandle, creating a new overflow bucketHandle.
							nextBucket, err := b.db.createOverflowBucket()
							if err != nil {
								return false, err
							}
							bh.next = nextBucket.offset
							originalB = bh
							bh = nextBucket
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
		if err != nil {
			return err
		}
		bucketIdx++
	}
	b.db.metrics.Dels.Add(delCount)
	b.db.metrics.Puts.Add(putCount)

	if b.db.syncWrites {
		return b.db.sync()
	}

	return nil
}

func (b *Batch) Commit() error {
	_assert(!b.managed, "managed tx commit not allowed")
	if b.mem == nil || b.mem.getref() == 0 {
		return nil
	}
	return b.commit()
}

func (b *Batch) Abort() {
	_assert(!b.managed, "managed tx commit not allowed")
	b.Reset()
	b.mem = nil
}

// Dump dumps batch contents. The returned slice can be loaded into the
// batch using Load method.
// The returned slice is not its own copy, so the contents should not be
// modified.
func (b *Batch) Dump() []byte {
	return b.data
}

// Load loads given slice into the batch. Previous contents of the batch
// will be discarded.
// The given slice will not be copied and will be used as batch buffer, so
// it is not safe to modify the contents of the slice.
func (b *Batch) Load(data []byte) error {
	return b.decode(data, -1)
}

// Len returns number of records in the batch.
func (b *Batch) Len() int {
	return len(b.index)
}

// Reset resets the batch.
func (b *Batch) Reset() {
	b.data = b.data[:0]
	b.index = b.index[:0]
	b.internalLen = 0
}

func (b *Batch) append(p *Batch) {
	ob := len(b.data)
	oi := len(b.index)
	b.data = append(b.data, p.data...)
	b.index = append(b.index, p.index...)
	b.internalLen += p.internalLen

	// Updating index offset.
	if ob != 0 {
		for ; oi < len(b.index); oi++ {
			index := &b.index[oi]
			index.keyPos += ob
			if index.valueLen != 0 {
				index.valuePos += ob
			}
		}
	}
}

func (b *Batch) decode(data []byte, expectedLen int) error {
	b.data = data
	b.index = b.index[:0]
	b.internalLen = 0
	err := decodeBatch(data, func(i int, index batchIndex) error {
		b.index = append(b.index, index)
		b.internalLen += index.keyLen + index.valueLen + 8
		return nil
	})
	if err != nil {
		return err
	}
	if expectedLen >= 0 && len(b.index) != expectedLen {
		logger.Print("invalid records length: %d vs %d", expectedLen, len(b.index))
	}
	return nil
}

func (b *Batch) put(seq uint64, mdb *memdb) error {
	var ik []byte
	for i, index := range b.index {
		ik = makeInternalKey(ik, index.k(b.data), seq+uint64(i), index.delFlag, index.expiresAt)
		if err := mdb.Put(ik, index.v(b.data)); err != nil {
			return err
		}
	}
	return nil
}

func (b *Batch) revert(seq uint64, mdb *memdb) error {
	var ik []byte
	for i, index := range b.index {
		ik = makeInternalKey(ik, index.k(b.data), seq+uint64(i), index.delFlag, index.expiresAt)
		if err := mdb.Delete(ik); err != nil {
			return err
		}
	}
	return nil
}

func newBatch() interface{} {
	return &Batch{}
}

func decodeBatch(data []byte, fn func(i int, index batchIndex) error) error {
	var index batchIndex
	for i, o := 0, 0; o < len(data); i++ {
		// Key type.
		index.delFlag = data[o] != 0
		// if index.keyType > keyTypeVal {
		// 	return newErrBatchCorrupted(fmt.Sprintf("bad record: invalid type %#x", uint(index.keyType)))
		// }
		o++

		// Key.
		x, n := binary.Uvarint(data[o:])
		o += n
		if n <= 0 || o+int(x) > len(data) {
			logger.Print("bad record: invalid key length")
			return nil
		}
		index.keyPos = o
		index.keyLen = int(x)
		o += index.keyLen

		// Value.
		if !index.delFlag {
			x, n = binary.Uvarint(data[o:])
			o += n
			if n <= 0 || o+int(x) > len(data) {
				logger.Print("bad record: invalid value length")
				return nil
			}
			index.valuePos = o
			index.valueLen = int(x)
			o += index.valueLen
		} else {
			index.valuePos = 0
			index.valueLen = 0
		}

		if err := fn(i, index); err != nil {
			return err
		}
	}
	return nil
}

func decodeBatchToDB(data []byte, expectSeq uint64, mdb *memdb) (seq uint64, batchLen int, err error) {
	seq, batchLen, err = decodeBatchHeader(data)
	if err != nil {
		return 0, 0, err
	}
	if seq < expectSeq {
		logger.Print("invalid sequence number")
		return 0, 0, nil
	}
	data = data[batchHeaderLen:]
	var ik []byte
	var decodedLen int
	err = decodeBatch(data, func(i int, index batchIndex) error {
		if i >= batchLen {
			logger.Print("invalid records length")
			return nil
		}
		ik = makeInternalKey(ik, index.k(data), seq+uint64(i), index.delFlag, index.expiresAt)
		if err := mdb.Put(ik, index.v(data)); err != nil {
			return err
		}
		decodedLen++
		return nil
	})
	if err == nil && decodedLen != batchLen {
		logger.Print("invalid records length: %d vs %d", batchLen, decodedLen)
	}
	return
}

func encodeBatchHeader(dst []byte, seq uint64, batchLen int) []byte {
	dst = ensureBuffer(dst, batchHeaderLen)
	binary.LittleEndian.PutUint64(dst, seq)
	binary.LittleEndian.PutUint32(dst[8:], uint32(batchLen))
	return dst
}

func decodeBatchHeader(data []byte) (seq uint64, batchLen int, err error) {
	if len(data) < batchHeaderLen {
		logger.Print("too short")
		return 0, 0, nil
	}

	seq = binary.LittleEndian.Uint64(data)
	batchLen = int(binary.LittleEndian.Uint32(data[8:]))
	if batchLen < 0 {
		logger.Print("invalid records length")
		return 0, 0, nil
	}
	return
}

func batchesLen(batches []*Batch) int {
	batchLen := 0
	for _, batch := range batches {
		batchLen += batch.Len()
	}
	return batchLen
}

func writeBatchesWithHeader(wr io.Writer, batches []*Batch, seq uint64) error {
	if _, err := wr.Write(encodeBatchHeader(nil, seq, batchesLen(batches))); err != nil {
		return err
	}
	for _, batch := range batches {
		if _, err := wr.Write(batch.data); err != nil {
			return err
		}
	}
	return nil
}

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}
