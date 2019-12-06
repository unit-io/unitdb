package memdb

import (
	"bytes"
	"errors"
	"io"
	"log"
	"math"
	"sync"
	"sync/atomic"
)

const (
	idSize          = 20
	entriesPerBlock = 23
	indexPostfix    = ".index"
	version         = 1 // file format version

	// MaxKeyLength is the maximum size of a key in bytes.
	MaxKeyLength = 1 << 16

	// MaxValueLength is the maximum size of a value in bytes.
	MaxValueLength = 1 << 30

	// MaxKeys is the maximum numbers of keys in the DB.
	MaxKeys = math.MaxUint32

	// Maximum value possible for sequence number; the 8-bits are
	// used by value type, so its can packed together in single
	// 64-bit integer.
	keyMaxSeq = (uint64(1) << 56) - 1
	// Maximum value possible for packed sequence number and type.
	keyMaxNum = (keyMaxSeq << 8) | 0

	// MaxTableSize value for maximum memroy use for the memdb.
	MaxTableSize = (int64(1) << 30) - 1
)

type dbInfo struct {
	seq        uint64
	count      uint32
	nBlocks    uint32
	blockIndex uint32
}

// DB represents the topic->key-value storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	// Need 64-bit alignment.
	mu      sync.RWMutex
	index   tableManager
	data    dataTable
	freeseq freesequence
	dbInfo
	// Close.
	closed uint32
	closer io.Closer
}

// Open opens or creates a new DB. Minimum memroy size is 1GB
func Open(path string, memSize int64) (*DB, error) {
	if memSize < 1<<30 {
		memSize = MaxTableSize
	}
	index, err := mem.newTable(path+indexPostfix, memSize)
	if err != nil {
		return nil, err
	}
	data, err := mem.newTable(path, memSize)
	if err != nil {
		return nil, err
	}
	db := &DB{
		index: index,
		data:  dataTable{tableManager: data},
		dbInfo: dbInfo{
			nBlocks: 1,
		},
	}

	if index.size() == 0 {
		if data.size() != 0 {
			if err := index.close(); err != nil {
				log.Print(err)
			}
			if err := mem.remove(index.name()); err != nil {
				log.Print(err)
			}
			if err := data.close(); err != nil {
				log.Print(err)
			}
			if err := mem.remove(data.name()); err != nil {
				log.Print(err)
			}
			// Data file exists, but index is missing.
			return nil, errors.New("database is corrupted")
		}
		if _, err = db.index.extend(headerSize + blockSize); err != nil {
			return nil, err
		}
		if _, err = db.data.extend(headerSize); err != nil {
			return nil, err
		}
		if err := db.writeHeader(); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func blockOffset(idx uint32) int64 {
	return int64(headerSize) + (int64(blockSize) * int64(idx))
}

func startBlockIndex(seq uint64) uint32 {
	return uint32(float64(seq-1) / float64(entriesPerBlock))
}

func (db *DB) writeHeader() error {
	h := header{
		signature: signature,
		version:   version,
	}
	buf, err := h.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = db.index.writeAt(buf, 0)
	return err
}

// Close closes the DB.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.index.close(); err != nil {
		return err
	}
	if err := mem.remove(db.index.name()); err != nil {
		return err
	}
	if err := db.data.close(); err != nil {
		return err
	}
	if err := mem.remove(db.data.name()); err != nil {
		return err
	}

	var err error
	if db.closer != nil {
		if err1 := db.closer.Close(); err == nil {
			err = err1
		}
		db.closer = nil
	}

	return err
}

func (db *DB) forEachBlock(startBlockIdx uint32, cb func(blockHandle) (bool, error)) error {
	off := blockOffset(startBlockIdx)
	for {
		b := blockHandle{table: db.index, offset: off}
		if err := b.read(); err != nil {
			return err
		}
		if stop, err := cb(b); stop || err != nil {
			return err
		}
		if b.next == 0 {
			return nil
		}
	}
}

// newBlock adds new block to db table and return block offset
func (db *DB) newBlock() (int64, error) {
	off, err := db.index.extend(blockSize)
	db.nBlocks++
	db.blockIndex++
	log.Println("memdb.newBlock: blockIndex, nBlocks ", db.blockIndex, db.nBlocks)
	return off, err
}

// Put sets the value for the given topic->key. It updates the value for the existing key.
func (db *DB) Put(hash uint32, id, topic, value []byte, expiresAt uint32) error {
	switch {
	case len(id) == 0:
		return errors.New("id is empty")
	case len(id) > MaxKeyLength:
		return errors.New("id is too large")
	case len(value) > MaxValueLength:
		return errors.New("value is too large")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	err := db.put(hash, id, topic, value, expiresAt)
	return err
}

func (db *DB) put(hash uint32, id, topic, value []byte, expiresAt uint32) (err error) {
	var b *blockHandle
	seq := db.nextSeq()
	startBlockIdx := startBlockIndex(seq)
	err = db.forEachBlock(startBlockIdx, func(curb blockHandle) (bool, error) {
		b = &curb
		if startBlockIdx == db.blockIndex && b.entryIdx == entriesPerBlock-1 {
			db.newBlock()
		}
		for i := 0; i < entriesPerBlock; i++ {
			e := b.entries[i]
			if e.mOffset == 0 {
				// Found an empty entry.
				return true, nil
			} else if hash == e.hash {
				// Key already exists.
				if _id, err := db.data.readId(e); bytes.Equal(id, _id) || err != nil {
					return true, err
				}
			}
		}
		return false, nil
	})
	if err != nil {
		db.freeseq.free(seq)
		return err
	}
	db.count++

	ew := entryWriter{
		block: b,
	}
	ew.entry = entry{
		seq:       seq,
		hash:      hash,
		topicSize: uint16(len(topic)),
		valueSize: uint32(len(value)),
		expiresAt: expiresAt,
	}
	if ew.entry.mOffset, err = db.data.writeMessage(id, topic, value); err != nil {
		db.freeseq.free(seq)
		return err
	}
	if err := ew.write(); err != nil {
		db.freeseq.free(seq)
		return err
	}
	return err
}

// delete deletes the given key from the DB.
func (db *DB) delete(seq uint64) error {
	b := blockHandle{}
	entryIdx := -1
	startBlockIdx := startBlockIndex(seq)
	err := db.forEachBlock(startBlockIdx, func(curb blockHandle) (bool, error) {
		b = curb
		for i := 0; i < entriesPerBlock; i++ {
			e := b.entries[i]
			if seq == e.seq {
				entryIdx = i
				db.data.free(e.mSize(), e.mOffset)
				return true, nil
			}
		}
		return false, nil
	})
	if entryIdx == -1 || err != nil {
		return err
	}
	b.entryIdx = uint16(entryIdx)
	ew := entryWriter{
		block: &b,
	}
	if err := ew.write(); err != nil {
		return err
	}
	db.freeseq.free(seq)
	db.count--
	return nil
}

// Cleanup addds block range into freeblocks list
func (db *DB) Cleanup(startSeq, endSeq uint64) (ok bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if startSeq >= endSeq && endSeq >= db.GetSeq() {
		return false
	}
	for seq := startSeq; seq <= endSeq; seq++ {
		db.delete(seq)
	}
	return true
}

// Count returns the number of items in the DB.
func (db *DB) Count() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.count
}

// FileSize returns the total size of the disk storage used by the DB.
func (db *DB) FileSize() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.index.size() + db.data.size, nil
}

// Get latest sequence number.
func (db *DB) GetSeq() uint64 {
	if ok, seq := db.freeseq.get(); ok {
		return seq
	}
	return atomic.AddUint64(&db.seq, 1)
}

func (db *DB) nextSeq() uint64 {
	if ok, seq := db.freeseq.get(); ok {
		return seq
	}
	seq := atomic.LoadUint64(&db.seq)
	atomic.AddUint64(&db.seq, 1)
	return seq
}
