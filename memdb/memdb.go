package memdb

import (
	"bytes"
	"errors"
	"io"
	"log"
	"math"
	"sync"
	"sync/atomic"

	"github.com/saffat-in/tracedb/fs"
	"github.com/saffat-in/tracedb/hash"
)

const (
	keySize         = 16
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
	MaxTableSize = 1 << 30
)

type dbInfo struct {
	level      uint8
	count      uint32
	nBlocks    uint32
	blockIndex uint32
	hashSeed   uint32
}

// DB represents the topic->key-value storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	// Need 64-bit alignment.
	seq   uint64
	mu    sync.RWMutex
	index tableManager
	data  dataTable
	lock  fs.LockFile
	// Free blocks
	fb freeblocks
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
		seed, err := hash.RandSeed()
		if err != nil {
			return nil, err
		}
		db.hashSeed = seed
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

func startBlockIndex(off int64) uint32 {
	return uint32((off - int64(headerSize)) / int64(blockSize))
}

func (db *DB) forEachBlock(startBlockIdx uint32, cb func(blockHandle) (bool, error)) error {
	off := blockOffset(startBlockIdx)
	t := db.index
	for {
		b := blockHandle{table: t, offset: off}
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

func (db *DB) hash(data []byte) uint32 {
	return hash.WithSalt(data, db.hashSeed)
}

// newBlock adds new block to db table and return block offset
func (db *DB) newBlock() int64 {
	db.index.extend(blockSize)
	db.nBlocks++
	db.blockIndex++
	log.Println("memdb.newBlock: blockIndex,, nBlocks ", db.blockIndex, db.nBlocks)
	return blockOffset(db.blockIndex)
}

// Put sets the value for the given topic->key. It updates the value for the existing key.
func (db *DB) Put(topic []byte, key, value []byte, expiresAt uint32) (blockOff, kvOff int64, err error) {
	switch {
	case len(key) == 0:
		return blockOff, kvOff, errors.New("key is empty")
	case len(key) > MaxKeyLength:
		return blockOff, kvOff, errors.New("key is too large")
	case len(value) > MaxValueLength:
		return blockOff, kvOff, errors.New("value is too large")
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	blockOff, kvOff, err = db.put(topic, key, value, expiresAt)
	if uint32(db.count/entriesPerBlock) > db.blockIndex {
		db.newBlock()
	}
	return blockOff, kvOff, err
}

func (db *DB) put(topic, key, value []byte, expiresAt uint32) (blockOff, kvOff int64, err error) {
	var b *blockHandle
	ew := entryWriter{
		block: &blockHandle{table: db.index, offset: blockOffset(db.blockIndex)},
	}
	entryIdx := 0
	keyHash := db.hash(key)
	startBlockIdx := db.blockIndex
	// search if any free block available
	if ok, idx := db.fb.get(false); ok {
		startBlockIdx = idx
	}
	err = db.forEachBlock(startBlockIdx, func(curb blockHandle) (bool, error) {
		b = &curb
		for i := 0; i < entriesPerBlock; i++ {
			e := b.entries[i]
			entryIdx = i
			if e.kvOffset == 0 {
				// Found an empty entry.
				return true, nil
			} else if keyHash == e.hash {
				// Key already exists.
				if eKey, err := db.data.readKey(e); bytes.Equal(key, eKey) || err != nil {
					return true, err
				}
			}
		}
		return false, nil
	})
	if err != nil {
		return blockOff, kvOff, err
	}

	// Inserting a new item.
	if db.count == MaxKeys {
		return blockOff, kvOff, errors.New("database is full")
	}
	db.count++

	ew.entryIdx = entryIdx
	ew.entry = entry{
		hash:      keyHash,
		topicSize: uint16(len(topic)),
		valueSize: uint32(len(value)),
		expiresAt: expiresAt,
	}
	ew.block = b
	if kvOff, err = db.data.writeKeyValue(topic, key, value); err != nil {
		return blockOff, kvOff, err
	}
	ew.entry.kvOffset = kvOff
	if err := ew.write(); err != nil {
		return blockOff, kvOff, err
	}
	return b.offset, kvOff, nil
}

// Cleanup addds block range into freeblocks list
func (db *DB) Cleanup(startSeq, endSeq uint64) (ok bool) {
	if startSeq >= endSeq {
		return false
	}
	for blockIdx, endBloackIdx := uint32(startSeq/entriesPerBlock), uint32(endSeq/entriesPerBlock); blockIdx <= endBloackIdx; blockIdx++ {
		if ok := db.fb.free(blockIdx, false); !ok {
			return false
		}
	}
	db.count -= uint32(endSeq - startSeq)
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
	return atomic.LoadUint64(&db.seq)
}

// Atomically adds delta to seq.
func (db *DB) AddSeq(delta uint64) {
	atomic.AddUint64(&db.seq, delta)
}

func (db *DB) SetSeq(seq uint64) {
	atomic.StoreUint64(&db.seq, seq)
}
