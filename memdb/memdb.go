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
	keySize         = 28
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
	dbInfo
	// Close.
	closed uint32
	closer io.Closer
}

// Open opens or creates a new DB.
func Open(path string) (*DB, error) {
	index, err := mem.newTable(path+indexPostfix, MaxTableSize)
	if err != nil {
		return nil, err
	}
	data, err := mem.newTable(path, MaxTableSize)
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
		if err = db.index.extend(headerSize + blockSize); err != nil {
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

// Items returns a new ItemIterator.
func (db *DB) Items(startSeq, endSeq uint64) *ItemIterator {
	if startSeq >= endSeq || endSeq > db.seq {
		return nil
	}
	return &ItemIterator{db: db, startSeq: startSeq, endSeq: endSeq}
}

func (db *DB) hash(data []byte) uint32 {
	return hash.WithSalt(data, db.hashSeed)
}

// Put sets the value for the given topic->key. It updates the value for the existing key.
func (db *DB) Put(topic []byte, seq uint64, dFlag bool, key, value []byte, expiresAt uint32) error {
	switch {
	case len(key) == 0:
		return errors.New("key is empty")
	case len(key) > MaxKeyLength:
		return errors.New("key is too large")
	case len(value) > MaxValueLength:
		return errors.New("value is too large")
	}
	hash := db.hash(key)
	db.mu.Lock()
	defer db.mu.Unlock()
	var ikey []byte
	ikey = makeInternalKey(ikey, key, seq, dFlag, expiresAt)
	if uint32(db.count/entriesPerBlock) > db.blockIndex {
		db.index.extend(blockSize)
		db.blockIndex++
		db.nBlocks++
	}
	err := db.put(hash, topic, ikey, value, expiresAt)
	return err
}

func (db *DB) put(keyHash uint32, topic, key, value []byte, expiresAt uint32) error {
	var b *blockHandle
	ew := entryWriter{
		block: &blockHandle{table: db.index, offset: blockOffset(db.blockIndex)},
	}
	entryIdx := 0
	err := db.forEachBlock(db.blockIndex, func(curb blockHandle) (bool, error) {
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
		return err
	}

	// Inserting a new item.
	if db.count == MaxKeys {
		return errors.New("database is full")
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
	if ew.entry.kvOffset, err = db.data.writeKeyValue(topic, key, value); err != nil {
		return err
	}
	if err := ew.write(); err != nil {
		return err
	}
	return nil
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
