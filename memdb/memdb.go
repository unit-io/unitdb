package memdb

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log"
	"math"
	"os"
	"sync"

	"github.com/saffat-in/tracedb/fs"
	"github.com/saffat-in/tracedb/hash"
)

const (
	keySize         = 28
	entriesPerBlock = 22
	loadFactor      = 0.7
	indexPostfix    = ".index"
	lockPostfix     = ".lock"
	filterPostfix   = ".filter"
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
)

type dbInfo struct {
	level         uint8
	count         uint32
	nBlocks       uint32
	splitBlockIdx uint32
	freelistOff   int64
	hashSeed      uint32
}

// DB represents the key-value storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	// Need 64-bit alignment.
	Seq          uint64
	mu           sync.RWMutex
	writeLockC   chan struct{}
	index        file
	data         dataFile
	lock         fs.LockFile
	cancelSyncer context.CancelFunc
	syncWrites   bool
	dbInfo
	// Close.
	closeW sync.WaitGroup
	closeC chan struct{}
	closed uint32
	closer io.Closer
}

// Open opens or creates a new DB.
func Open(path string, opts *Options) (*DB, error) {
	opts = opts.CopyWithDefaults()
	fileFlag := os.O_CREATE | os.O_RDWR
	fsys := opts.FileSystem
	fileMode := os.FileMode(0666)
	lock, needsRecovery, err := fsys.CreateLockFile(path+lockPostfix, fileMode)
	if err != nil {
		return nil, err
	}

	index, err := openFile(fsys, path+indexPostfix, fileFlag, fileMode)
	if err != nil {
		return nil, err
	}
	data, err := openFile(fsys, path, fileFlag, fileMode)
	if err != nil {
		return nil, err
	}
	db := &DB{
		index:      index,
		data:       dataFile{file: data},
		lock:       lock,
		writeLockC: make(chan struct{}, 1),
		dbInfo: dbInfo{
			nBlocks:     1,
			freelistOff: -1,
		},
		// Close
		closeC: make(chan struct{}),
	}

	if index.size == 0 {
		if data.size != 0 {
			if err := index.Close(); err != nil {
				log.Print(err)
			}
			if err := data.Close(); err != nil {
				log.Print(err)
			}
			if err := lock.Unlock(); err != nil {
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
	} else {
		if err := db.readHeader(!needsRecovery); err != nil {
			if err := index.Close(); err != nil {
				log.Print(err)
			}
			if err := data.Close(); err != nil {
				log.Print(err)
			}
			if err := lock.Unlock(); err != nil {
				log.Print(err)
			}
			return nil, err
		}
	}
	if needsRecovery {
		if err := db.recover(); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func blockOffset(idx uint32) int64 {
	return int64(headerSize) + (int64(blockSize) * int64(idx))
}

func (db *DB) forEachBlock(startBlockIdx uint32, fillCache bool, cb func(blockHandle) (bool, error)) error {
	off := blockOffset(startBlockIdx)
	f := db.index.FileManager
	for {
		b := blockHandle{file: f, offset: off}
		if err := b.read(fillCache); err != nil {
			return err
		}
		if stop, err := cb(b); stop || err != nil {
			return err
		}
		if b.next == 0 {
			return nil
		}
		off = b.next
		f = db.data.FileManager
	}
}

func (db *DB) createOverflowBlock() (*blockHandle, error) {
	off, err := db.data.allocate(blockSize)
	if err != nil {
		return nil, err
	}
	return &blockHandle{file: db.data, offset: off}, nil
}

func (db *DB) writeHeader() error {
	db.data.fl.defrag()
	freelistOff, err := db.data.fl.write(db.data.file)
	if err != nil {
		return err
	}
	db.dbInfo.freelistOff = freelistOff
	h := header{
		signature: signature,
		version:   version,
		dbInfo:    db.dbInfo,
	}
	return db.index.writeMarshalableAt(h, 0)
}

func (db *DB) readHeader(readFreeList bool) error {
	h := &header{}
	if err := db.index.readUnmarshalableAt(h, headerSize, 0); err != nil {
		return err
	}
	// if !bytes.Equal(h.signature[:], signature[:]) {
	// 	return errCorrupted
	// }
	db.dbInfo = h.dbInfo
	if readFreeList {
		if err := db.data.fl.read(db.data.file, db.dbInfo.freelistOff); err != nil {
			return err
		}
	}
	db.dbInfo.freelistOff = -1
	return nil
}

// Close closes the DB.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Wait for all gorotines to exit.
	db.closeW.Wait()

	if db.cancelSyncer != nil {
		db.cancelSyncer()
	}
	if err := db.writeHeader(); err != nil {
		return err
	}
	if err := db.data.Close(); err != nil {
		return err
	}
	if err := db.index.Close(); err != nil {
		return err
	}
	if err := db.lock.Unlock(); err != nil {
		return err
	}

	var err error
	// Signal all goroutines.
	close(db.closeC)

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
	return &ItemIterator{db: db, startSeq: startSeq, endSeq: endSeq}
}

func (db *DB) blockIndex(hash uint32) uint32 {
	idx := hash & ((1 << db.level) - 1)
	if idx < db.splitBlockIdx {
		return hash & ((1 << (db.level + 1)) - 1)
	}
	return idx
}

func (db *DB) hash(data []byte) uint32 {
	return hash.WithSalt(data, db.hashSeed)
}

// Get returns the value for the given key stored in the DB or nil if the key doesn't exist.
func (db *DB) Get(key []byte) ([]byte, error) {
	h := db.hash(key)
	db.mu.RLock()
	defer db.mu.RUnlock()
	var retValue []byte

	err := db.forEachBlock(db.blockIndex(h), true, func(b blockHandle) (bool, error) {
		for i := 0; i < entriesPerBlock; i++ {
			e := b.entries[i]
			if e.kvOffset == 0 {
				return b.next == 0, nil
			} else if h == e.hash {
				eKey, value, err := db.data.readKeyValue(e)
				if err != nil {
					return true, err
				}
				if bytes.Equal(key, eKey) {
					retValue = value
					return true, nil
				}
			}
		}
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return retValue, nil
}

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
	err := db.put(hash, topic, ikey, value, expiresAt)
	if float64(db.count)/float64(db.nBlocks*entriesPerBlock) > loadFactor {
		if err := db.split(); err != nil {
			return err
		}
	}
	return err
}

func (db *DB) put(hash uint32, topic, key, value []byte, expiresAt uint32) error {
	var b *blockHandle
	var originalB *blockHandle
	entryIdx := 0
	err := db.forEachBlock(db.blockIndex(hash), false, func(curb blockHandle) (bool, error) {
		b = &curb
		for i := 0; i < entriesPerBlock; i++ {
			e := b.entries[i]
			entryIdx = i
			if e.kvOffset == 0 {
				// Found an empty entry.
				return true, nil
			} else if hash == e.hash {
				// Key already exists.
				if eKey, err := db.data.readKey(e); bytes.Equal(key, eKey) || err != nil {
					return true, err
				}
			}
		}
		if b.next == 0 {
			// Couldn't find free space in the current blockHandle, creating a new overflow blockHandle.
			nextBlock, err := db.createOverflowBlock()
			if err != nil {
				return false, err
			}
			b.next = nextBlock.offset
			originalB = b
			b = nextBlock
			entryIdx = 0
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return err
	}

	// Inserting a new item.
	if b.entries[entryIdx].kvOffset == 0 {
		if db.count == MaxKeys {
			return errors.New("database is full")
		}
		db.count++
	} else {
		defer db.data.free(b.entries[entryIdx].kvSize(), b.entries[entryIdx].kvOffset)
	}

	b.entries[entryIdx] = entry{
		hash:      hash,
		topicSize: uint16(len(topic)),
		valueSize: uint32(len(value)),
		expiresAt: expiresAt,
	}
	if b.entries[entryIdx].kvOffset, err = db.data.writeKeyValue(topic, key, value); err != nil {
		return err
	}
	if err := b.write(); err != nil {
		return err
	}
	if originalB != nil {
		return originalB.write()
	}
	return nil
}

// Delete deletes the given key from the DB.
func (db *DB) Delete(key []byte) error {
	h := db.hash(key)
	db.mu.Lock()
	defer db.mu.Unlock()
	b := blockHandle{}
	entryIdx := -1
	err := db.forEachBlock(db.blockIndex(h), false, func(curb blockHandle) (bool, error) {
		b = curb
		for i := 0; i < entriesPerBlock; i++ {
			e := b.entries[i]
			if e.kvOffset == 0 {
				return b.next == 0, nil
			} else if h == e.hash {
				eKey, err := db.data.readKey(e)
				if err != nil {
					return true, err
				}
				if bytes.Equal(key, eKey) {
					entryIdx = i
					return true, nil
				}
			}
		}
		return false, nil
	})
	if entryIdx == -1 || err != nil {
		return err
	}
	e := b.entries[entryIdx]
	b.del(entryIdx)
	if err := b.write(); err != nil {
		return err
	}
	db.data.free(e.kvSize(), e.kvOffset)
	db.count--
	return err
}

func (db *DB) split() error {
	updatedBlockIdx := db.splitBlockIdx
	updatedBlockOff := blockOffset(updatedBlockIdx)
	updatedBlock := entryWriter{
		block: &blockHandle{file: db.index, offset: updatedBlockOff},
	}

	newBlockOff, err := db.index.extend(blockSize)
	if err != nil {
		return err
	}
	newBlock := entryWriter{
		block: &blockHandle{file: db.index, offset: newBlockOff},
	}

	db.splitBlockIdx++
	if db.splitBlockIdx == 1<<db.level {
		db.level++
		db.splitBlockIdx = 0
	}

	var overflowBlocks []int64
	if err := db.forEachBlock(updatedBlockIdx, false, func(curb blockHandle) (bool, error) {
		for j := 0; j < entriesPerBlock; j++ {
			e := curb.entries[j]
			if e.kvOffset == 0 {
				break
			}
			if db.blockIndex(e.hash) == updatedBlockIdx {
				if err := updatedBlock.insert(e, db); err != nil {
					return true, err
				}
			} else {
				if err := newBlock.insert(e, db); err != nil {
					return true, err
				}
			}
		}
		if curb.next != 0 {
			overflowBlocks = append(overflowBlocks, curb.next)
		}
		return false, nil
	}); err != nil {
		return err
	}

	for _, off := range overflowBlocks {
		db.data.free(blockSize, off)
	}

	if err := newBlock.write(); err != nil {
		return err
	}
	if err := updatedBlock.write(); err != nil {
		return err
	}

	db.nBlocks++
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
	var err error
	is, err := db.index.Stat()
	if err != nil {
		return -1, err
	}
	ds, err := db.data.Stat()
	if err != nil {
		return -1, err
	}
	return is.Size() + ds.Size(), nil
}
