package memdb

import (
	"errors"
	"io"
	"sync"
)

const (
	idSize          = 20
	entriesPerBlock = 19
	indexPostfix    = ".index"
	dataPostfix     = ".data"
	version         = 1 // file format version

	// MaxTableSize value for maximum memroy use for the memdb.
	MaxTableSize = (int64(1) << 33) - 1
)

type dbInfo struct {
	count      uint32
	nBlocks    uint32
	blockIndex uint32
}

// DB represents the topic->key-value storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	mu    sync.RWMutex
	index tableManager
	data  dataTable
	//block cache
	entryCache map[uint64]*entryHeader
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
	data, err := mem.newTable(path+dataPostfix, memSize)
	if err != nil {
		return nil, err
	}
	db := &DB{
		index:      index,
		data:       dataTable{tableManager: data},
		entryCache: make(map[uint64]*entryHeader, 100),
		dbInfo: dbInfo{
			nBlocks: 1,
		},
	}

	if index.size() == 0 {
		if _, err = db.index.extend(blockSize); err != nil {
			return nil, err
		}
	}

	return db, nil
}

func blockOffset(idx uint32) int64 {
	return int64(blockSize) * int64(idx)
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

// newBlock adds new block to db table and return block offset
func (db *DB) NewBlock() (int64, error) {
	if db.count == 0 {
		return 0, nil
	}
	off, err := db.index.extend(blockSize)
	db.nBlocks++
	db.blockIndex++
	// log.Println("memdb.newBlock: blockIndex, nBlocks ", db.blockIndex, db.nBlocks)
	return off, err
}

func (db *DB) Has(mseq uint64) bool {
	db.mu.RLock()
	defer db.mu.RUnlock()
	_, ok := db.entryCache[mseq]
	return ok
}

func (db *DB) Get(mseq uint64) ([]byte, []byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	e, ok := db.entryCache[mseq]
	if !ok {
		return nil, nil, errors.New("cache for entry seq not found")
	}
	off := blockOffset(e.blockIndex)
	b := blockHandle{table: db.index, offset: off}
	block, err := b.readRaw()
	if err != nil {
		return nil, nil, err
	}
	data, err := db.data.readRaw(e.offset, int64(e.messageSize))
	return block, data, err
}

func (db *DB) GetBlock(mseq uint64) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	e, ok := db.entryCache[mseq]
	if !ok {
		return nil, errors.New("cache for entry seq not found")
	}
	off := blockOffset(e.blockIndex)
	b := blockHandle{table: db.index, offset: off}
	raw, err := b.readRaw()
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func (db *DB) GetData(mseq uint64) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	e, ok := db.entryCache[mseq]
	if !ok {
		return nil, errors.New("cache for entry seq not found")
	}
	return db.data.readRaw(e.offset, int64(e.messageSize))
}

// Put sets the value for the given topic->key. It updates the value for the existing key.
func (db *DB) Put(mseq uint64, seq uint64, id, topic, value []byte, offset int64, expiresAt uint32) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	off := blockOffset(db.blockIndex)
	b := &blockHandle{table: db.index, offset: off}
	if err := b.readFooter(); err != nil {
		return err
	}
	db.count++
	ew := entryWriter{
		block: b,
	}
	ew.entry = entry{
		seq:       seq,
		topicSize: uint16(len(topic)),
		valueSize: uint32(len(value)),
		mOffset:   offset,
		expiresAt: expiresAt,
	}
	memoff, err := db.data.writeMessage(id, topic, value)
	if err != nil {
		return err
	}
	if err := ew.write(); err != nil {
		return err
	}
	db.entryCache[mseq] = &entryHeader{blockIndex: db.blockIndex, messageSize: ew.entry.mSize(), offset: memoff}
	if b.entryIdx == entriesPerBlock {
		if _, err := db.NewBlock(); err != nil {
			return err
		}
	}
	return err
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
