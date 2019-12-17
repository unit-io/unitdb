package memdb

import (
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
)

const (
	idSize          = 20
	entriesPerBlock = 19
	indexPostfix    = ".index"
	version         = 1 // file format version

	// MaxTableSize value for maximum memroy use for the memdb.
	MaxTableSize = (int64(1) << 33) - 1
)

type dbInfo struct {
	seq                    uint64
	count                  uint32
	nBlocks                uint32
	blockIndex             uint32
	lastCommitedBlockIndex uint32
}

// DB represents the topic->key-value storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	// Need 64-bit alignment.
	mu        sync.RWMutex
	index     tableManager
	data      dataTable
	logWriter *writer
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
	data, err := mem.newTable(path, memSize)
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
	}
	// else {
	// 	if err := db.readHeader(); err != nil {
	// 		if err := index.close(); err != nil {
	// 			log.Print(err)
	// 		}
	// 		if err := mem.remove(index.name()); err != nil {
	// 			log.Print(err)
	// 		}
	// 		if err := data.close(); err != nil {
	// 			log.Print(err)
	// 		}
	// 		if err := mem.remove(data.name()); err != nil {
	// 			log.Print(err)
	// 		}
	// 		// Data file exists, but index is missing.
	// 		return nil, errors.New("database is corrupted")
	// 	}
	// }

	logOpts := options{Dirname: path, TargetSize: memSize}
	logWriter, err := newWriter(db.lastCommitedBlockIndex, logOpts)
	if err != nil {
		errors.New("Error creating WAL writer")
	}
	db.logWriter = logWriter
	if err := db.writeHeader(); err != nil {
		return nil, err
	}
	return db, nil
}

func (db *DB) writeHeader() error {
	h := header{
		signature: signature,
		version:   version,
		dbInfo:    db.dbInfo,
	}
	buf, err := h.MarshalBinary()
	if err != nil {
		return err
	}
	return db.logWriter.writeHeader(buf)
}

func (db *DB) readHeader() error {
	h := &header{}
	buf := make([]byte, headerSize)
	if _, err := db.index.readAt(buf, 0); err != nil {
		return err
	}
	if err := h.UnmarshalBinary(buf); err != nil {
		return err
	}
	// if !bytes.Equal(h.signature[:], signature[:]) {
	// 	return errCorrupted
	// }
	db.dbInfo = h.dbInfo
	return nil
}

func blockOffset(idx uint32) int64 {
	return int64(headerSize) + (int64(blockSize) * int64(idx))
}

// Close closes the DB.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	if err := db.writeHeader(); err != nil {
		return err
	}
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
	if err := db.logWriter.close(); err != nil {
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

func (db *DB) Sync(startSeq, endSeq uint64) error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	start, ok := db.entryCache[startSeq]
	if !ok {
		return errors.New("startSeq not found")
	}
	if start.blockIndex < db.lastCommitedBlockIndex {
		return errors.New(fmt.Sprintf("memdb.Sync: received start blockIndex less than last commited blockIndex: %d < %d", start.blockIndex, db.lastCommitedBlockIndex))
	}
	end, ok := db.entryCache[endSeq]
	if !ok {
		return errors.New("endSeq not found")
	}
	data, err := db.data.readRaw(start.offset, (end.offset-start.offset)+int64(end.messageSize))
	if err != nil {
		return errors.New("write failed")
	}
	if err := db.logWriter.append(start.blockIndex, data); err != nil {
		return errors.New("write failed")
	}
	return db.logWriter.sync()
}

func (db *DB) SignalBatchCommited(mseq uint64) error {
	e, ok := db.entryCache[mseq]
	if !ok {
		return errors.New("Seq not found")
	}
	db.lastCommitedBlockIndex = e.blockIndex
	return db.writeHeader()
}

// newBlock adds new block to db table and return block offset
func (db *DB) newBlock() (int64, error) {
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
		if _, err := db.newBlock(); err != nil {
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
