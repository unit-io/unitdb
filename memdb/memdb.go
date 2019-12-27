package memdb

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"
)

const (
	// MaxTableSize value for maximum memroy use for the memdb.
	MaxTableSize = (int64(1) << 33) - 1
)

// DB represents the topic->key-value storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	mu   sync.RWMutex
	data dataTable
	//block cache
	cache map[uint64]int64
	// Close.
	closed uint32
	closer io.Closer
}

// Open opens or creates a new DB. Minimum memroy size is 1GB
func Open(path string, memSize int64) (*DB, error) {
	if memSize < 1<<30 {
		memSize = MaxTableSize
	}
	data, err := mem.newTable(path, memSize)
	if err != nil {
		return nil, err
	}
	db := &DB{
		data:  dataTable{tableManager: data},
		cache: make(map[uint64]int64, 100),
	}

	return db, nil
}

// Close closes the DB.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()

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

func (db *DB) Get(key uint64) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	off, ok := db.cache[key]
	if !ok {
		return nil, errors.New("cache for entry seq not found")
	}
	scratch, err := db.data.readRaw(off, 4) // read dataLength
	if err != nil {
		return nil, err
	}
	dataLen := binary.LittleEndian.Uint32(scratch[:4])
	data, err := db.data.readRaw(off, dataLen)
	if err != nil {
		return nil, err
	}
	return data[4:], nil
}

// Set sets the value for the given key->value. It updates the value for the existing key.
func (db *DB) Set(key uint64, data []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	off, err := db.data.allocate(uint32(len(data) + 4))
	if err != nil {
		return err
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(data)+4))

	if _, err := db.data.writeAt(scratch[:], off); err != nil {
		return err
	}
	if _, err := db.data.writeAt(data, off+4); err != nil {
		return err
	}
	db.cache[key] = off
	return nil
}

// Count returns the number of items in the DB.
func (db *DB) Count() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return uint32(len(db.cache))
}

// FileSize returns the total size of the disk storage used by the DB.
func (db *DB) FileSize() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.data.size, nil
}
