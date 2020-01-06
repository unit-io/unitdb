package memdb

import (
	"encoding/binary"
	"errors"
	"math"
	"sync"

	"github.com/unit-io/tracedb/hash"
)

const (
	//MaxBlocks support for sharding memdb block cache
	MaxBlocks  = math.MaxUint32 / 4096
	nContracts = 271 // TODO implelemt sharding based on total Contracts in db

	// MaxTableSize value for maximum memroy use for the memdb.
	MaxTableSize = (int64(1) << 33) - 1
)

// A "thread" safe map of type seq:offset.
// To avoid lock bottlenecks this map is dived to several (SHARD_COUNT) map shards.
type blockCache []*concurrentCache

type concurrentCache struct {
	cache        map[uint64]int64 // seq and offset map
	sync.RWMutex                  // Read Write mutex, guards access to internal map.
}

// newCache creates a new concurrent block cache.
func newCache() blockCache {
	m := make(blockCache, nContracts)
	for i := 0; i < nContracts; i++ {
		m[i] = &concurrentCache{cache: make(map[uint64]int64)}
	}
	return m
}

// DB represents the topic->key-value storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	mu   sync.RWMutex
	data dataTable
	//block cache
	consistent *hash.Consistent
	blockCache blockCache // seq and offset map
	// Close.
	// closed uint32
	// closer io.Closer
}

// Open opens or creates a new DB. Minimum memroy size is 1GB
func Open(path string, memSize int64) (*DB, error) {
	if memSize > MaxTableSize {
		memSize = MaxTableSize
	}
	data, err := mem.newTable(path, memSize)
	if err != nil {
		return nil, err
	}
	db := &DB{
		data:       dataTable{tableManager: data},
		blockCache: newCache(),
	}

	db.consistent = hash.InitConsistent(int(MaxBlocks), int(nContracts))

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

	// var err error
	// if db.closer != nil {
	// 	if err1 := db.closer.Close(); err == nil {
	// 		err = err1
	// 	}
	// 	db.closer = nil
	// }

	return nil
}

// GetShard returns shard under given contract
func (db *DB) GetShard(contract uint32) *concurrentCache {
	return db.blockCache[db.consistent.FindBlock(contract)]
}

// Get gets data for the provided key
func (db *DB) Get(contract uint32, key uint64) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	// Get shard
	shard := db.GetShard(contract)
	shard.RLock()
	// Get item from shard.
	off, ok := shard.cache[key]
	shard.RUnlock()
	// off, ok := db.cache[key]
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
func (db *DB) Set(contract uint32, key uint64, data []byte) error {
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
	// Get cache shard.
	shard := db.GetShard(contract)
	shard.Lock()
	shard.cache[key] = off
	shard.Unlock()
	// db.cache[key] = off
	return nil
}

// Count returns the number of items in the DB.
func (db *DB) Count() uint32 {
	count := 0
	for i := 0; i < nContracts; i++ {
		shard := db.blockCache[i]
		shard.RLock()
		count += len(shard.cache)
		shard.RUnlock()
	}
	return uint32(count)
}

// FileSize returns the total size of the disk storage used by the DB.
func (db *DB) FileSize() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.data.size, nil
}
