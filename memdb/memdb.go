package memdb

import (
	"encoding/binary"
	"errors"
	"sync"
	"time"

	"github.com/unit-io/unitdb/hash"
)

const (
	nShards = 32

	// maxTableSize value for maximum memory use for memdb.
	maxTableSize = (int64(1) << 40) - 1

	drainInterval         = 1 * time.Second
	memShrinkFactor       = 0.7
	dataTableShrinkFactor = 0.33 // shrinker try to free 33% of total memdb size
)

// To avoid lock bottlenecks block cache is divided into several (nShards) shards.
type blockCache []*memCache

type memCache struct {
	data         dataTable
	freeOffset   int64            // mem cache keep lowest offset that can be free.
	m            map[uint64]int64 // map[seq]offset
	sync.RWMutex                  // Read Write mutex, guards access to internal map.
}

// newBlockCache creates a new concurrent block cache.
func newBlockCache(memSize int64) blockCache {
	m := make(blockCache, nShards)
	for i := 0; i < nShards; i++ {
		m[i] = &memCache{data: dataTable{}, m: make(map[uint64]int64)}
	}
	return m
}

// DB represents the block cache mem store.
// All DB methods are safe for concurrent use by multiple goroutines.
// Note: memdb is not a general purpose mem store but it designed for specific use in unitdb.
type DB struct {
	targetSize int64
	resetLockC chan struct{}
	// block cache
	consistent *hash.Consistent
	blockCache blockCache

	// close
	closeC chan struct{}
}

// Open opens or creates a new DB of given size.
func Open(memSize int64) (*DB, error) {
	if memSize > maxTableSize {
		memSize = maxTableSize
	}
	db := &DB{
		targetSize: memSize,
		blockCache: newBlockCache(memSize),
		// Close
		closeC: make(chan struct{}),
	}

	db.consistent = hash.InitConsistent(int(nShards), int(nShards))

	db.drain(drainInterval)

	return db, nil
}

func (db *DB) drain(interval time.Duration) {
	shrinkerTicker := time.NewTicker(interval)
	go func() {
		defer func() {
			shrinkerTicker.Stop()
		}()
		for {
			select {
			case <-db.closeC:
				return
			case <-shrinkerTicker.C:
				memSize, err := db.Size()
				if err == nil && float64(memSize) > float64(db.targetSize)*memShrinkFactor {
					db.shrinkDataTable()
				}
			}
		}
	}()
}

func (db *DB) shrinkDataTable() error {
	for i := 0; i < nShards; i++ {
		cache := db.blockCache[i]
		cache.Lock()
		if cache.freeOffset > 0 {
			if err := cache.data.shrink(cache.freeOffset); err != nil {
				return err
			}
		}
		for seq, off := range cache.m {
			if off < cache.freeOffset {
				delete(cache.m, seq)
			} else {
				cache.m[seq] = off - cache.freeOffset
			}
		}
		cache.freeOffset = 0
		cache.Unlock()
	}

	return nil
}

// Close closes the memdb.
func (db *DB) Close() error {
	for i := 0; i < nShards; i++ {
		cache := db.blockCache[i]
		cache.RLock()
		if err := cache.data.close(); err != nil {
			return err
		}
		cache.RUnlock()
	}

	return nil
}

// getCache returns cache under given contract
func (db *DB) getCache(contract uint64) *memCache {
	return db.blockCache[db.consistent.FindBlock(contract)]
}

// Get gets data for the provided key under a contract
func (db *DB) Get(contract uint64, key uint64) ([]byte, error) {
	// Get cache
	cache := db.getCache(contract)
	cache.RLock()
	defer cache.RUnlock()
	// Get item from cache.
	off, ok := cache.m[key]
	if !ok {
		return nil, nil
	}
	if off == -1 {
		return nil, errors.New("entry deleted")
	}
	scratch, err := cache.data.readRaw(off, 4) // read data length
	if err != nil {
		return nil, err
	}
	dataLen := binary.LittleEndian.Uint32(scratch[:4])
	data, err := cache.data.readRaw(off, dataLen)
	if err != nil {
		return nil, err
	}
	return data[4:], nil
}

// Remove sets data offset to -1 for the key under a contract
func (db *DB) Remove(contract uint64, key uint64) error {
	// Get cache
	cache := db.getCache(contract)
	cache.RLock()
	defer cache.RUnlock()
	// Get item from cache.
	if _, ok := cache.m[key]; ok {
		cache.m[key] = -1
	}
	return nil
}

// Set sets the value for the given entry for a contract.
func (db *DB) Set(contract uint64, key uint64, data []byte) error {
	// Get cache.
	cache := db.getCache(contract)
	cache.Lock()
	defer cache.Unlock()
	off, err := cache.data.allocate(uint32(len(data) + 4))
	if err != nil {
		return err
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(data)+4))

	if _, err := cache.data.writeAt(scratch[:], off); err != nil {
		return err
	}
	if _, err := cache.data.writeAt(data, off+4); err != nil {
		return err
	}
	cache.m[key] = off
	return nil
}

// Free free keeps first offset that can be free if memdb exceeds target size.
func (db *DB) Free(contract uint64, key uint64) error {
	// Get cache
	cache := db.getCache(contract)
	cache.Lock()
	defer cache.Unlock()
	if cache.freeOffset > 0 {
		return nil
	}
	off, ok := cache.m[key]
	// Get item from cache.
	if ok {
		if (cache.freeOffset == 0 || cache.freeOffset < off) && float64(off) > float64(cache.data.size)*dataTableShrinkFactor {
			cache.freeOffset = off
		}
	}

	return nil
}

// Count returns the number of items in memdb.
func (db *DB) Count() uint64 {
	count := 0
	for i := 0; i < nShards; i++ {
		cache := db.blockCache[i]
		cache.RLock()
		count += len(cache.m)
		cache.RUnlock()
	}
	return uint64(count)
}

// Size returns the total size of memdb.
func (db *DB) Size() (int64, error) {
	size := int64(0)
	for i := 0; i < nShards; i++ {
		cache := db.blockCache[i]
		cache.RLock()
		size += int64(cache.data.size)
		cache.RUnlock()
	}
	return size, nil
}
