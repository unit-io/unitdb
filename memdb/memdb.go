package memdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/unit-io/tracedb/hash"
)

const (
	//MaxBlocks support for sharding memdb block cache
	maxShards = math.MaxUint32 / 4096
	nShards   = 271 // TODO implelemt sharding based on total Contracts in db

	// maxTableSize value for maximum memroy use for the memdb.
	maxTableSize = (int64(1) << 33) - 1

	backgroundMemResetInterval = 1 * time.Second
	memShrinkFactor            = 0.7
	dataTableShrinkFactor      = 0.2 // shirnker try to free 20% of total datatable size
)

// A "thread" safe map of type seq:offset.
// To avoid lock bottlenecks this map is dived to several (SHARD_COUNT) map shards.
type blockCache []*concurrentCache

type concurrentCache struct {
	data         dataTable
	freeOffset   int64            // cache keep lowest offset that can be free.
	cache        map[uint64]int64 // seq and offset map
	sync.RWMutex                  // Read Write mutex, guards access to internal map.
}

// newCache creates a new concurrent block cache.
func newCache(path string, memSize int64) blockCache {
	m := make(blockCache, nShards)
	for i := 0; i < nShards; i++ {
		path := fmt.Sprintf("%s-%d.data", path, i)
		data, err := mem.newTable(path, memSize)
		if err != nil {
			return nil
		}
		m[i] = &concurrentCache{data: dataTable{tableManager: data}, cache: make(map[uint64]int64)}
	}
	return m
}

// DB represents the topic->key-value storage.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	targetSize int64
	resetLockC chan struct{}
	// block cache
	consistent *hash.Consistent
	blockCache blockCache // seq and offset map

	// close
	closeC chan struct{}
}

// Open opens or creates a new DB. Minimum memroy size is 1GB
func Open(path string, memSize int64) (*DB, error) {
	if memSize > maxTableSize {
		memSize = maxTableSize
	}
	db := &DB{
		targetSize: memSize,
		blockCache: newCache(path, memSize),
		// Close
		closeC: make(chan struct{}),
	}

	db.consistent = hash.InitConsistent(int(nShards), int(nShards))

	db.startBufferShrinker(backgroundMemResetInterval)

	return db, nil
}

func (db *DB) startBufferShrinker(interval time.Duration) {
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
				memSize, err := db.FileSize()
				if err == nil && float64(memSize) > float64(db.targetSize)*memShrinkFactor {
					db.shrinkDataTable()
				}
			}
		}
	}()
}

func (db *DB) shrinkDataTable() error {
	for i := 0; i < nShards; i++ {
		shard := db.blockCache[i]
		shard.Lock()
		if shard.freeOffset > 0 {
			if err := shard.data.shrink(shard.freeOffset); err != nil {
				return err
			}
		}
		for seq, off := range shard.cache {
			if off < shard.freeOffset {
				delete(shard.cache, seq)
			} else {
				shard.cache[seq] = off - shard.freeOffset
			}
		}
		shard.freeOffset = 0
		shard.Unlock()
	}

	return nil
}

// Close closes the DB.
func (db *DB) Close() error {
	// Signal all goroutines.
	close(db.closeC)

	for i := 0; i < nShards; i++ {
		shard := db.blockCache[i]
		shard.RLock()
		if err := shard.data.close(); err != nil {
			return err
		}
		if err := mem.remove(shard.data.name()); err != nil {
			return err
		}
		shard.RUnlock()
	}

	return nil
}

// getShard returns shard under given contract
func (db *DB) getShard(prefix uint64) *concurrentCache {
	return db.blockCache[db.consistent.FindBlock(prefix)]
}

// Get gets data for the provided key
func (db *DB) Get(prefix uint64, key uint64) ([]byte, error) {
	// Get shard
	shard := db.getShard(prefix)
	shard.RLock()
	// Get item from shard.
	off, ok := shard.cache[key]
	shard.RUnlock()
	if !ok {
		return nil, errors.New("cache for entry seq not found")
	}
	scratch, err := shard.data.readRaw(off, 4) // read dataLength
	if err != nil {
		return nil, err
	}
	dataLen := binary.LittleEndian.Uint32(scratch[:4])
	data, err := shard.data.readRaw(off, dataLen)
	if err != nil {
		return nil, err
	}
	return data[4:], nil
}

// Set sets the value for the given key->value. It updates the value for the existing key.
func (db *DB) Set(prefix uint64, key uint64, data []byte) error {
	// Get cache shard.
	shard := db.getShard(prefix)
	shard.Lock()
	defer shard.Unlock()
	off, err := shard.data.allocate(uint32(len(data) + 4))
	if err != nil {
		return err
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(data)+4))

	if _, err := shard.data.writeAt(scratch[:], off); err != nil {
		return err
	}
	if _, err := shard.data.writeAt(data, off+4); err != nil {
		return err
	}
	shard.cache[key] = off
	return nil
}

// Free free keeps first offset that can be free if memdb exceeds target size threshold.
func (db *DB) Free(prefix uint64, key uint64) error {
	// Get shard
	shard := db.getShard(prefix)
	shard.Lock()
	defer shard.Unlock()
	if shard.freeOffset > 0 {
		return nil
	}
	off, ok := shard.cache[key]
	// Get item from shard.
	if ok {
		if (shard.freeOffset == 0 || shard.freeOffset < off) && float64(off) > float64(shard.data.size)*dataTableShrinkFactor {
			shard.freeOffset = off
		}
	}

	return nil
}

// Count returns the number of items in the DB.
func (db *DB) Count() uint32 {
	count := 0
	for i := 0; i < nShards; i++ {
		shard := db.blockCache[i]
		shard.RLock()
		count += len(shard.cache)
		shard.RUnlock()
	}
	return uint32(count)
}

// FileSize returns the total size of the disk storage used by the DB.
func (db *DB) FileSize() (int64, error) {
	size := int64(0)
	for i := 0; i < nShards; i++ {
		shard := db.blockCache[i]
		shard.RLock()
		size += int64(shard.data.size)
		shard.RUnlock()
	}
	return size, nil
}
