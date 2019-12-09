package memdb

import (
	"errors"
	"io"
	"log"
	"sort"
	"sync"
)

type Cache interface {
	Has(key uint64) bool
	Get(key uint64, size uint32) ([]byte, error)
	Set(key uint64, off int64, raw []byte) error
	Delete(key uint64) error
}

// MemCache represents the topic->key-value storage.
// All MemCache methods are safe for concurrent use by multiple goroutines.
type MemCache struct {
	mu    sync.RWMutex
	index tableManager
	data  dataTable
	// Free blocks
	fb freeIndices
	// Close.
	closed uint32
	closer io.Closer
}

// New opens or creates a new MemCache. Minimum memroy size is 1GB
func NewCache(path string, memSize int64) (*MemCache, error) {
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
	mcache := &MemCache{
		index: index,
		data:  dataTable{tableManager: data},
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
		if _, err = mcache.index.extend(blockSize); err != nil {
			return nil, err
		}
		if _, err = mcache.data.extend(headerSize); err != nil {
			return nil, err
		}
	}

	return mcache, nil
}

// Close closes the MemCache.
func (mcache *MemCache) Close() error {
	mcache.mu.Lock()
	defer mcache.mu.Unlock()

	if err := mcache.index.close(); err != nil {
		return err
	}
	if err := mem.remove(mcache.index.name()); err != nil {
		return err
	}
	if err := mcache.data.close(); err != nil {
		return err
	}
	if err := mem.remove(mcache.data.name()); err != nil {
		return err
	}

	var err error
	if mcache.closer != nil {
		if err1 := mcache.closer.Close(); err == nil {
			err = err1
		}
		mcache.closer = nil
	}

	return err
}

type freeIndex struct {
	idx uint32
}

type freeIndices struct {
	idxs []freeIndex
}

func (fb *freeIndices) search(idx uint32) int {
	return sort.Search(len(fb.idxs), func(i int) bool {
		return fb.idxs[i].idx == idx
	})
}

func (fb *freeIndices) get() (ok bool, idx uint32) {
	if len(fb.idxs) == 0 {
		return ok, idx
	}
	// get first blockIndex
	idx = fb.idxs[0].idx
	fb.idxs = fb.idxs[1:]
	return true, idx
}

func (fb *freeIndices) free(idx uint32) (ok bool) {
	i := fb.search(idx)
	if i < len(fb.idxs) && idx == fb.idxs[i].idx {
		return false
	}

	fb.idxs = append(fb.idxs, freeIndex{})
	copy(fb.idxs[i+1:], fb.idxs[i:])
	fb.idxs[i] = freeIndex{idx: idx}
	return true
}

func memOffset(idx uint32) int64 {
	return int64(blockSize) * int64(idx)
}

func memBlockIndex(off int64) uint32 {
	return uint32(off / int64(blockSize))
}

type BlockCache struct {
	mcache *MemCache
	cache  map[uint64]int64
}

func (mcache *MemCache) NewBlockCache() (Cache, error) {
	if mcache.index == nil {
		return nil, errors.New("memcache table is empty")
	}
	return BlockCache{mcache: mcache, cache: make(map[uint64]int64)}, nil
}

func (bc BlockCache) Has(key uint64) bool {
	_, ok := bc.cache[key]
	return ok
}

func (bc BlockCache) Get(key uint64, blockSize uint32) ([]byte, error) {
	off, ok := bc.cache[key]
	if !ok {
		return nil, errors.New("cache key not found")
	}
	b := blockHandle{table: bc.mcache.index, offset: off}
	raw, err := b.readRaw()
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func (bc BlockCache) set(off int64, raw []byte) error {
	b := blockHandle{table: bc.mcache.index, offset: off}
	if err := b.writeRaw(raw); err != nil {
		return err
	}
	return nil
}

func (bc BlockCache) Set(key uint64, off int64, raw []byte) (err error) {
	if raw == nil {
		return errors.New("no block to cache")
	}
	if _, ok := bc.cache[key]; !ok {
		// look for any empty blocks in free list
		if ok, idx := bc.mcache.fb.get(); ok {
			off = memOffset(idx)
		} else {
			off, err = bc.mcache.index.extend(blockSize)
			if err != nil {
				return err
			}
		}
		if err = bc.set(off, raw); err != nil {
			return err
		}
		bc.cache[key] = off
	}
	return err
}

func (bc BlockCache) Delete(key uint64) error {
	off, ok := bc.cache[key]
	if !ok {
		return errors.New("cache key not found")
	}
	blockIdx := memBlockIndex(off)
	bc.mcache.fb.free(blockIdx)
	delete(bc.cache, key)
	return nil
}

type DataCache struct {
	mcache *MemCache
	cache  map[uint64]int64
}

func (mcache *MemCache) NewDataCache() (Cache, error) {
	if mcache.data.tableManager == nil {
		return nil, errors.New("memcache table is empty")
	}
	return DataCache{mcache: mcache, cache: make(map[uint64]int64)}, nil
}

func (dc DataCache) Has(key uint64) bool {
	_, ok := dc.cache[key]
	return ok
}

func (dc DataCache) Get(key uint64, kvSize uint32) ([]byte, error) {
	off, ok := dc.cache[key]
	if !ok {
		return nil, errors.New("cache key not found")
	}
	return dc.mcache.data.readRaw(off, int64(kvSize))
}

func (dc DataCache) Set(key uint64, off int64, raw []byte) (err error) {
	if _, ok := dc.cache[key]; !ok {
		if raw != nil {
			off, err = dc.mcache.data.writeRaw(raw)
			if err != nil {
				return err
			}
		}
		dc.cache[key] = off
	}
	return nil
}

func (dc DataCache) Delete(key uint64) error {
	_, ok := dc.cache[key]
	if !ok {
		return errors.New("cache key not found")
	}
	delete(dc.cache, key)
	return nil
}
