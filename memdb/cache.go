package memdb

import (
	"errors"
	"log"
)

type Cache interface {
	Has(key uint64) bool
	Get(key uint64, size uint32) ([]byte, error)
	Set(key uint64, off int64, raw []byte) error
	Delete(key uint64) error
}

type BlockCache struct {
	db    *DB
	cache map[uint64]int64
}

func (db *DB) NewBlockCache() (Cache, error) {
	if db.index == nil {
		return nil, errors.New("memdb table is empty")
	}
	return BlockCache{db: db, cache: make(map[uint64]int64)}, nil
}

func (bc BlockCache) Has(key uint64) bool {
	_, ok := bc.cache[key]
	return ok
}

func (bc BlockCache) Get(key uint64, blockSize uint32) ([]byte, error) {
	off, ok := bc.cache[key]
	if !ok {
		// log.Println("cache.Get: cache key not found ", key)
		return nil, errors.New("cache key not found")
	}
	b := blockHandle{table: bc.db.index, offset: off}
	raw, err := b.readRaw()
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func (bc BlockCache) set(off int64, raw []byte) error {
	b := blockHandle{table: bc.db.index, offset: off}
	if err := b.writeRaw(raw); err != nil {
		log.Println("cache.Set: writeRaw error ", err)
		return err
	}
	bc.db.count += entriesPerBlock
	return nil
}

func (bc BlockCache) Set(key uint64, off int64, raw []byte) error {
	if raw == nil {
		return errors.New("no block to cache")
	}
	if _, ok := bc.cache[key]; !ok {
		// look for any empty blocks in free list
		if ok, idx := bc.db.fb.get(true); ok {
			off = blockOffset(idx)
		} else {
			// add current blockIndex to free list incase any slot is empty
			bc.db.fb.free(bc.db.blockIndex, false)
			// extend index to accomodate new cache block
			off = bc.db.newBlock()
		}
		if err := bc.set(off, raw); err != nil {
			return err
		}
		bc.cache[key] = off
		// extend index after new cache is added and increase the db count and block counts
		bc.db.newBlock()
		// log.Println("cache.Set: blockIndex, nBlocks ", bc.db.blockIndex, bc.db.nBlocks)
	}
	return nil
}

func (bc BlockCache) Delete(key uint64) error {
	off, ok := bc.cache[key]
	if !ok {
		// log.Println("cache.Delete: cache key not found ", key)
		return errors.New("cache key not found")
	}
	blockIdx := startBlockIndex(off)
	bc.db.fb.free(blockIdx, true)
	delete(bc.cache, key)
	bc.db.count -= entriesPerBlock
	// log.Println("cache.Delete: blockIndex, Offset ", blockIdx, off)
	return nil
}

type NewDataCache struct {
	db    *DB
	cache map[uint64]int64
}

func (db *DB) NewDataCache() (Cache, error) {
	if db.data.tableManager == nil {
		return nil, errors.New("memdb table is empty")
	}
	return NewDataCache{db: db, cache: make(map[uint64]int64)}, nil
}

func (dc NewDataCache) Has(key uint64) bool {
	_, ok := dc.cache[key]
	return ok
}

func (dc NewDataCache) Get(key uint64, kvSize uint32) ([]byte, error) {
	off, ok := dc.cache[key]
	if !ok {
		return nil, errors.New("cache key not found")
	}
	return dc.db.data.readRaw(off, int64(kvSize))
}

func (dc NewDataCache) Set(key uint64, off int64, raw []byte) (err error) {
	if _, ok := dc.cache[key]; !ok {
		if raw != nil {
			off, err = dc.db.data.writeRaw(raw)
			if err != nil {
				return err
			}
		}
		dc.cache[key] = off
	}
	return nil
}

func (dc NewDataCache) Delete(key uint64) error {
	_, ok := dc.cache[key]
	if !ok {
		return errors.New("cache key not found")
	}
	delete(dc.cache, key)
	return nil
}
