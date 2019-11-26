package tracedb

import (
	"encoding/binary"
	"time"

	"github.com/allegro/bigcache"
	"github.com/saffat-in/tracedb/filter"
)

var config = bigcache.Config{
	// number of shards (must be a power of 2)
	Shards: 1024,

	// time after which entry can be evicted
	LifeWindow: 10 * time.Minute,

	// Interval between removing expired entries (clean up).
	// If set to <= 0 then no action is performed.
	// Setting to < 1 second is counterproductive — bigcache has a one second resolution.
	CleanWindow: 5 * time.Minute,

	// rps * lifeWindow, used only in initial memory allocation
	MaxEntriesInWindow: 1000 * 10 * 60,

	// max entry size in bytes, used only in initial memory allocation
	MaxEntrySize: 500,

	// prints information about additional memory allocation
	Verbose: true,

	// cache will not allocate more memory than this limit, value in MB
	// if value is reached then the oldest entries can be overridden for the new ones
	// 0 value means no size limit
	HardMaxCacheSize: 8192,

	// callback fired when the oldest entry is removed because of its expiration time or no space left
	// for the new entry, or because delete was called. A bitmask representing the reason will be returned.
	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	OnRemove: nil,

	// OnRemoveWithReason is a callback fired when the oldest entry is removed because of its expiration time or no space left
	// for the new entry, or because delete was called. A constant representing the reason will be passed through.
	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	// Ignored if OnRemove is specified.
	OnRemoveWithReason: nil,
}

type Filter struct {
	table
	filterBlock *filter.FilterGenerator
	cache       *bigcache.BigCache
	cacheID     uint64
}

func (f *Filter) Append(h uint64) {
	f.filterBlock.Append(h)
}

func (f *Filter) Test(h uint64) bool {
	/// Test filter block for presence
	fltr, _ := f.getFilterBlock(true)
	if fltr != nil && !fltr.Test(h) {
		return false
	}
	return true
}

// Close finalizes the SST being written.
func (f *Filter) close() error {
	f.writeFilterBlock()
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

// writeFilterBlock writes the filter block and returns a blockHandle pointing to it.
func (f *Filter) writeFilterBlock() error {
	d := f.filterBlock.Finish()
	if _, err := f.WriteAt(d, 0); err != nil {
		return err
	}

	return nil
}

func (f *Filter) getFilterBlock(fillCache bool) (*filter.FilterBlock, error) {
	if f.size <= 0 {
		return nil, nil
	}
	var cacheKey string
	if f.cache != nil {
		var kb [8]byte
		binary.LittleEndian.PutUint64(kb[:8], f.cacheID^uint64(f.size))
		// binary.LittleEndian.PutUint64(kb[4:], uint64(f.size))
		cacheKey = string(kb[:])

		if data, err := f.cache.Get(cacheKey); data != nil {
			return filter.NewFilterBlock(data), err
		}
	}

	raw := make([]byte, f.size)
	if _, err := f.ReadAt(raw, 0); err != nil {
		return nil, err
	}

	if f.cache != nil && fillCache {
		f.cache.Set(cacheKey, raw)
	}
	return filter.NewFilterBlock(raw), nil
}
