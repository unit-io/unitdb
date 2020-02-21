package tracedb

import (
	"github.com/unit-io/tracedb/filter"
	"github.com/unit-io/tracedb/memdb"
)

// Filter filter is bloom filter generator
type Filter struct {
	file
	filterBlock *filter.Generator
	cache       *memdb.DB
	cacheID     uint64
}

// Append appends an entry to bloom filter
func (f *Filter) Append(h uint64) {
	f.filterBlock.Append(h)
}

// Test tests entry in bloom filter. it return if entry difinately does not exist or entry maybe existing in db
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

func (f *Filter) getFilterBlock(fillCache bool) (*filter.Block, error) {
	if f.size <= 0 {
		return nil, nil
	}
	var cacheKey uint64
	if f.cache != nil {
		cacheKey = f.cacheID ^ uint64(f.size)
		if data, err := f.cache.Get(0, cacheKey); data != nil {
			return filter.NewFilterBlock(data), err
		}
	}

	raw := make([]byte, f.size)
	if _, err := f.ReadAt(raw, 0); err != nil {
		return nil, err
	}

	if f.cache != nil && fillCache {
		f.cache.Set(0, cacheKey, raw)
	}
	return filter.NewFilterBlock(raw), nil
}
