/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unitdb

import (
	"github.com/unit-io/unitdb/filter"
	"github.com/unit-io/unitdb/memdb"
)

// Filter filter is bloom filter generator.
type Filter struct {
	file
	filterBlock *filter.Generator
	cache       *memdb.DB
	cacheID     uint64
}

// Append appends an entry to bloom filter.
func (f *Filter) Append(h uint64) {
	f.filterBlock.Append(h)
}

// Test tests entry in bloom filter. It returns false if entry definitely does not exist or entry maybe existing in DB.
func (f *Filter) Test(h uint64) bool {
	/// Test filter block for presence.
	fltr, _ := f.getFilterBlock(true)
	if fltr != nil && !fltr.Test(h) {
		return false
	}
	return true
}

// Close finalizes writing filter to file.
func (f *Filter) close() error {
	f.writeFilterBlock()
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}

// writeFilterBlock writes the filter block.
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
