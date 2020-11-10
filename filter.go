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
	file        _FileSet
	filterBlock *filter.Generator
	blockCache  *memdb.DB
	cacheID     uint64
}

// Append appends an entry to bloom filter.
func (f *Filter) Append(h uint64) {
	f.filterBlock.Append(h)
}

// Test tests entry in bloom filter. It returns false if entry definitely does not exist or true may be entry exist in DB.
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
	if err := f.file.Close(); err != nil {
		return err
	}
	return nil
}

// writeFilterBlock writes the filter block.
func (f *Filter) writeFilterBlock() error {
	d := f.filterBlock.Finish()
	if _, err := f.file.WriteAt(d, 0); err != nil {
		return err
	}

	return nil
}

func (f *Filter) getFilterBlock(fillCache bool) (*filter.Block, error) {
	if f.file.size <= 0 {
		return nil, nil
	}
	var cacheKey uint64
	if f.blockCache != nil {
		cacheKey = f.cacheID ^ uint64(f.file.size)
		if data, err := f.blockCache.Get(cacheKey); data != nil {
			return filter.NewFilterBlock(data), err
		}
	}

	raw := make([]byte, f.file.size)
	if _, err := f.file.ReadAt(raw, 0); err != nil {
		return nil, err
	}

	if f.blockCache != nil && fillCache {
		f.blockCache.Put(cacheKey, raw)
	}
	return filter.NewFilterBlock(raw), nil
}
