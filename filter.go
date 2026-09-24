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
)

// Filter is a bloom filter of the sequences written to the index. It is kept
// in memory and persisted by sync before index blocks are written, so the
// saved filter never rules out an entry that is on disk.
type Filter struct {
	file        _FileSet
	filterBlock *filter.Generator
}

// Append appends an entry to bloom filter.
func (f *Filter) Append(h uint64) {
	f.filterBlock.Append(h)
}

// Test tests entry in bloom filter. It returns false if entry definitely does not exist or true may be entry exist in DB.
func (f *Filter) Test(h uint64) bool {
	return f.filterBlock.Test(h)
}

// write persists the filter followed by its checksum.
func (f *Filter) write() error {
	data := f.filterBlock.Bytes()
	buf := make([]byte, len(data)+checksumSize)
	copy(buf, data)
	putChecksum(buf, len(data))
	_, err := f.file.WriteAt(buf, 0)
	return err
}

// loadFilter restores the filter saved by sync. A db without a valid saved
// filter, such as one created before the filter was persisted, has it rebuilt
// from the index so that entries already on disk are never ruled out.
func (db *DB) loadFilter() error {
	f := &db.internal.filter
	if size := f.file.currSize(); size == int64(filter.Size()+checksumSize) {
		raw := make([]byte, size)
		if _, err := f.file.ReadAt(raw, 0); err != nil {
			return err
		}
		// Strict: an all-zero filter would rule out every entry.
		if matchesChecksum(raw, filter.Size()) {
			f.filterBlock = filter.NewFilterGeneratorFromBytes(raw[:filter.Size()])
			return nil
		}
		// The filter is derived from the index, so rebuild it rather than refuse to open.
		logger.Error().Err(corrupted(f.file._File, 0, "filter")).Str("context", "db.loadFilter").Msg("rebuilding filter from index")
	}

	f.filterBlock = filter.NewFilterGenerator()
	indexFile, err := db.fs.getFile(_FileDesc{fileType: typeIndex})
	if err != nil {
		return err
	}
	r := _BlockReader{indexFile: indexFile}
	for off := int64(0); off+int64(blockSize) <= indexFile.currSize(); off += int64(blockSize) {
		b, err := r.readIndexBlock(off)
		if err != nil {
			return err
		}
		for _, e := range b.entries {
			if e.seq != 0 {
				f.Append(e.seq)
			}
		}
	}

	return f.write()
}
