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
// in memory, made from the index when the DB opens (deriveFromIndex), and
// added to by sync, which saves it, for older versions to read.
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
