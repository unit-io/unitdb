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

package memdb

import (
	"encoding/binary"
	"fmt"
)

// Verify checks that the state the DB keeps besides its records agrees with
// them, and returns the first disagreement found:
//
//   - a time block in use has not been freed;
//   - each record points at an entry of the block's data holding its key
//     and its deleted flag;
//   - a block's count is the number of its records not deleted;
//   - the time filter of each key's block key names the time block, or Get
//     and Delete would not look in it.
//
// It may run alongside writes; the tests run it after every operation.
func (db *DB) Verify() error {
	if err := db.ok(); err != nil {
		return err
	}
	db.mu.RLock()
	blocks := make(map[_TimeID]*_Block, len(db.timeBlocks))
	for timeID, b := range db.timeBlocks {
		blocks[timeID] = b
	}
	db.mu.RUnlock()

	for timeID, b := range blocks {
		if err := db.verifyBlock(timeID, b); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) verifyBlock(timeID _TimeID, b *_Block) error {
	b.RLock()
	defer b.RUnlock()
	if b.data == nil {
		// Freed: a block a release took out of timeBlocks may be freed
		// since the snapshot, but no block still in it may be.
		if cur, ok := db.timeBlock(timeID); ok && cur == b {
			return fmt.Errorf("memdb: time block %d is in use and freed", timeID)
		}
		return nil
	}
	var live int64
	for ikey, off := range b.records {
		if ikey.delFlag == 0 {
			live++
		}
		if err := b.verifyRecord(ikey, off); err != nil {
			return fmt.Errorf("memdb: time block %d: %w", timeID, err)
		}
		if ikey.delFlag != 0 {
			continue
		}
		r, ok := db.timeFilters[db.blockKey(ikey.key)]
		if !ok {
			return fmt.Errorf("memdb: key %d has no time filter", ikey.key)
		}
		r.RLock()
		_, ok = r.timeRecords[timeID]
		r.RUnlock()
		if !ok {
			return fmt.Errorf("memdb: time filter of key %d misses time block %d holding it", ikey.key, timeID)
		}
	}
	if live != b.count {
		return fmt.Errorf("memdb: time block %d counts %d entries; it holds %d", timeID, b.count, live)
	}
	return nil
}

// verifyRecord checks that the entry at off is the record's: put writes the
// entry length, the deleted flag and the key, then the value.
func (b *_Block) verifyRecord(ikey _Key, off int64) error {
	head, err := b.data.Slice(off, off+4+1+8)
	if err != nil {
		return fmt.Errorf("record of key %d at %d: %v", ikey.key, off, err)
	}
	dataLen := int64(binary.LittleEndian.Uint32(head[:4]))
	if dataLen < 4+1+8 || off+dataLen > b.data.Size() {
		return fmt.Errorf("record of key %d at %d: entry length %d out of the block's %d bytes", ikey.key, off, dataLen, b.data.Size())
	}
	if flag, key := head[4], binary.LittleEndian.Uint64(head[5:13]); flag != ikey.delFlag || key != ikey.key {
		return fmt.Errorf("record of key %d (deleted %d) at %d holds key %d (deleted %d)", ikey.key, ikey.delFlag, off, key, flag)
	}
	return nil
}
