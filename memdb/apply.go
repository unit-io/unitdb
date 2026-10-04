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

import "encoding/binary"

// putEntry and deleteEntry are the two changes to the blocks: Put, Batch
// and Delete make them as they run, and recovery makes them again, entry by
// entry of the WAL in the order written. Recovery kept its own copy of them,
// which differed: it applied a log's puts after its deletes, rebuilt a
// block's data without its deletes and marked it unwritten, and freed
// blocks without their logs.

// putEntry appends key's entry to block, of time ID timeID, replacing a
// version of key the block holds.
func (db *DB) putEntry(block *_Block, timeID _TimeID, key uint64, data []byte) error {
	block.Lock()
	defer block.Unlock()
	if err := block.put(iKey(false, key), data); err != nil {
		return err
	}
	db.addTimeFilter(timeID, key)
	db.internal.meter.Puts.Inc(1)
	return nil
}

// deleteEntry deletes key's version in the block from, of time ID fromID,
// and appends the delete to the block to, for the WAL: from's logs then
// stay in it until to's go (applyLogs). A nil from, in recovery, is a
// block released, whose versions are gone: the delete is only appended.
// It reports whether from was left with no entries and its data all in
// the WAL, for the caller to release it.
func (db *DB) deleteEntry(from *_Block, fromID _TimeID, to *_Block, key uint64) (emptied bool, err error) {
	if from != nil {
		from.Lock()
		// The key may be gone: a concurrent Delete, or a filter's block
		// the key is not in.
		if err := from.delete(key); err != nil {
			from.Unlock()
			return false, err
		}
		emptied = from.count == 0 && from.lastOffset == from.size()
		from.Unlock()
		db.internal.meter.Dels.Inc(1)
	}

	var ref [8]byte
	binary.LittleEndian.PutUint64(ref[:], uint64(fromID))
	to.Lock()
	err = to.put(iKey(true, key), ref[:])
	to.Unlock()
	if err != nil || from == nil {
		return emptied, err
	}
	db.internal.logMu.Lock()
	to.deleteFrom(from)
	db.internal.logMu.Unlock()
	return emptied, nil
}
