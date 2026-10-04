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
	"time"

	"github.com/unit-io/unitdb/wal"
)

// startRecovery recovers pending entries from the WAL.
func (db *DB) startRecovery() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// start log recovery
	r, err := db.internal.wal.NewReader()
	if err != nil {
		return err
	}

	// Deletes are applied in log order, to the versions recovered so far: a
	// delete refers to the block its version is in, and the key may be put
	// again in that block after it. Blocks left with no entries are released
	// once every log is read.
	//
	// Blocks recovered from logs that don't record their block.
	legacy := make(map[_TimeID]bool)
	err = r.Iterator(func(ID int64) (ok bool, err error) {
		// New logs must not reuse the IDs of these, if the clock went back.
		if ID > db.internal.lastLogID {
			db.internal.lastLogID = ID
		}
		log := make(map[uint64][]byte)
		l := r.Count()
		// The block the log was written for. A log from before the WAL
		// recorded it is put in the block of its time truncated to the
		// block duration, as writes are (newTinyLog); a batch's then joins
		// the block of its second.
		timeID := _TimeID(r.BlockID())
		if timeID == 0 {
			timeID = db.legacyBlock(_TimeID(ID))
			legacy[timeID] = true
		}
		block, ok := db.timeBlocks[timeID]
		if !ok {
			block = &_Block{data: db.internal.buffer.Get(), records: make(map[_Key]int64)}
			db.timeBlocks[timeID] = block
		}
		for i := uint32(0); i < l; i++ {
			logData, ok, err := r.Next()
			if err != nil {
				return false, err
			}
			if !ok {
				break
			}

			for off := 0; off < len(logData); {
				dBit, key, val, next, err := nextEntry(logData, off)
				if err != nil {
					return false, fmt.Errorf("%w: log %d: %v", wal.ErrCorrupted, ID, err)
				}
				if dBit > 1 {
					return false, fmt.Errorf("%w: log %d: entry at %d has flag %d", wal.ErrCorrupted, ID, off, dBit)
				}
				off = next
				if dBit == 1 && len(val) != 8 {
					// A put marked deleted, as older versions wrote one
					// deleted before it reached the WAL: the delete
					// follows it.
					continue
				}
				if dBit == 1 {
					timeRefID := _TimeID(binary.LittleEndian.Uint64(val[:8]))
					if timeRefID == timeID {
						// Put earlier in this log.
						delete(log, key)
					}
					// A batch's block is named by the time of the batch; a
					// log that doesn't record its block recovers it into
					// the block of its time truncated.
					if _, ok := db.timeBlocks[timeRefID]; !ok && legacy[db.legacyBlock(timeRefID)] {
						timeRefID = db.legacyBlock(timeRefID)
					}
					// A block that is not recovered was released: its
					// versions are gone already.
					if from, ok := db.timeBlocks[timeRefID]; ok {
						db.deleteRecovered(timeRefID, key)
						block.deleteFrom(from)
					}
				} else {
					log[key] = val
				}
			}
			db.internal.timeMark.add(timeID)
			block.Lock()
			for key, val := range log {
				ikey := iKey(false, key)
				if err := block.put(ikey, val); err != nil {
					return false, err
				}
				blockKey := db.blockKey(key)
				r, ok := db.timeFilters[blockKey]
				if ok {
					r.timeRecords[timeID] = struct{}{}
				}
				db.internal.meter.Puts.Inc(1)
			}
			// The block's data is in the WAL: a tiny log writing to the
			// block after the reopen writes only what follows.
			block.lastOffset = block.size()
			block.timeRefs = append(block.timeRefs, _TimeID(ID))
			block.Unlock()
			db.internal.timeMark.release(timeID)
			db.internal.meter.Recovers.Inc(int64(len(log)))
		}
		return false, nil
	})
	if err != nil {
		return err
	}

	// Release the blocks with no entries: deleted, or holding deletes only.
	// Their logs go as they would have before the reopen (applyLogs).
	var released []*_Block
	for timeID, block := range db.timeBlocks {
		if block.count > 0 {
			continue
		}
		delete(db.timeBlocks, timeID)
		db.internal.timeMark.timeUnref(timeID)
		block.released = true
		released = append(released, block)
		block.free(db.internal.buffer)
		db.removeTimeFilter(timeID)
	}
	db.internal.logMu.Lock()
	defer db.internal.logMu.Unlock()
	for _, block := range released {
		if err := db.signalApplied(block.applyLogs()); err != nil {
			return err
		}
	}

	return nil
}

// nextEntry returns the entry at off of a block's data, as put writes it:
// its length, its deleted flag, its key, and its value; and the offset of
// the next.
func nextEntry(data []byte, off int) (dBit byte, key uint64, val []byte, next int, err error) {
	const head = 4 + 1 + 8
	if off+4 > len(data) {
		return 0, 0, nil, 0, fmt.Errorf("entry at %d: past the end of %d bytes", off, len(data))
	}
	n := int(binary.LittleEndian.Uint32(data[off : off+4]))
	if n < head || n > len(data)-off {
		return 0, 0, nil, 0, fmt.Errorf("entry at %d: length %d in %d bytes", off, n, len(data))
	}
	e := data[off : off+n : off+n]
	return e[4], binary.LittleEndian.Uint64(e[5:head]), e[head:], off + n, nil
}

// legacyBlock returns the block recovery puts a log of time ID in when the
// log doesn't record its block: writes group by the block duration
// (newTinyLog).
func (db *DB) legacyBlock(timeID _TimeID) _TimeID {
	return _TimeID(time.Unix(0, int64(timeID)).UTC().Truncate(db.opts.timeBlockDuration).UnixNano())
}

// deleteRecovered deletes key from the recovered block timeID, if it holds
// it.
func (db *DB) deleteRecovered(timeID _TimeID, key uint64) {
	block, ok := db.timeBlocks[timeID]
	if !ok {
		return
	}
	block.Lock()
	defer block.Unlock()
	if block.delete(key) == nil {
		db.internal.meter.Dels.Inc(1)
	}
}

// All gets all keys from DB recovered from WAL.
func (db *DB) All(f func(timeID int64, keys []uint64) (bool, error)) (err error) {
	// Get timeIDs of timeBlock successfully committed to WAL.
	timeIDs := db.internal.timeMark.allRefs()
	for _, timeID := range timeIDs {
		db.mu.RLock()
		block, ok := db.timeBlocks[timeID]
		db.mu.RUnlock()
		if !ok {
			continue
		}
		var keys []uint64
		block.RLock()
		for ik := range block.records {
			if ik.delFlag == 0 {
				keys = append(keys, ik.key)
			}
		}
		block.RUnlock()
		if len(keys) == 0 {
			continue
		}
		if stop, err := f(int64(timeID), keys); stop || err != nil {
			return err
		}
	}

	return nil
}
