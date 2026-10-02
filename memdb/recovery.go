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
	"time"
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
	// again in that block after it. Blocks a delete empties are freed once
	// every log is read.
	emptied := make(map[_TimeID]bool)
	err = r.Iterator(func(ID int64) (ok bool, err error) {
		log := make(map[uint64][]byte)
		l := r.Count()
		// The block the log was written for: writes group by the block
		// duration too (newTinyLog).
		timeID := _TimeID(time.Unix(0, ID).UTC().Truncate(db.opts.timeBlockDuration).UnixNano())
		for i := uint32(0); i < l; i++ {
			logData, ok, err := r.Next()
			if err != nil {
				return false, err
			}
			if !ok {
				break
			}

			var off int
			for off < len(logData) {
				dataLen := int(binary.LittleEndian.Uint32(logData[off : off+4]))
				data := logData[off+4 : off+dataLen]
				dBit := data[0]
				key := binary.LittleEndian.Uint64(data[1:9])
				val := data[9:]
				off += dataLen
				if dBit == 1 {
					timeRefID := _TimeID(binary.LittleEndian.Uint64(val[:8]))
					if timeRefID == timeID {
						// Put earlier in this log.
						delete(log, key)
					}
					// A block that is not recovered was released: its
					// versions are gone already.
					if db.deleteRecovered(timeRefID, key) {
						emptied[timeRefID] = true
					}
				} else {
					log[key] = val
				}
			}
			db.internal.timeMark.add(timeID)
			block, ok := db.timeBlocks[timeID]
			if !ok {
				block = &_Block{data: db.internal.buffer.Get(), records: make(map[_Key]int64)}
				db.timeBlocks[timeID] = block
			}
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

	for timeID := range emptied {
		block, ok := db.timeBlocks[timeID]
		if !ok || len(block.records) > 0 {
			continue
		}
		delete(db.timeBlocks, timeID)
		block.free(db.internal.buffer)
		db.removeTimeFilter(timeID)
	}

	return nil
}

// deleteRecovered deletes key from the recovered block timeID, if it holds
// it, and reports whether the block was left empty.
func (db *DB) deleteRecovered(timeID _TimeID, key uint64) bool {
	block, ok := db.timeBlocks[timeID]
	if !ok {
		return false
	}
	ikey := iKey(false, key)
	block.Lock()
	defer block.Unlock()
	if _, ok := block.records[ikey]; !ok {
		return false
	}
	delete(block.records, ikey)
	block.count--
	db.internal.meter.Dels.Inc(1)
	return len(block.records) == 0
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
