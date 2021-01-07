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
	"sort"
	"time"

	"github.com/unit-io/unitdb/filter"
)

// delete deletes entry from the DB.
//
// This method is not thread safe.
func (db *DB) delete(key uint64) error {
	// Get time block
	blockKey := db.blockKey(key)
	r, ok := db.timeFilters[blockKey]
	if !ok {
		return errEntryDoesNotExist
	}

	var timeIDs []_TimeID
	r.RLock()
	for timeID := range r.timeRecords {
		timeIDs = append(timeIDs, timeID)
	}
	r.RUnlock()
	sort.Slice(timeIDs[:], func(i, j int) bool {
		return timeIDs[i] > timeIDs[j]
	})
	for _, timeID := range timeIDs {
		block, ok := db.timeBlocks[timeID]
		if ok {
			block.RLock()
			_, ok := block.records[iKey(false, key)]
			block.RUnlock()
			if !ok {
				r.RLock()
				fltr := r.timeRecords[timeID]
				r.RUnlock()
				if !fltr.Test(key) {
					return errEntryDoesNotExist
				}
				continue
			}
			block.Lock()
			ikey := iKey(false, key)
			delete(block.records, ikey)
			block.count--
			db.internal.meter.Dels.Inc(1)
			if len(block.records) == 0 {
				delete(db.timeBlocks, _TimeID(timeID))
				db.internal.buffer.Put(block.data)
			}
			block.Unlock()

			return nil
		}
	}

	return errEntryDoesNotExist
}

// startRecovery recovers pending entries from the WAL.
func (db *DB) startRecovery() error {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// start log recovery
	r, err := db.internal.wal.NewReader()
	if err != nil {
		return err
	}

	delKeys := make(map[_TimeID][]uint64)
	err = r.Read(func(ID int64) (ok bool, err error) {
		log := make(map[uint64][]byte)
		l := r.Count()
		timeID := _TimeID(time.Unix(0, ID).UTC().Truncate(db.opts.logInterval).UnixNano())
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
					if _, ok := delKeys[timeRefID]; ok {
						delKeys[timeRefID] = append(delKeys[timeRefID], key)
					} else {
						delKeys[timeRefID] = []uint64{key}
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
					if _, ok := r.timeRecords[timeID]; !ok {
						r.timeRecords[timeID] = filter.NewFilterBlock(r.filter.Bytes())
					}

					// Append key to bloom filter
					r.filter.Append(key)
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

	for timeID, keys := range delKeys {
		if block, exists := db.timeBlocks[timeID]; exists {
			for _, key := range keys {
				ikey := iKey(false, key)
				block.RLock()
				_, ok := block.records[ikey]
				block.RUnlock()
				if !ok {
					// errEntryDoesNotExist
					continue
				}
				block.Lock()
				delete(block.records, ikey)
				block.count--
				db.internal.meter.Dels.Inc(1)
				if len(block.records) == 0 {
					delete(db.timeBlocks, _TimeID(timeID))
					db.internal.buffer.Put(block.data)
				}
				block.Unlock()
			}
		} else {
			for _, key := range keys {
				db.delete(key)
			}
		}
	}

	return nil
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
