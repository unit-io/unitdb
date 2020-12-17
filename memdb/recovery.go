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
)

// startRecovery recovers pending entries from the WAL.
func (db *DB) startRecovery() error {
	// start log recovery
	r, err := db.internal.wal.NewReader()
	if err != nil {
		return err
	}

	log := make(map[uint64][]byte)
	err = r.Read(func(ID int64) (ok bool, err error) {
		l := r.Count()
		for i := uint32(0); i < l; i++ {
			logData, ok, err := r.Next()
			if err != nil {
				return false, err
			}
			if !ok {
				break
			}
			dBit := logData[0]
			key := binary.LittleEndian.Uint64(logData[1:9])
			val := logData[9:]
			if dBit == 1 {
				if _, exists := log[key]; exists {
					delete(log, key)
				}
				continue
			}
			log[key] = val
		}
		return false, nil
	})

	db.internal.wal.Reset()

	for k, val := range log {
		if _, err := db.Put(k, val); err != nil {
			return err
		}
	}

	db.internal.meter.Recovers.Inc(int64(len(log)))

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
		db.internal.timeMark.timeUnref(timeID)
	}

	return nil
}
