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
	"sort"
	"time"

	"github.com/unit-io/unitdb/wal"
)

// startRecovery recovers pending entries from the WAL. It runs in Open,
// before any other goroutine has the DB, and takes no db.mu: it held it for
// reading throughout, writing the map of time blocks under it, and took the
// locks below it in the lock order.
func (db *DB) startRecovery() error {

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
		db.internal.timeMark.add(timeID)
		puts := int64(0)
		for i := uint32(0); i < l; i++ {
			logData, ok, err := r.Next()
			if err != nil {
				return false, err
			}
			if !ok {
				break
			}

			// Each entry is applied as written, by the code that wrote it.
			for off := 0; off < len(logData); {
				dBit, key, val, next, err := nextEntry(logData, off)
				if err != nil {
					return false, fmt.Errorf("%w: log %d: %v", wal.ErrCorrupted, ID, err)
				}
				if dBit > 1 {
					return false, fmt.Errorf("%w: log %d: entry at %d has flag %d", wal.ErrCorrupted, ID, off, dBit)
				}
				off = next
				switch {
				case dBit == 0:
					if err := db.recoverPut(block, timeID, key, val); err != nil {
						return false, err
					}
					puts++
				case len(val) != 8:
					// A put marked deleted, as older versions wrote one
					// deleted before it reached the WAL: the delete
					// follows it.
				default:
					fromID := _TimeID(binary.LittleEndian.Uint64(val))
					// A batch's block is named by the time of the batch; a
					// log that doesn't record its block recovers it into
					// the block of its time truncated.
					if _, ok := db.timeBlocks[fromID]; !ok && legacy[db.legacyBlock(fromID)] {
						fromID = db.legacyBlock(fromID)
					}
					// A block that is not recovered was released: its
					// versions are gone already.
					from := db.timeBlocks[fromID]
					_, err := db.deleteEntry(from, fromID, block, key)
					if err == errEntryDoesNotExist {
						_, err = db.deleteEntry(nil, fromID, block, key)
					}
					if err != nil {
						return false, err
					}
					sh := db.index.shard(key)
					if loc, ok := sh.keys[key]; ok && loc.block == from {
						delete(sh.keys, key)
					}
				}
			}
		}
		// The block's data is in the WAL: a tiny log writing to the block
		// after the reopen writes only what follows.
		block.Lock()
		block.lastOffset = block.size()
		block.Unlock()
		block.timeRefs = append(block.timeRefs, db.nextLogRef(_TimeID(ID)))
		db.internal.timeMark.release(timeID)
		db.internal.meter.Recovers.Inc(puts)
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
		block.setState(blockReleased)
		released = append(released, block)
		block.free(db.internal.buffer)
	}
	for timeID := range db.timeBlocks {
		db.recovered = append(db.recovered, timeID)
	}
	sort.Slice(db.recovered, func(i, j int) bool { return db.recovered[i] < db.recovered[j] })
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

// recoverPut replays a put of key in block. A value of key in another block
// was deleted before the put, and the delete written before it: but a WAL
// written when keys had a value in each block they were put in has none,
// and the value is deleted here, the block linked to keep the other's logs
// as long as its own. Recovery runs alone: it takes no shard lock.
func (db *DB) recoverPut(block *_Block, timeID _TimeID, key uint64, val []byte) error {
	sh := db.index.shard(key)
	if loc, ok := sh.keys[key]; ok && loc.block != block {
		loc.block.Lock()
		err := loc.block.delete(key)
		loc.block.Unlock()
		if err != nil && err != errEntryDoesNotExist {
			return err
		}
		db.internal.logMu.Lock()
		block.deleteFrom(loc.block)
		db.internal.logMu.Unlock()
	}
	if err := db.putEntry(block, key, val); err != nil {
		return err
	}
	sh.keys[key] = _Loc{timeID: timeID, block: block}
	return nil
}

// legacyBlock returns the block recovery puts a log of time ID in when the
// log doesn't record its block: writes group by the block duration
// (newTinyLog).
func (db *DB) legacyBlock(timeID _TimeID) _TimeID {
	return _TimeID(time.Unix(0, int64(timeID)).UTC().Truncate(db.opts.timeBlockDuration).UnixNano())
}

// All gets all keys from DB recovered from WAL.
func (db *DB) All(f func(timeID int64, keys []uint64) (bool, error)) (err error) {
	// The blocks recovered: the live tiny log may be writing to one, as
	// after a reopen within its block duration, which Free leaves to it.
	for _, timeID := range db.recovered {
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
