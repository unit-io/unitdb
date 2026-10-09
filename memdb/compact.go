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

// A block's logs stay in the WAL while it holds a value, and so do the logs
// of every block that deletes from it, and of those that delete from them
// (applyLogs): a delete must outlive the put it deletes. A value kept for
// long, a session's row, a message waiting on a subscriber away, then keeps
// the logs of most blocks written after it, in a store that rewrites and
// deletes its keys: a server's message store kept 85,000.
//
// Compact moves the values left in mostly dead blocks to the current block,
// so that those blocks hold none, and go, with the logs that waited on them.
// A block is mostly dead when its values take up at most half its data. A
// mostly live block is moved too once it holds more logs than it has values:
// the logs of the blocks emptied since that delete from it, and from those,
// and so on. Its values may never be replaced, a server's session rows put
// at its start, and moving them costs a write each, which frees a log at
// least. A value moves under its key's index lock, as a put does: a put or
// delete of the key waits, and other writes go on.
//
// A store that keeps a value per entry and frees blocks itself, as the
// engine's does, doesn't compact: it finds an entry by its block.

// Compact moves the values left in mostly dead blocks to the current block,
// and returns how many it moved.
func (db *DB) Compact() (int, error) {
	if err := db.ok(); err != nil {
		return 0, err
	}
	type candidate struct {
		block *_Block
		keys  []uint64
	}
	current := db.timeID()
	var candidates []candidate
	for timeID, b := range db.blocksByID() {
		if timeID == current {
			continue
		}
		b.RLock()
		if b.data == nil || b.count == 0 {
			b.RUnlock()
			continue
		}
		var live int64
		for ik, off := range b.records {
			if ik.delFlag == 0 {
				live += b.entryLen(off)
			}
		}
		move := live*2 <= b.size()
		if !move {
			db.internal.logMu.Lock()
			pinned := b.pinned()
			db.internal.logMu.Unlock()
			move = pinned > int(b.count)
		}
		var keys []uint64
		if move {
			keys = b.liveKeys()
		}
		b.RUnlock()
		if move {
			candidates = append(candidates, candidate{block: b, keys: keys})
		}
	}

	moved := 0
	for _, c := range candidates {
		for _, key := range c.keys {
			ok, err := db.move(key, c.block)
			if err != nil {
				return moved, err
			}
			if ok {
				moved++
			}
		}
	}
	db.internal.meter.Compactions.Inc(1)
	db.internal.meter.Moves.Inc(int64(moved))
	return moved, nil
}

// move moves key's value from the block from to the current block, if it is
// there still, as Put would put it again, and reports whether it did.
func (db *DB) move(key uint64, from *_Block) (bool, error) {
	if err := db.ok(); err != nil {
		return false, err
	}
	db.internal.logManager.rotateMu.RLock()
	defer db.internal.logManager.rotateMu.RUnlock()
	timeID := db.timeID()
	block, ok := db.timeBlock(timeID)
	if !ok {
		return false, errForbidden
	}
	sh := db.index.shard(key)
	sh.Lock()
	loc, had := sh.keys[key]
	if !had || loc.block != from || from == block {
		// Put again, deleted, or moved since it was listed.
		sh.Unlock()
		return false, nil
	}
	from.RLock()
	off, ok := from.records[iKey(false, key)]
	var val []byte
	var err error
	if ok {
		val, err = from.get(off)
	}
	from.RUnlock()
	if !ok || err != nil {
		sh.Unlock()
		return false, err
	}
	emptied, err := db.replace(loc, key)
	if err != nil {
		sh.Unlock()
		return false, err
	}
	delete(sh.keys, key)
	err = db.putEntry(block, key, val)
	if err == nil {
		sh.keys[key] = _Loc{timeID: timeID, block: block}
	}
	sh.Unlock()
	if err != nil {
		return false, err
	}
	return true, db.releaseEmptied(emptied, loc.timeID)
}

// pinned returns the number of logs of released blocks that wait on b,
// through deletes: they stay in the WAL until b goes (applyLogs). The caller
// holds logMu.
func (b *_Block) pinned() int {
	n := 0
	seen := map[*_Block]bool{b: true}
	stack := append([]*_Block(nil), b.waiters...)
	for len(stack) > 0 {
		x := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[x] {
			continue
		}
		seen[x] = true
		if x.state == blockReleased {
			n += len(x.timeRefs)
		}
		stack = append(stack, x.waiters...)
	}
	return n
}

// blocksByID returns the blocks in use, by time ID.
func (db *DB) blocksByID() map[_TimeID]*_Block {
	db.mu.RLock()
	defer db.mu.RUnlock()
	blocks := make(map[_TimeID]*_Block, len(db.timeBlocks))
	for id, b := range db.timeBlocks {
		blocks[id] = b
	}
	return blocks
}

// entryLen returns the length of the entry at off, as put wrote it. The
// caller holds the block's lock.
func (b *_Block) entryLen(off int64) int64 {
	head, err := b.data.Slice(off, off+4)
	if err != nil || len(head) < 4 {
		return 0
	}
	return int64(binary.LittleEndian.Uint32(head))
}
