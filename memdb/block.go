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

	"github.com/unit-io/bpool"
)

// To avoid lock bottlenecks block cache is divided into several (nShards) shards.
type (
	_TimeID     int64
	_TimeBlocks map[_TimeID]*_Block
)

type (
	// _Key is an internal key that includes deleted flag for the key.
	_Key struct {
		delFlag uint8 // deleted flag
		key     uint64
	}
	_Block struct {
		rwMutex[blockRank] // guards the block but its logs (logMu).
		count              int64
		data               *bpool.Buffer
		records            map[_Key]int64 // map[key]offset

		lastOffset int64 // last offset of block data written to the log

		// The block's logs in the WAL, and what keeps them there; guarded
		// by the DB's logMu, not the block's lock. A log holding the delete
		// of a version must stay in the WAL as long as the log holding its
		// put, or the version comes back on the next recovery: see
		// applyLogs.
		timeRefs []_LogRef
		state    blockState
		deletes  map[*_Block]bool // blocks it deletes versions from
		waiters  []*_Block        // blocks deleting versions from this one
	}

	// _LogRef is a log of a block, and its place in the order logs were
	// written to the WAL, which recovery replays them in.
	_LogRef struct {
		id  _TimeID
		seq uint64
	}
)

// blockState is where a block is in its life, which goes one way:
//
//	blockLive      in timeBlocks: written to while its time ID is current,
//	               then synced, deleted from, or its batch aborted
//	blockReleased  out of timeBlocks, its buffer back in the pool: by the
//	               one that took it out (releaseLog, or recovery); its
//	               logs stay in the WAL while a block deleting from it
//	               holds its own there (applyLogs)
//	blockGone      its logs applied
//
// It is guarded by logMu. setState panics on any other step: a block
// released twice returned its buffer to the pool twice, and two blocks
// then shared it.
type blockState uint8

const (
	blockLive blockState = iota
	blockReleased
	blockGone
)

func (s blockState) String() string {
	return [...]string{"live", "released", "gone"}[s]
}

// setState moves the block to its next state. The caller holds logMu.
func (b *_Block) setState(to blockState) {
	if to != b.state+1 {
		panic(fmt.Sprintf("memdb: a %s block made %s", b.state, to))
	}
	b.state = to
}

// iKey an internal key includes deleted flag.
func iKey(delFlag bool, k uint64) _Key {
	dFlag := uint8(0)
	if delFlag {
		dFlag = 1
	}
	return _Key{delFlag: dFlag, key: k}
}

// get returns a copy of the value at off. The block's buffer goes back to the
// pool when the block is freed, so a slice of it must not outlive the lock.
func (b *_Block) get(off int64) ([]byte, error) {
	if b.data == nil {
		return nil, errEntryDoesNotExist // freed
	}
	scratch, err := b.data.Slice(off, off+4) // read data length.
	if err != nil {
		return nil, err
	}
	dataLen := int64(binary.LittleEndian.Uint32(scratch[:4]))
	data, err := b.data.Slice(off, off+dataLen)
	if err != nil {
		return nil, err
	}
	val := make([]byte, len(data)-(8+1+4))
	copy(val, data[8+1+4:])

	return val, nil
}

// put appends an entry to the block's data, which is what its logs write to
// the WAL, and which recovery reads (nextEntry):
//
//	0   4  the entry's length, these 13 bytes and the value
//	4   1  1 for a delete, 0 for a put
//	5   8  key
//	13  -  value; for a delete, the time ID of the block, 8 bytes, of the
//	       version deleted
func (b *_Block) put(ikey _Key, data []byte) error {
	if b.data == nil {
		return errForbidden // freed
	}
	dataLen := int64(len(data) + 8 + 1 + 4) // data len + key len + flag bit + scratch len
	off, err := b.data.Extend(dataLen)
	if err != nil {
		return err
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(dataLen))
	if _, err := b.data.WriteAt(scratch[:], off); err != nil {
		return err
	}

	// k with flag bit
	var k [9]byte
	k[0] = ikey.delFlag
	binary.LittleEndian.PutUint64(k[1:], ikey.key)
	if _, err := b.data.WriteAt(k[:], off+4); err != nil {
		return err
	}
	if _, err := b.data.WriteAt(data, off+8+1+4); err != nil {
		return err
	}
	// A key put again in the block replaces its record: one record still.
	if _, again := b.records[ikey]; ikey.delFlag == 0 && !again {
		b.count++
	}
	b.records[ikey] = off

	return nil
}

// free returns the block's buffer to the pool and empties the block, so a
// reader or writer still holding the block finds nothing rather than a buffer
// reused by another block. The caller holds the block's write lock.
func (b *_Block) free(pool *bpool.BufferPool) {
	if b.data == nil {
		panic("memdb: a block freed twice")
	}
	pool.Put(b.data)
	b.data = nil
	b.records = nil
	b.count = 0
}

// size returns the size of the block's data; zero once freed.
func (b *_Block) size() int64 {
	if b.data == nil {
		return 0
	}
	return b.data.Size()
}

// delete forgets the key's entry. The entry's data is left as written: it
// may not be in the WAL yet, and must reach it as a put, which the delete
// written after it (DB.move) deletes on recovery. Marking the entry deleted
// wrote a put that recovery took for a delete, and read its value as the
// time ID of a block.
func (b *_Block) delete(key uint64) error {
	ikey := iKey(false, key)
	if _, ok := b.records[ikey]; !ok || b.data == nil {
		return errEntryDoesNotExist
	}

	delete(b.records, ikey)
	b.count--

	return nil
}
