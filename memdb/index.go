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

// A key has one value: the index gives the block holding it. Put of a key
// another block holds deletes that version first, writing the delete to
// the WAL, and Delete deletes the key. A key had a version in each block it
// was put in, Get returned the newest block's, and Delete deleted only
// that: callers deleted every version before each put, and a store that
// wrote tombstones instead never freed a block.
//
// The index is sharded by key. A shard's lock is held for writing across a
// put or delete of its keys, which it serializes, and for reading across a
// get; it is taken before the locks of blocks (see the lock order).

const nIndexShards = 64

// _Loc is where a key's value is: its block, and the block's time ID. The
// block tells a block released and replaced under the same time ID, as the
// live tiny log's is, from the new one.
type _Loc struct {
	timeID _TimeID
	block  *_Block
}

type _IndexShard struct {
	rwMutex[indexRank]
	keys map[uint64]_Loc
}

type _Index struct {
	shards [nIndexShards]_IndexShard
}

func newIndex() *_Index {
	ix := &_Index{}
	for i := range ix.shards {
		ix.shards[i].keys = make(map[uint64]_Loc)
	}
	return ix
}

// shard returns the key's shard. Keys are often a counter above a fixed
// low half, as message logs' are: they are mixed first.
func (ix *_Index) shard(key uint64) *_IndexShard {
	return &ix.shards[(key*0x9E3779B97F4A7C15)>>58]
}

// forget removes the keys block holds from the index, once the block is
// released: a key whose value is elsewhere since stays.
func (ix *_Index) forget(block *_Block, keys []uint64) {
	for _, key := range keys {
		sh := ix.shard(key)
		sh.Lock()
		if loc, ok := sh.keys[key]; ok && loc.block == block {
			delete(sh.keys, key)
		}
		sh.Unlock()
	}
}

// liveKeys returns the keys the block holds a value of. The caller holds
// the block's lock.
func (b *_Block) liveKeys() []uint64 {
	keys := make([]uint64, 0, b.count)
	for ik := range b.records {
		if ik.delFlag == 0 {
			keys = append(keys, ik.key)
		}
	}
	return keys
}
