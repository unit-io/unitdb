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
	"sync"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/filter"
)

// To avoid lock bottlenecks block cache is divided into several (nShards) shards.
type (
	_TimeID     int64
	_TimeFilter struct {
		timeRecords map[_TimeID]*filter.Block
		// bloom filter adds keys to the filter for all entries in a time block.
		// filter is checked during get or delete operation
		// to indicate key definitely not exist in the time block.
		filter       *filter.Generator
		sync.RWMutex // Read Write mutex, guards access to internal map.
	}
	_TimeBlocks map[_TimeID]*_Block
)

type (
	// _Key is an internal key that includes deleted flag for the key.
	_Key struct {
		delFlag uint8 // deleted flag
		key     uint64
	}
	_BlockKey uint16
	_Block    struct {
		sync.RWMutex // Read Write mutex, guards access to internal map.
		count        int64
		data         *bpool.Buffer
		records      map[_Key]int64 // map[key]offset

		timeRefs   []_TimeID
		lastOffset int64 // last offset of block data written to the log
	}
)

// iKey an internal key includes deleted flag.
func iKey(delFlag bool, k uint64) _Key {
	dFlag := uint8(0)
	if delFlag {
		dFlag = 1
	}
	return _Key{delFlag: dFlag, key: k}
}

func (b *_Block) get(off int64) ([]byte, error) {
	scratch, err := b.data.Slice(off, off+4) // read data length.
	if err != nil {
		return nil, err
	}
	dataLen := int64(binary.LittleEndian.Uint32(scratch[:4]))
	data, err := b.data.Slice(off, off+dataLen)
	if err != nil {
		return nil, err
	}

	return data[8+1+4:], nil
}

func (b *_Block) put(ikey _Key, data []byte) error {
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
	if ikey.delFlag == 0 {
		b.count++
	}
	b.records[ikey] = off

	return nil
}

func (b *_Block) delete(key uint64) error {
	ikey := iKey(false, key)
	off := b.records[ikey]
	// k with flag bit
	var k [9]byte
	k[0] = 1
	binary.LittleEndian.PutUint64(k[1:], key)
	if _, err := b.data.WriteAt(k[:], off+4); err != nil {
		return err
	}

	delete(b.records, ikey)
	b.count--

	return nil
}
