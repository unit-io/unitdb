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

package unitdb

import (
	"encoding/binary"
	"io"
	"sort"
	"sync"

	"github.com/unit-io/unitdb/hash"
)

type _Leases struct {
	ls           map[uint64]struct{} // map[timeID]map[seq]
	sync.RWMutex                     // Read Write mutex, guards access to internal collection.
}

// A "thread" safe lease freeblocks.
// To avoid lock bottlenecks blocks are divided into several shards (nShards).
type _Lease struct {
	file                  _FileSet
	leases                []*_Leases
	blocks                []*_FreeBlocks
	size                  int64 // Total size of free blocks.
	minimumFreeBlocksSize int64 // Minimum free blocks size before free blocks are reused for new allocation.
	consistent            *hash.Consistent
}

type _FreeBlock struct {
	offset int64
	size   uint32
}

type _FreeBlocks struct {
	fb           []_FreeBlock
	cache        map[int64]bool // cache free offset.
	sync.RWMutex                // Read Write mutex, guards access to internal collection.
}

// newLeaswing creates a new concurrent freeblocks.
func newLease(fs _FileSet, minimumSize int64) *_Lease {
	l := &_Lease{
		file:                  fs,
		leases:                make([]*_Leases, nShards),
		blocks:                make([]*_FreeBlocks, nShards),
		minimumFreeBlocksSize: minimumSize,
		consistent:            hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
		l.leases[i] = &_Leases{ls: make(map[uint64]struct{})}
	}

	for i := 0; i < nShards; i++ {
		l.blocks[i] = &_FreeBlocks{cache: make(map[int64]bool)}
	}

	return l
}

// MarshalBinary serialized leased blocks into binary data.
func (b *_FreeBlocks) MarshalBinary() []byte {
	size := 4 + (12 * len(b.fb))
	buf := make([]byte, size)
	data := buf
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(b.fb)))
	buf = buf[4:]
	for i := 0; i < len(b.fb); i++ {
		binary.LittleEndian.PutUint64(buf[:8], uint64(b.fb[i].offset))
		binary.LittleEndian.PutUint32(buf[8:12], b.fb[i].size)
		buf = buf[12:]
	}
	return data
}

// UnmarshalBinary de-serialized leased blocks from binary data.
func (b *_FreeBlocks) UnmarshalBinary(data []byte, size uint32) error {
	for i := uint32(0); i < size; i++ {
		// _ = data[12] // bounds check hint to compiler; see golang.org/issue/14808.
		blockOff := int64(binary.LittleEndian.Uint64(data[:8]))
		blockSize := binary.LittleEndian.Uint32(data[8:12])
		if blockOff != 0 {
			b.fb = append(b.fb, _FreeBlock{size: blockSize, offset: blockOff})
		}
		data = data[12:]
	}
	return nil
}

// leaseBlock returns leases under given blockID.
func (l *_Lease) leaseBlock(blockID uint64) *_Leases {
	return l.leases[l.consistent.FindBlock(blockID)]
}

// freeBlocks returns freeBlocks under given blockID.
func (l *_Lease) freeBlocks(blockID uint64) *_FreeBlocks {
	return l.blocks[l.consistent.FindBlock(blockID)]
}

func (b *_FreeBlocks) search(size uint32) int {
	return sort.Search(len(b.fb), func(i int) bool {
		return b.fb[i].size >= size
	})
}

func (b *_FreeBlocks) len() int {
	return len(b.fb)
}

func (b *_FreeBlocks) defrag() {
	l := len(b.fb)
	if l <= 1 {
		return
	}
	sort.Slice(b.fb[:l], func(i, j int) bool {
		return b.fb[i].offset < b.fb[j].offset
	})
	var merged []_FreeBlock
	curOff := b.fb[0].offset
	curSize := b.fb[0].size
	for i := 1; i < l; i++ {
		if curOff+int64(curSize) == b.fb[i].offset {
			curSize += b.fb[i].size
			delete(b.cache, b.fb[i].offset)
		} else {
			merged = append(merged, _FreeBlock{size: curSize, offset: curOff})
			curOff = b.fb[i].offset
			curSize = b.fb[i].size
		}
	}
	merged = append(merged, _FreeBlock{offset: curOff, size: curSize})
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].size < merged[j].size
	})
	copy(b.fb[:l], merged)
}

func (l *_Lease) defrag() {
	for i := 0; i < nShards; i++ {
		fbs := l.blocks[i]
		fbs.defrag()
	}
}

func (l *_Lease) freeBlock(off int64, size uint32) {
	fbs := l.freeBlocks(uint64(off))
	fbs.Lock()
	defer fbs.Unlock()
	// Verify that block is not already free.
	if fbs.cache[off] {
		return
	}
	fbs.fb = append(fbs.fb, _FreeBlock{offset: off, size: size})
	fbs.cache[off] = true
	l.size += int64(size)
}

func (l *_Lease) free(seq uint64, off int64, size uint32) {
	if size == 0 {
		panic("unable to free zero bytes")
	}
	l.freeBlock(off, size)
}

func (l *_Lease) allocate(size uint32) int64 {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	if l.size < l.minimumFreeBlocksSize {
		return -1
	}
	fbs := l.freeBlocks(uint64(size))
	fbs.Lock()
	defer fbs.Unlock()
	i := fbs.search(size)
	if i >= len(fbs.fb) {
		return -1
	}
	off := fbs.fb[i].offset
	if fbs.fb[i].size == size {
		copy(fbs.fb[i:], fbs.fb[i+1:])
		fbs.fb[len(fbs.fb)-1] = _FreeBlock{}
		fbs.fb = fbs.fb[:len(fbs.fb)-1]
	} else {
		fbs.fb[i].size -= size
		fbs.fb[i].offset += int64(size)
	}
	delete(fbs.cache, off)
	l.size -= int64(size)
	return off
}

func (l *_Lease) read() error {
	off := int64(0)
	blocks := &_FreeBlocks{cache: make(map[int64]bool)}
	buf := make([]byte, 4)
	if _, err := l.file.ReadAt(buf, off); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	size := binary.LittleEndian.Uint32(buf)
	off += 4
	buf = make([]byte, 12*size)
	if _, err := l.file.ReadAt(buf, off); err != nil {
		return err
	}
	blocks.UnmarshalBinary(buf, size)

	for _, b := range blocks.fb {
		l.freeBlock(b.offset, b.size)
	}

	return nil
}

func (l *_Lease) write() error {
	if len(l.blocks) == 0 {
		return nil
	}
	if err := l.file.Truncate(0); err != nil {
		return err
	}
	var off int64
	blocks := &_FreeBlocks{cache: make(map[int64]bool)}
	for i := 0; i < nShards; i++ {
		fbs := l.blocks[i]
		if fbs.len() == 0 {
			continue
		}
		blocks.fb = append(blocks.fb, fbs.fb...)
	}

	data := blocks.MarshalBinary()
	if _, err := l.file.WriteAt(data, off); err != nil {
		return err
	}

	return nil
}
