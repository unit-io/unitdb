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

type freeslots struct {
	cache        map[uint64]bool // map[seq]bool.
	fs           []uint64
	sync.RWMutex // Read Write mutex, guards access to internal collection.
}

// A "thread" safe lease freeblocks.
// To avoid lock bottlenecks slots are divided into several shards (nShards).
type lease struct {
	file
	slots                 []*freeslots
	blocks                []*freeBlocks
	size                  int64 // Total size of free blocks.
	minimumFreeBlocksSize int64 // Minimum free blocks size before free blocks are reused for new allocation.
	consistent            *hash.Consistent
}

type freeblock struct {
	offset int64
	size   uint32
}

type freeBlocks struct {
	fb           []freeblock
	cache        map[int64]bool // cache free offset.
	sync.RWMutex                // Read Write mutex, guards access to internal collection.
}

// newLeaswing creates a new concurrent freeblocks.
func newLease(f file, minimumSize int64) *lease {
	l := &lease{
		file:                  f,
		slots:                 make([]*freeslots, nShards),
		blocks:                make([]*freeBlocks, nShards),
		minimumFreeBlocksSize: minimumSize,
		consistent:            hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
		l.slots[i] = &freeslots{cache: make(map[uint64]bool)}
	}

	for i := 0; i < nShards; i++ {
		l.blocks[i] = &freeBlocks{cache: make(map[int64]bool)}
	}

	return l
}

func align4096(n int) int {
	return (n + 4095) &^ 4095
}

// MarshalBinary serialized leased slots into binary data.
func (s *freeslots) MarshalBinary() []byte {
	size := 4 + (8 * len(s.fs))
	buf := make([]byte, size)
	data := buf
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(s.fs)))
	buf = buf[4:]
	for _, seq := range s.fs {
		binary.LittleEndian.PutUint64(buf[:8], seq)
		buf = buf[8:]
	}
	return data
}

// UnmarshalBinary de-serialized leased slots from binary data.
func (s *freeslots) UnmarshalBinary(data []byte, size uint32) error {
	for i := uint32(0); i < size; i++ {
		// _ = data[8] // bounds check hint to compiler; see golang.org/issue/14808.
		seq := binary.LittleEndian.Uint64(data[:8])
		if seq != 0 {
			s.cache[seq] = true
			s.fs = append(s.fs, seq)
		}
		data = data[8:]
	}
	return nil
}

// MarshalBinary serialized leased blocks into binary data.
func (s *freeBlocks) MarshalBinary() []byte {
	size := 4 + (12 * len(s.fb))
	buf := make([]byte, size)
	data := buf
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(s.fb)))
	buf = buf[4:]
	for i := 0; i < len(s.fb); i++ {
		binary.LittleEndian.PutUint64(buf[:8], uint64(s.fb[i].offset))
		binary.LittleEndian.PutUint32(buf[8:12], s.fb[i].size)
		buf = buf[12:]
	}
	return data
}

// UnmarshalBinary de-serialized leased blocks from binary data.
func (s *freeBlocks) UnmarshalBinary(data []byte, size uint32) error {
	for i := uint32(0); i < size; i++ {
		// _ = data[12] // bounds check hint to compiler; see golang.org/issue/14808.
		blockOff := int64(binary.LittleEndian.Uint64(data[:8]))
		blockSize := binary.LittleEndian.Uint32(data[8:12])
		if blockOff != 0 {
			s.fb = append(s.fb, freeblock{size: blockSize, offset: blockOff})
		}
		data = data[12:]
	}
	return nil
}

// freeSlots returns freeSlots under given blockID.
func (l *lease) freeSlots(blockID uint64) *freeslots {
	return l.slots[l.consistent.FindBlock(blockID)]
}

// getSlot gets seq from free slot.
func (l *lease) getSlot() (ok bool, seq uint64) {
	// Get shard.
	for i := uint64(0); i < nShards; i++ {
		fss := l.slots[i]
		fss.Lock()
		if len(fss.fs) == 0 {
			fss.Unlock()
			continue
		}
		seq := fss.fs[0]
		delete(fss.cache, seq)
		fss.fs = fss.fs[1:]
		fss.Unlock()
		return true, seq
	}
	return false, seq
}

func (l *lease) freeSlot(seq uint64) (ok bool) {
	// Get shard.
	fss := l.freeSlots(seq)
	fss.Lock()
	defer fss.Unlock()
	if ok := fss.cache[seq]; ok {
		return !ok
	}
	fss.cache[seq] = true
	fss.fs = append(fss.fs, seq)
	return true
}

func (fs *freeslots) len() int {
	return len(fs.fs)
}

// freeBlocks returns freeBlocks under given blockID.
func (l *lease) freeBlocks(blockID uint64) *freeBlocks {
	return l.blocks[l.consistent.FindBlock(blockID)]
}

func (s *freeBlocks) search(size uint32) int {
	return sort.Search(len(s.fb), func(i int) bool {
		return s.fb[i].size >= size
	})
}

func (fs *freeBlocks) len() int {
	return len(fs.fb)
}

func (s *freeBlocks) defrag() {
	l := len(s.fb)
	if l <= 1 {
		return
	}
	sort.Slice(s.fb[:l], func(i, j int) bool {
		return s.fb[i].offset < s.fb[j].offset
	})
	var merged []freeblock
	curOff := s.fb[0].offset
	curSize := s.fb[0].size
	for i := 1; i < l; i++ {
		if curOff+int64(curSize) == s.fb[i].offset {
			curSize += s.fb[i].size
			delete(s.cache, s.fb[i].offset)
		} else {
			merged = append(merged, freeblock{size: curSize, offset: curOff})
			curOff = s.fb[i].offset
			curSize = s.fb[i].size
		}
	}
	merged = append(merged, freeblock{offset: curOff, size: curSize})
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].size < merged[j].size
	})
	copy(s.fb[:l], merged)
}

func (l *lease) defrag() {
	for i := 0; i < nShards; i++ {
		fbs := l.blocks[i]
		fbs.defrag()
	}
}

func (l *lease) freeBlock(off int64, size uint32) {
	fbs := l.freeBlocks(uint64(off))
	fbs.Lock()
	defer fbs.Unlock()
	// Verify that block is not already free.
	if fbs.cache[off] {
		return
	}
	fbs.fb = append(fbs.fb, freeblock{offset: off, size: size})
	fbs.cache[off] = true
	l.size += int64(size)
}

func (l *lease) free(seq uint64, off int64, size uint32) {
	if size == 0 {
		panic("unable to free zero bytes")
	}
	l.freeSlot(seq)
	l.freeBlock(off, size)
}

func (l *lease) allocate(size uint32) int64 {
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
		fbs.fb[len(fbs.fb)-1] = freeblock{}
		fbs.fb = fbs.fb[:len(fbs.fb)-1]
	} else {
		fbs.fb[i].size -= size
		fbs.fb[i].offset += int64(size)
	}
	delete(fbs.cache, off)
	l.size -= int64(size)
	return off
}

func (l *lease) read() error {
	off := int64(0)
	slots := &freeslots{cache: make(map[uint64]bool)}
	buf := make([]byte, 4)
	if _, err := l.ReadAt(buf, off); err != nil {
		if err == io.EOF {
			return nil
		}
		return err
	}
	size := binary.LittleEndian.Uint32(buf)
	off += 4
	buf = make([]byte, 8*size)
	if _, err := l.ReadAt(buf, off); err != nil {
		return err
	}
	off += int64(8 * size)
	slots.UnmarshalBinary(buf, size)

	for _, seq := range slots.fs {
		l.freeSlot(seq)
	}

	blocks := &freeBlocks{cache: make(map[int64]bool)}
	buf = make([]byte, 4)
	if _, err := l.ReadAt(buf, off); err != nil {
		return err
	}
	size = binary.LittleEndian.Uint32(buf)
	off += 4
	buf = make([]byte, 12*size)
	if _, err := l.ReadAt(buf, off); err != nil {
		return err
	}
	blocks.UnmarshalBinary(buf, size)

	for _, b := range blocks.fb {
		l.freeBlock(b.offset, b.size)
	}

	return nil
}

func (l *lease) write() error {
	if len(l.blocks) == 0 {
		return nil
	}
	if err := l.Truncate(0); err != nil {
		return err
	}
	var off int64
	slots := &freeslots{cache: make(map[uint64]bool)}
	for i := 0; i < nShards; i++ {
		fss := l.slots[i]
		if fss.len() == 0 {
			continue
		}
		slots.fs = append(slots.fs, fss.fs...)
	}

	data := slots.MarshalBinary()
	n, err := l.WriteAt(data, off)
	if err != nil {
		return err
	}
	off += int64(n)

	blocks := &freeBlocks{cache: make(map[int64]bool)}
	for i := 0; i < nShards; i++ {
		fbs := l.blocks[i]
		if fbs.len() == 0 {
			continue
		}
		blocks.fb = append(blocks.fb, fbs.fb...)
	}

	data = blocks.MarshalBinary()
	if _, err = l.WriteAt(data, off); err != nil {
		return err
	}

	return nil
}
