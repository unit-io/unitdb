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
	"fmt"
	"sort"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/fs"
)

const (
	entrySize        = 26
	blockSize uint32 = 4096
)

type (
	entry struct {
		seq       uint64
		topicSize uint16
		valueSize uint32
		msgOffset int64
		expiresAt uint32 // expiresAt for recovery from log and not persisted to Index file but persisted to the time window file

		topicHash  uint64 // topicHash for recovery from log and not persisted to the DB
		cacheBlock []byte // block from memdb if it exist
	}

	block struct {
		entries  [entriesPerIndexBlock]entry
		baseSeq  uint64
		next     uint32
		entryIdx uint16

		dirty  bool
		leased bool
	}

	blockHandle struct {
		block
		file   fs.FileManager
		offset int64
	}

	blockWriter struct {
		upperSeq uint64
		blocks   map[int32]block // map[blockIdx]block

		*file
		buffer *bpool.Buffer

		leasing map[uint64]struct{}
	}
)

func newBlockWriter(f *file, buf *bpool.Buffer) *blockWriter {
	return &blockWriter{blocks: make(map[int32]block), file: f, buffer: buf, leasing: make(map[uint64]struct{})}
}

func (e entry) ExpiresAt() uint32 {
	return e.expiresAt
}

func startBlockIndex(seq uint64) int32 {
	return int32(float64(seq-1) / float64(entriesPerIndexBlock))
}

func blockOffset(idx int32) int64 {
	if idx == -1 {
		return int64(headerSize)
	}
	return int64(headerSize + (blockSize * uint32(idx)))
}

func (e entry) mSize() uint32 {
	return idSize + uint32(e.topicSize) + e.valueSize
}

// MarshalBinary serialized entry into binary data
func (e entry) MarshalBinary() ([]byte, error) {
	buf := make([]byte, entrySize)
	data := buf
	binary.LittleEndian.PutUint64(buf[:8], e.seq)
	binary.LittleEndian.PutUint16(buf[8:10], e.topicSize)
	binary.LittleEndian.PutUint32(buf[10:14], e.valueSize)
	binary.LittleEndian.PutUint32(buf[14:18], e.expiresAt)
	binary.LittleEndian.PutUint64(buf[18:26], e.topicHash)
	return data, nil
}

// MarshalBinary de-serialized entry from binary data
func (e *entry) UnmarshalBinary(data []byte) error {
	e.seq = binary.LittleEndian.Uint64(data[:8])
	e.topicSize = binary.LittleEndian.Uint16(data[8:10])
	e.valueSize = binary.LittleEndian.Uint32(data[10:14])
	e.expiresAt = binary.LittleEndian.Uint32(data[14:18])
	e.topicHash = binary.LittleEndian.Uint64(data[18:26])
	return nil
}

func (b block) validation(blockIdx int32) error {
	startBlockIdx := startBlockIndex(b.entries[0].seq)
	if startBlockIdx != blockIdx {
		return fmt.Errorf("validation failed blockIdx %d, startBlockIdx %d", blockIdx, startBlockIdx)
	}
	return nil
}

// MarshalBinary serialized entries block into binary data
func (b block) MarshalBinary() []byte {
	buf := make([]byte, blockSize)
	data := buf

	baseSeq := b.entries[0].seq
	binary.LittleEndian.PutUint64(buf[:8], baseSeq)
	buf = buf[8:]
	for i := 0; i < entriesPerIndexBlock; i++ {
		e := b.entries[i]
		if e.seq == 0 {
			binary.LittleEndian.PutUint16(buf[:2], uint16(e.seq)) // marshal relative seq
		} else {
			binary.LittleEndian.PutUint16(buf[:2], uint16(int16(e.seq-baseSeq)+255)) // marshal relative seq
		}
		binary.LittleEndian.PutUint16(buf[2:4], e.topicSize)
		binary.LittleEndian.PutUint32(buf[4:8], e.valueSize)
		binary.LittleEndian.PutUint64(buf[8:16], uint64(e.msgOffset))
		buf = buf[16:]
	}
	binary.LittleEndian.PutUint32(buf[:4], b.next)
	binary.LittleEndian.PutUint16(buf[4:6], b.entryIdx)
	return data
}

// UnmarshalBinary de-serialized entries block from binary data
func (b *block) UnmarshalBinary(data []byte) error {
	b.baseSeq = binary.LittleEndian.Uint64(data[:8])
	data = data[8:]
	for i := 0; i < entriesPerIndexBlock; i++ {
		_ = data[16] // bounds check hint to compiler; see golang.org/issue/14808
		seq := int16(binary.LittleEndian.Uint16(data[:2]))
		if seq == 0 {
			b.entries[i].seq = uint64(seq)
		} else {
			b.entries[i].seq = b.baseSeq + uint64(seq) - 255 // unmarshal from relative sequence
		}
		b.entries[i].topicSize = binary.LittleEndian.Uint16(data[2:4])
		b.entries[i].valueSize = binary.LittleEndian.Uint32(data[4:8])
		b.entries[i].msgOffset = int64(binary.LittleEndian.Uint64(data[8:16]))
		data = data[16:]
	}
	b.next = binary.LittleEndian.Uint32(data[:4])
	b.entryIdx = binary.LittleEndian.Uint16(data[4:6])
	return nil
}

func (bh *blockHandle) read() error {
	buf, err := bh.file.Slice(bh.offset, bh.offset+int64(blockSize))
	if err != nil {
		return err
	}
	return bh.UnmarshalBinary(buf)
}

func (bw *blockWriter) del(seq uint64) (entry, error) {
	var delEntry entry
	blockIdx := startBlockIndex(seq)
	off := blockOffset(blockIdx)
	b := blockHandle{file: bw.file, offset: off}
	if err := b.read(); err != nil {
		return delEntry, err
	}
	entryIdx := -1
	for i := 0; i < int(b.entryIdx); i++ {
		e := b.entries[i]
		if e.seq == seq { //record exist in db
			entryIdx = i
			break
		}
	}
	if entryIdx == -1 {
		return delEntry, nil // no entry in db to delete
	}
	delEntry = b.entries[entryIdx]
	b.entryIdx--

	i := entryIdx
	for ; i < entriesPerIndexBlock-1; i++ {
		b.entries[i] = b.entries[i+1]
	}
	b.entries[i] = entry{}

	return delEntry, nil
}

func (bw *blockWriter) append(e entry, blockIdx int32) (exists bool, err error) {
	var b block
	var ok bool
	if e.seq == 0 {
		panic("unable to append zero sequence")
	}
	startBlockIdx := startBlockIndex(e.seq)
	b, ok = bw.blocks[startBlockIdx]
	if !ok {
		if startBlockIdx <= blockIdx {
			off := blockOffset(startBlockIdx)
			bh := blockHandle{file: bw.file, offset: off}
			if err := bh.read(); err != nil {
				return false, err
			}
			b = bh.block

			b.leased = true
		}
		if b.baseSeq == 0 {
			b.baseSeq = e.seq
		}
	}
	entryIdx := 0
	for i := 0; i < int(b.entryIdx); i++ {
		if b.entries[i].seq == e.seq { //record exist in db
			entryIdx = -1
			break
		}
	}
	if entryIdx == -1 {
		return true, nil
	}
	if bw.upperSeq < e.seq {
		bw.upperSeq = e.seq
	}

	if b.leased {
		bw.leasing[e.seq] = struct{}{}
	}
	if b.entryIdx > entriesPerIndexBlock-1 {
		var seqs []uint64
		for _, e := range b.entries {
			seqs = append(seqs, e.seq)
		}
		fmt.Println("blockWriter.append: seq, startBlockIdx, baseSeq, block ", e.seq, startBlockIdx, b.baseSeq, seqs)
	}
	b.entries[b.entryIdx] = e
	b.dirty = true
	b.entryIdx++
	if err := b.validation(startBlockIdx); err != nil {
		fmt.Println("block.append: seq, baseSeq  ", e.seq, b.entries[0].seq, b.baseSeq)
		return false, err
	}
	bw.blocks[startBlockIdx] = b

	return false, nil
}

func (bw *blockWriter) write() error {
	// var leasedBlocks []int32
	for bIdx, b := range bw.blocks {
		if !b.leased || !b.dirty {
			continue
		}
		if err := b.validation(bIdx); err != nil {
			return err
		}
		off := blockOffset(bIdx)
		buf := b.MarshalBinary()
		if _, err := bw.WriteAt(buf, off); err != nil {
			return err
		}
		b.dirty = false
		bw.blocks[bIdx] = b
		// leasedBlocks = append(leasedBlocks, bIdx)
	}

	// sort blocks by blockIdx
	var blockIdx []int32
	for bIdx := range bw.blocks {
		if bw.blocks[bIdx].leased || !bw.blocks[bIdx].dirty {
			continue
		}
		blockIdx = append(blockIdx, bIdx)
	}
	sort.Slice(blockIdx, func(i, j int) bool { return blockIdx[i] < blockIdx[j] })
	blockRange, err := blockRange(blockIdx)
	if err != nil {
		return err
	}
	// fmt.Println("block.write: leasedBlocks, blockRange ", leasedBlocks, blockRange)
	bufOff := int64(0)
	for _, blocks := range blockRange {
		if len(blocks) == 1 {
			bIdx := blocks[0]
			off := blockOffset(bIdx)
			b := bw.blocks[bIdx]
			if err := b.validation(bIdx); err != nil {
				return err
			}
			buf := b.MarshalBinary()
			if _, err := bw.WriteAt(buf, off); err != nil {
				return err
			}
			b.dirty = false
			bw.blocks[bIdx] = b
			continue
		}
		blockOff := blockOffset(blocks[0])
		// bw.buffer.Reset()
		for bIdx := blocks[0]; bIdx <= blocks[1]; bIdx++ {
			b := bw.blocks[bIdx]
			if err := b.validation(bIdx); err != nil {
				return err
			}
			bw.buffer.Write(b.MarshalBinary())
			b.dirty = false
			bw.blocks[bIdx] = b
		}
		blockData, err := bw.buffer.Slice(bufOff, bw.buffer.Size())
		if err != nil {
			return err
		}
		if _, err := bw.WriteAt(blockData, blockOff); err != nil {
			return err
		}
		bufOff = bw.buffer.Size()
	}

	return nil
}

func (bw *blockWriter) UpperSeq() uint64 {
	return bw.upperSeq
}

func blockRange(idx []int32) ([][]int32, error) {
	if len(idx) == 0 {
		return nil, nil
	}
	var parts [][]int32
	for n1 := 0; ; {
		n2 := n1 + 1
		for n2 < len(idx) && idx[n2] == idx[n2-1]+1 {
			n2++
		}
		s := []int32{(idx[n1])}
		if n2 == n1+2 {
			parts = append(parts, []int32{idx[n2-1]})
		} else if n2 > n1+2 {
			s = append(s, idx[n2-1])
		}
		parts = append(parts, s)
		if n2 == len(idx) {
			break
		}
		if idx[n2] == idx[n2-1] {
			return nil, fmt.Errorf("sequence repeats value %d", idx[n2])
		}
		if idx[n2] < idx[n2-1] {
			return nil, fmt.Errorf("sequence not ordered: %d < %d", idx[n2], idx[n2-1])
		}
		n1 = n2
	}
	return parts, nil
}

func (bw *blockWriter) rollback() error {
	for seq := range bw.leasing {
		if _, err := bw.del(seq); err != nil {
			return err
		}
	}
	return nil
}
