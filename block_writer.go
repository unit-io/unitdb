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
	"fmt"
	"sort"

	"github.com/unit-io/bpool"
)

type _BlockWriter struct {
	blockIdx    int32
	indexBlocks map[int32]_IndexBlock // map[blockIdx]block

	fs     *_FileSet
	lease  *_Lease
	buffer *bpool.Buffer

	indexLeases                     map[uint64]struct{} //map[seq]struct
	dataLeases                      map[int64]uint32    // map[offset]size
	indexFile, dataFile             *_File
	offset, indexOffset, dataOffset int64
}

func newBlockWriter(fs *_FileSet, lease *_Lease, buf *bpool.Buffer) (*_BlockWriter, error) {
	w := &_BlockWriter{blockIdx: -1, indexBlocks: make(map[int32]_IndexBlock), fs: fs, lease: lease, buffer: buf}
	w.indexLeases = make(map[uint64]struct{})
	w.dataLeases = make(map[int64]uint32)

	indexFile, err := fs.getFile(_FileDesc{fileType: typeIndex})
	if err != nil {
		return nil, err
	}
	w.indexFile = indexFile
	w.indexOffset = indexFile.currSize()
	if w.indexOffset > 0 {
		w.blockIdx = int32(w.indexOffset / int64(blockSize))
		// read final block from index file.
		if w.indexOffset > int64(w.blockIdx*blockSize) {
			r := newBlockReader(w.fs)
			b, err := r.readIndexBlock(w.blockIdx)
			if err != nil {
				return nil, err
			}
			w.indexBlocks[w.blockIdx] = b
		}
	}

	dataFile, err := fs.getFile(_FileDesc{fileType: typeData})
	if err != nil {
		return nil, err
	}
	w.dataFile = dataFile
	w.offset = dataFile.currSize()
	w.dataOffset = dataFile.currSize()
	return w, nil
}

func (w *_BlockWriter) extend(upperSeq uint64) (int64, error) {
	off := blockOffset(blockIndex(upperSeq))
	if off <= w.indexFile.currSize() {
		return w.indexFile.currSize(), nil
	}
	return w.indexFile.extend(uint32(off - w.indexFile.currSize()))
}

func (w *_BlockWriter) del(seq uint64) (_IndexEntry, error) {
	var delEntry _IndexEntry
	bIdx := blockIndex(seq)
	if bIdx > w.blockIdx {
		return delEntry, nil // no entry in db to delete
	}
	r := newBlockReader(w.fs)
	b, err := r.readIndexBlock(bIdx)
	if err != nil {
		return _IndexEntry{}, err
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
	b.dirty = true
	b.entryIdx--

	i := entryIdx
	for ; i < entriesPerIndexBlock-1; i++ {
		b.entries[i] = b.entries[i+1]
	}
	b.entries[i] = _IndexEntry{}
	w.indexBlocks[bIdx] = b

	return delEntry, nil
}

func (w *_BlockWriter) append(e _IndexEntry) (err error) {
	var b _IndexBlock
	var ok bool
	if e.seq == 0 {
		panic("unable to append zero sequence")
	}
	bIdx := blockIndex(e.seq)
	b, ok = w.indexBlocks[bIdx]
	if !ok {
		if bIdx < w.blockIdx {
			r := newBlockReader(w.fs)
			b, err = r.readIndexBlock(bIdx)
			if err != nil {
				return err
			}

			b.leased = true
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
		return errEntryExist
	}

	if len(e.cache) == 0 {
		return errEntryInvalid
	}

	dataLen := len(e.cache)
	off := w.lease.allocate(uint32(dataLen))
	if off != -1 {
		buf := make([]byte, dataLen)
		copy(buf, e.cache)
		if _, err = w.dataFile.WriteAt(buf, off); err != nil {
			return err
		}
		w.dataLeases[off] = uint32(dataLen)
	} else {
		off = w.offset
		offset, err := w.buffer.Extend(int64(dataLen))
		if err != nil {
			return err
		}
		if _, err := w.buffer.WriteAt(e.cache, offset); err != nil {
			return err
		}
		w.offset += int64(dataLen)
	}
	e.msgOffset = off

	if b.leased {
		w.indexLeases[e.seq] = struct{}{}
	}

	b.entries[b.entryIdx] = e
	b.dirty = true
	b.entryIdx++
	if err := b.validation(bIdx); err != nil {
		return err
	}
	w.indexBlocks[bIdx] = b

	return nil
}

func (w *_BlockWriter) write() error {
	// write data blocks
	if _, err := w.dataFile.write(w.buffer.Bytes()); err != nil {
		return err
	}

	// Reset buffer before reusing it.
	w.buffer.Reset()
	for bIdx, b := range w.indexBlocks {
		if !b.leased || !b.dirty {
			continue
		}
		if err := b.validation(bIdx); err != nil {
			return err
		}
		off := blockOffset(bIdx)
		buf := b.MarshalBinary()
		if _, err := w.indexFile.WriteAt(buf, off); err != nil {
			return err
		}
		b.dirty = false
		w.indexBlocks[bIdx] = b
	}

	// sort blocks by blockIdx.
	var blockIdx []int32
	for bIdx := range w.indexBlocks {
		if w.indexBlocks[bIdx].leased || !w.indexBlocks[bIdx].dirty {
			continue
		}
		blockIdx = append(blockIdx, bIdx)
	}
	sort.Slice(blockIdx, func(i, j int) bool { return blockIdx[i] < blockIdx[j] })
	blockRange, err := blockRange(blockIdx)
	if err != nil {
		return err
	}
	bufOff := int64(0)
	for _, blocks := range blockRange {
		if len(blocks) == 1 {
			bIdx := blocks[0]
			off := blockOffset(bIdx)
			b := w.indexBlocks[bIdx]
			if err := b.validation(bIdx); err != nil {
				return err
			}
			buf := b.MarshalBinary()
			if _, err := w.indexFile.WriteAt(buf, off); err != nil {
				return err
			}
			b.dirty = false
			w.indexBlocks[bIdx] = b
			continue
		}
		blockOff := blockOffset(blocks[0])
		for bIdx := blocks[0]; bIdx <= blocks[1]; bIdx++ {
			b := w.indexBlocks[bIdx]
			if err := b.validation(bIdx); err != nil {
				return err
			}
			w.buffer.Write(b.MarshalBinary())
			b.dirty = false
			w.indexBlocks[bIdx] = b
		}
		blockData, err := w.buffer.Slice(bufOff, w.buffer.Size())
		if err != nil {
			return err
		}
		if _, err := w.indexFile.WriteAt(blockData, blockOff); err != nil {
			return err
		}
		bufOff = w.buffer.Size()
	}

	return nil
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

func (w *_BlockWriter) rollback() error {
	// rollback data leases
	for off, size := range w.dataLeases {
		w.lease.freeBlock(off, size)
	}

	// roll back index leases
	for seq := range w.indexLeases {
		if _, err := w.del(seq); err != nil {
			return err
		}
	}
	return nil
}

func (w *_BlockWriter) reset() error {
	w.buffer.Reset()

	w.indexOffset = w.indexFile.currSize()
	w.blockIdx = int32(w.indexOffset / int64(blockSize))

	w.dataOffset = w.dataFile.currSize()

	return nil
}

func (w *_BlockWriter) abort() error {
	w.indexFile.truncate(w.indexOffset)
	w.dataFile.truncate(w.dataOffset)

	return w.rollback()
}
