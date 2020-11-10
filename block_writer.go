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
	blocks map[int32]_Block // map[blockIdx]block

	file   *_File
	buffer *bpool.Buffer

	leasing map[uint64]struct{}
}

func newBlockWriter(f *_File, buf *bpool.Buffer) *_BlockWriter {
	return &_BlockWriter{blocks: make(map[int32]_Block), file: f, buffer: buf, leasing: make(map[uint64]struct{})}
}

func (w *_BlockWriter) del(seq uint64) (_Slot, error) {
	var delEntry _Slot
	startBlockIdx := startBlockIndex(seq)
	r := newBlockReader(w.file)
	b, err := r.readBlock(seq)
	if err != nil {
		return _Slot{}, err
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
	b.entries[i] = _Slot{}
	w.blocks[startBlockIdx] = b

	return delEntry, nil
}

func (w *_BlockWriter) append(s _Slot, blockIdx int32) (exists bool, err error) {
	var b _Block
	var ok bool
	if s.seq == 0 {
		panic("unable to append zero sequence")
	}
	startBlockIdx := startBlockIndex(s.seq)
	b, ok = w.blocks[startBlockIdx]
	if !ok {
		if startBlockIdx <= blockIdx {
			r := newBlockReader(w.file)
			b, err := r.readBlock(s.seq)
			if err != nil {
				return false, err
			}

			b.leased = true
		}
	}
	entryIdx := 0
	for i := 0; i < int(b.entryIdx); i++ {
		if b.entries[i].seq == s.seq { //record exist in db
			entryIdx = -1
			break
		}
	}
	if entryIdx == -1 {
		return true, nil
	}

	if b.leased {
		w.leasing[s.seq] = struct{}{}
	}
	b.entries[b.entryIdx] = s
	b.dirty = true
	b.entryIdx++
	if err := b.validation(startBlockIdx); err != nil {
		return false, err
	}
	w.blocks[startBlockIdx] = b

	return false, nil
}

func (w *_BlockWriter) write() error {
	for bIdx, b := range w.blocks {
		if !b.leased || !b.dirty {
			continue
		}
		if err := b.validation(bIdx); err != nil {
			return err
		}
		off := blockOffset(bIdx)
		buf := b.MarshalBinary()
		if _, err := w.file.WriteAt(buf, off); err != nil {
			return err
		}
		b.dirty = false
		w.blocks[bIdx] = b
	}

	// sort blocks by blockIdx.
	var blockIdx []int32
	for bIdx := range w.blocks {
		if w.blocks[bIdx].leased || !w.blocks[bIdx].dirty {
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
			b := w.blocks[bIdx]
			if err := b.validation(bIdx); err != nil {
				return err
			}
			buf := b.MarshalBinary()
			if _, err := w.file.WriteAt(buf, off); err != nil {
				return err
			}
			b.dirty = false
			w.blocks[bIdx] = b
			continue
		}
		blockOff := blockOffset(blocks[0])
		for bIdx := blocks[0]; bIdx <= blocks[1]; bIdx++ {
			b := w.blocks[bIdx]
			if err := b.validation(bIdx); err != nil {
				return err
			}
			w.buffer.Write(b.MarshalBinary())
			b.dirty = false
			w.blocks[bIdx] = b
		}
		blockData, err := w.buffer.Slice(bufOff, w.buffer.Size())
		if err != nil {
			return err
		}
		if _, err := w.file.WriteAt(blockData, blockOff); err != nil {
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
	for seq := range w.leasing {
		if _, err := w.del(seq); err != nil {
			return err
		}
	}
	return nil
}
