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
	"io"
	"sort"
	"time"

	"github.com/unit-io/bpool"
)

type windowWriter struct {
	*timeWindowBucket
	winBlocks map[int32]winBlock // map[windowIdx]winBlock

	buffer *bpool.Buffer

	leasing map[int32][]uint64 // map[blockIdx][]seq
}

func newWindowWriter(tw *timeWindowBucket, buf *bpool.Buffer) *windowWriter {
	return &windowWriter{winBlocks: make(map[int32]winBlock), timeWindowBucket: tw, buffer: buf, leasing: make(map[int32][]uint64)}
}

func (wb *windowWriter) del(seq uint64, bIdx int32) error {
	off := int64(blockSize * uint32(bIdx))
	w := windowHandle{file: wb.file, offset: off}
	if err := w.read(); err != nil {
		return err
	}
	entryIdx := -1
	for i := 0; i < int(w.entryIdx); i++ {
		e := w.entries[i]
		if e.sequence == seq { //record exist in db.
			entryIdx = i
			break
		}
	}
	if entryIdx == -1 {
		return nil // no entry in db to delete.
	}
	w.entryIdx--

	i := entryIdx
	for ; i < entriesPerIndexBlock-1; i++ {
		w.entries[i] = w.entries[i+1]
	}
	w.entries[i] = winEntry{}

	return nil
}

// append appends window entries to buffer.
func (wb *windowWriter) append(topicHash uint64, off int64, wEntries windowEntries) (newOff int64, err error) {
	var w winBlock
	var ok bool
	var winIdx int32
	if off == 0 {
		wb.windowIdx++
		winIdx = wb.windowIdx
	} else {
		winIdx = int32(off / int64(blockSize))
	}
	w, ok = wb.winBlocks[winIdx]
	if !ok && off > 0 {
		if winIdx <= wb.windowIdx {
			wh := windowHandle{file: wb.file, offset: off}
			if err := wh.read(); err != nil && err != io.EOF {
				return off, err
			}
			w = wh.winBlock
			w.validation(topicHash)
			w.leased = true
		}
	}
	w.topicHash = topicHash
	for _, we := range wEntries {
		if we.sequence == 0 {
			continue
		}
		entryIdx := 0
		for i := 0; i < seqsPerWindowBlock; i++ {
			e := w.entries[i]
			if e.sequence == we.sequence { //record exist in db.
				entryIdx = -1
				break
			}
		}
		if entryIdx == -1 {
			continue
		}
		if w.entryIdx == seqsPerWindowBlock {
			topicHash := w.topicHash
			next := int64(blockSize * uint32(winIdx))
			// set approximate cutoff on winBlock.
			w.cutoffTime = time.Now().Unix()
			wb.winBlocks[winIdx] = w
			wb.windowIdx++
			winIdx = wb.windowIdx
			w = winBlock{topicHash: topicHash, next: next}
		}
		if w.leased {
			wb.leasing[winIdx] = append(wb.leasing[winIdx], we.sequence)
		}
		w.entries[w.entryIdx] = winEntry{sequence: we.sequence, expiresAt: we.expiresAt}
		w.dirty = true
		w.entryIdx++
	}

	wb.winBlocks[winIdx] = w
	return int64(blockSize * uint32(winIdx)), nil
}

func (wb *windowWriter) write() error {
	for bIdx, w := range wb.winBlocks {
		if !w.leased || !w.dirty {
			continue
		}
		off := int64(blockSize * uint32(bIdx))
		if _, err := wb.WriteAt(w.MarshalBinary(), off); err != nil {
			return err
		}
		w.dirty = false
		wb.winBlocks[bIdx] = w
	}

	// sort blocks by blockIdx.
	var blockIdx []int32
	for bIdx := range wb.winBlocks {
		if wb.winBlocks[bIdx].leased || !wb.winBlocks[bIdx].dirty {
			continue
		}
		blockIdx = append(blockIdx, bIdx)
	}

	sort.Slice(blockIdx, func(i, j int) bool { return blockIdx[i] < blockIdx[j] })

	winBlocks, err := blockRange(blockIdx)
	if err != nil {
		return err
	}
	bufOff := int64(0)
	for _, blocks := range winBlocks {
		if len(blocks) == 1 {
			bIdx := blocks[0]
			off := int64(blockSize * uint32(bIdx))
			w := wb.winBlocks[bIdx]
			buf := w.MarshalBinary()
			if _, err := wb.WriteAt(buf, off); err != nil {
				return err
			}
			w.dirty = false
			wb.winBlocks[bIdx] = w
			continue
		}
		blockOff := int64(blockSize * uint32(blocks[0]))
		for bIdx := blocks[0]; bIdx <= blocks[1]; bIdx++ {
			w := wb.winBlocks[bIdx]
			wb.buffer.Write(w.MarshalBinary())
			w.dirty = false
			wb.winBlocks[bIdx] = w
		}
		blockData, err := wb.buffer.Slice(bufOff, wb.buffer.Size())
		if err != nil {
			return err
		}
		if _, err := wb.WriteAt(blockData, blockOff); err != nil {
			return err
		}
		bufOff = wb.buffer.Size()
	}
	return nil
}

func (wb *windowWriter) rollback() error {
	for bIdx, seqs := range wb.leasing {
		for _, seq := range seqs {
			if err := wb.del(seq, bIdx); err != nil {
				return err
			}
		}
	}
	return nil
}
