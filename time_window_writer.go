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
	"sort"
	"time"

	"github.com/unit-io/bpool"
)

type _WindowWriter struct {
	windowIdx int32
	winBlocks map[int32]_WinBlock // map[windowIdx]winBlock
	winLeases map[int32][]uint64  // map[blockIdx][]seq

	file      *_File
	buffer    *bpool.Buffer
	winOffset int64
}

func newWindowWriter(f *_File, buf *bpool.Buffer) *_WindowWriter {
	w := &_WindowWriter{windowIdx: -1, winBlocks: make(map[int32]_WinBlock), winLeases: make(map[int32][]uint64), file: f, buffer: buf}
	w.winOffset = f.currSize()
	if w.winOffset > 0 {
		w.windowIdx = int32(w.winOffset / int64(blockSize))
	}

	return w
}

func (w *_WindowWriter) del(seq uint64, winIdx int32) error {
	off := int64(blockSize * winIdx)
	r := newWindowReader(w.file)
	b, err := r.readBlock(off)
	if err != nil {
		return err
	}
	entryIdx := -1
	for i := 0; i < int(b.entryIdx); i++ {
		e := b.entries[i]
		if e.sequence == seq { //record exist in db.
			entryIdx = i
			break
		}
	}
	if entryIdx == -1 {
		return nil // no entry in db to delete.
	}
	b.dirty = true
	b.entryIdx--

	i := entryIdx
	for ; i < entriesPerIndexBlock-1; i++ {
		b.entries[i] = b.entries[i+1]
	}
	b.entries[i] = _WinEntry{}

	w.winBlocks[winIdx] = b
	return nil
}

// append appends window entries to buffer.
func (w *_WindowWriter) append(topicHash uint64, off int64, wEntries _WindowEntries) (newOff int64, err error) {
	var b _WinBlock
	var ok bool
	var wIdx int32
	if off == 0 {
		w.windowIdx++
		wIdx = w.windowIdx
	} else {
		wIdx = int32(off / int64(blockSize))
	}
	b, ok = w.winBlocks[wIdx]
	if !ok && off > 0 {
		if wIdx <= w.windowIdx {
			r := newWindowReader(w.file)
			b, err = r.readBlock(off)
			if err != nil {
				return off, err
			}
			b.validation(topicHash)
			b.leased = true
		}
	}
	b.topicHash = topicHash
	for _, we := range wEntries {
		if we.sequence == 0 {
			continue
		}
		if b.entryIdx == entriesPerWindowBlock {
			topicHash := b.topicHash
			next := int64(blockSize * wIdx)
			// set approximate cutoff on winBlock.
			b.cutoffTime = time.Now().Unix()
			w.winBlocks[wIdx] = b
			w.windowIdx++
			wIdx = w.windowIdx
			b = _WinBlock{topicHash: topicHash, next: next}
		}
		if b.leased {
			w.winLeases[wIdx] = append(w.winLeases[wIdx], we.sequence)
		}
		b.entries[b.entryIdx] = _WinEntry{sequence: we.sequence, expiresAt: we.expiresAt}
		b.dirty = true
		b.entryIdx++
	}
	w.winBlocks[wIdx] = b

	return int64(blockSize * wIdx), nil
}

func (w *_WindowWriter) write() error {
	for bIdx, b := range w.winBlocks {
		if !b.leased || !b.dirty {
			continue
		}
		off := int64(blockSize * bIdx)
		if _, err := w.file.WriteAt(b.MarshalBinary(), off); err != nil {
			return err
		}
		b.dirty = false
		w.winBlocks[bIdx] = b
		// fmt.Println("timeWindow.write: topicHash, seq ", b.topicHash, b.entries[0])
	}

	// sort blocks by blockIdx.
	var blockIdx []int32
	for bIdx := range w.winBlocks {
		if w.winBlocks[bIdx].leased || !w.winBlocks[bIdx].dirty {
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
			off := int64(blockSize * bIdx)
			b := w.winBlocks[bIdx]
			buf := b.MarshalBinary()
			if _, err := w.file.WriteAt(buf, off); err != nil {
				return err
			}
			b.dirty = false
			w.winBlocks[bIdx] = b
			// fmt.Println("timeWindow.write: topicHash, seq ", b.topicHash, b.entries[0])
			continue
		}
		blockOff := int64(blockSize * blocks[0])
		for bIdx := blocks[0]; bIdx <= blocks[1]; bIdx++ {
			b := w.winBlocks[bIdx]
			w.buffer.Write(b.MarshalBinary())
			b.dirty = false
			w.winBlocks[bIdx] = b
			// fmt.Println("timeWindow.write: topicHash, seq ", b.topicHash, b.entries[0])
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

func (w *_WindowWriter) rollback() error {
	for bIdx, seqs := range w.winLeases {
		for _, seq := range seqs {
			if err := w.del(seq, bIdx); err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *_WindowWriter) reset() error {
	w.buffer.Reset()
	w.winOffset = w.file.currSize()

	return nil
}

func (w *_WindowWriter) abort() error {
	w.file.truncate(w.winOffset)

	return w.rollback()
}
