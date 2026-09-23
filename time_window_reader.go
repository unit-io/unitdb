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
)

type _WindowReader struct {
	winBlock  _WinBlock
	windowIdx int32
	fs        *_FileSet
	winFile   *_File
	offset    int64
}

func newWindowReader(fs *_FileSet) *_WindowReader {
	w := &_WindowReader{windowIdx: -1, fs: fs}
	winFile, err := fs.getFile(_FileDesc{fileType: typeTimeWindow})
	if err != nil {
		return w
	}
	w.winFile = winFile

	if winFile.currSize() > 0 {
		w.windowIdx = int32(winFile.currSize() / int64(blockSize))
	}
	return w
}

func (r *_WindowReader) readWindowBlock() (_WinBlock, error) {
	buf, err := r.winFile.slice(r.offset, r.offset+int64(blockSize))
	if err != nil {
		return _WinBlock{}, err
	}
	if err := r.winBlock.unmarshalBinary(buf); err != nil {
		return _WinBlock{}, err
	}

	return r.winBlock, nil
}

// blockIterator calls f once per topic stored in the window file, with the
// first sequence of the topic's oldest block (whose entry holds the topic) and
// the offset of its newest block, the head of the topic's chain.
func (r *_WindowReader) blockIterator(f func(startSeq, topicHash uint64, off int64) (bool, error)) (err error) {
	type topicBlocks struct {
		startSeq uint64
		hasStart bool
		headOff  int64
	}
	topics := make(map[uint64]*topicBlocks)
	var order []uint64
	for windowIdx := int32(0); windowIdx <= r.windowIdx; windowIdx++ {
		r.offset = winBlockOffset(windowIdx)
		b, err := r.readWindowBlock()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if b.entryIdx == 0 {
			continue
		}
		tb, ok := topics[b.topicHash]
		if !ok {
			tb = &topicBlocks{}
			topics[b.topicHash] = tb
			order = append(order, b.topicHash)
		}
		// New blocks are always appended, so the last block seen is the head.
		tb.headOff = r.offset
		if b.next == 0 && !tb.hasStart {
			tb.startSeq = b.entries[0].sequence
			tb.hasStart = true
		}
	}
	for _, h := range order {
		tb := topics[h]
		if !tb.hasStart {
			continue
		}
		if stop, err := f(tb.startSeq, h, tb.headOff); stop || err != nil {
			return err
		}
	}
	return nil
}
