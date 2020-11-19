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

import "io"

type _WindowReader struct {
	windowIdx int32
	winBlock  _WinBlock
	file      *_File
}

func newWindowReader(f *_File) *_WindowReader {
	w := &_WindowReader{windowIdx: -1, file: f}
	if f.currSize() > 0 {
		w.windowIdx = int32(f.currSize() / int64(blockSize))
	}
	return w
}

func (r *_WindowReader) readBlock(off int64) (_WinBlock, error) {
	buf, err := r.file.slice(off, off+int64(blockSize))
	if err != nil {
		return _WinBlock{}, err
	}
	r.winBlock.UnmarshalBinary(buf)

	return r.winBlock, nil
}

// foreachWindowBlock iterates winBlocks on DB init to store topic hash and last offset of topic into trie.
func (r *_WindowReader) foreachWindowBlock(f func(startSeq, topicHash uint64, off int64) (bool, error)) (err error) {
	windowIdx := int32(0)
	nBlocks := r.windowIdx
	for windowIdx <= nBlocks {
		off := winBlockOffset(windowIdx)
		b, err := r.readBlock(off)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		windowIdx++
		if b.entryIdx == 0 || b.next != 0 {
			continue
		}
		// fmt.Println("timeWindow.foreachTimeBlock: topicHash, seq ", b.topicHash, b.entries[0].sequence)
		if stop, err := f(b.entries[0].sequence, b.topicHash, off); stop || err != nil {
			return err
		}
	}
	return nil
}
