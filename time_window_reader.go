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

// blockIterator iterates all window blocks from disk.
func (r *_WindowReader) blockIterator(f func(startSeq, topicHash uint64, off int64) (bool, error)) (err error) {
	windowIdx := int32(0)
	nBlocks := r.windowIdx
	for windowIdx <= nBlocks {
		r.offset = winBlockOffset(windowIdx)
		b, err := r.readWindowBlock()
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
		// fmt.Println("timeWindow.blockIterator: topicHash, seq ", b.topicHash, b.entries[0].sequence)
		if stop, err := f(b.entries[0].sequence, b.topicHash, r.offset); stop || err != nil {
			return err
		}
	}
	return nil
}
