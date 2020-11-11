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

type _BlockReader struct {
	block _Block

	file *_File
}

func newBlockReader(f *_File) *_BlockReader {
	return &_BlockReader{file: f}
}

func (r *_BlockReader) readBlock(seq uint64) (_Block, error) {
	off := blockOffset(startBlockIndex(seq))
	buf, err := r.file.slice(off, off+int64(blockSize))
	if err != nil {
		return _Block{}, err
	}
	r.block.UnmarshalBinary(buf)

	return r.block, nil
}

func (r *_BlockReader) read(seq uint64) (_Slot, error) {
	if _, err := r.readBlock(seq); err != nil {
		return _Slot{}, err
	}

	entryIdx := -1
	for i := 0; i < entriesPerIndexBlock; i++ {
		s := r.block.entries[i]
		if s.seq == seq { //topic exist in db
			entryIdx = i
			break
		}
	}
	if entryIdx == -1 {
		return _Slot{}, errEntryInvalid
	}

	return r.block.entries[entryIdx], nil
}

func (r *_BlockReader) size() (int64, error) {
	ds, err := r.file.Stat()
	if err != nil {
		return 0, err
	}

	return ds.Size(), nil
}
