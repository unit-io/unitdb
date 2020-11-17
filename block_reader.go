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
	indexBlock _IndexBlock

	fs *_FileSet
}

func newBlockReader(fs *_FileSet) *_BlockReader {
	return &_BlockReader{fs: fs}
}

func (r *_BlockReader) readIndexBlock(seq uint64) (_IndexBlock, error) {
	off := blockOffset(blockIndex(seq))
	indexFile, err := r.fs.getFile(_FileDesc{fileType: typeIndex})
	if err != nil {
		return _IndexBlock{}, err
	}
	buf, err := indexFile.slice(off, off+int64(blockSize))
	if err != nil {
		return _IndexBlock{}, err
	}
	r.indexBlock.UnmarshalBinary(buf)

	return r.indexBlock, nil
}

func (r *_BlockReader) readIndexEntry(seq uint64) (_IndexEntry, error) {
	if _, err := r.readIndexBlock(seq); err != nil {
		return _IndexEntry{}, err
	}

	entryIdx := -1
	for i := 0; i < entriesPerIndexBlock; i++ {
		s := r.indexBlock.entries[i]
		if s.seq == seq { //topic exist in db
			entryIdx = i
			break
		}
	}
	if entryIdx == -1 {
		return _IndexEntry{}, errEntryInvalid
	}

	return r.indexBlock.entries[entryIdx], nil
}

func (r *_BlockReader) readMessage(e _IndexEntry) ([]byte, []byte, error) {
	if e.cache != nil {
		return e.cache[:idSize], e.cache[e.topicSize+idSize:], nil
	}
	dataFile, err := r.fs.getFile(_FileDesc{fileType: typeData})
	if err != nil {
		return nil, nil, err
	}
	message, err := dataFile.slice(e.msgOffset, e.msgOffset+int64(e.mSize()))
	if err != nil {
		return nil, nil, err
	}
	return message[:idSize], message[e.topicSize+idSize:], nil
}

func (r *_BlockReader) readTopic(e _IndexEntry) ([]byte, error) {
	if e.cache != nil {
		return e.cache[idSize : e.topicSize+idSize], nil
	}
	dataFile, err := r.fs.getFile(_FileDesc{fileType: typeData})
	if err != nil {
		return nil, err
	}
	return dataFile.slice(e.msgOffset+int64(idSize), e.msgOffset+int64(e.topicSize)+int64(idSize))
}