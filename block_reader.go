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

// _BlockReader reads index blocks and messages. The DB shares one reader
// between concurrent readers, so it keeps no state between calls.
type _BlockReader struct {
	fs                           *_FileSet
	indexFile, dataFile, sumFile *_File
}

func newBlockReader(fs *_FileSet) *_BlockReader {
	r := &_BlockReader{fs: fs}

	indexFile, err := fs.getFile(_FileDesc{fileType: typeIndex})
	if err != nil {
		return r
	}
	r.indexFile = indexFile

	dataFile, err := fs.getFile(_FileDesc{fileType: typeData})
	if err != nil {
		return nil
	}
	r.dataFile = dataFile

	sumFile, err := fs.getFile(_FileDesc{fileType: typeChecksum})
	if err != nil {
		return nil
	}
	r.sumFile = sumFile

	return r
}

// readIndexBlock reads the index block at off.
func (r *_BlockReader) readIndexBlock(off int64) (_IndexBlock, error) {
	buf, err := r.indexFile.slice(off, off+int64(blockSize))
	if err != nil {
		return _IndexBlock{}, err
	}
	if !validChecksum(buf, indexChecksumOff) {
		return _IndexBlock{}, corrupted(r.indexFile, off, "index block")
	}
	var b _IndexBlock
	if err := b.unmarshalBinary(buf); err != nil {
		return _IndexBlock{}, err
	}

	return b, nil
}

// readEntry reads the index entry for seq, returning errMsgIDDeleted for deleted entries.
func (r *_BlockReader) readEntry(seq uint64) (_IndexEntry, error) {
	e, err := r.readIndexEntry(seq)
	if err != nil {
		return _IndexEntry{}, err
	}
	if e.deleted() {
		return _IndexEntry{}, errMsgIDDeleted
	}

	return e, nil
}

// readIndexEntry reads the index entry for seq, including deleted entries.
func (r *_BlockReader) readIndexEntry(seq uint64) (_IndexEntry, error) {
	b, err := r.readIndexBlock(blockOffset(blockIndex(seq)))
	if err == io.EOF {
		// the index block has not been written yet.
		return _IndexEntry{}, errEntryInvalid
	}
	if err != nil {
		return _IndexEntry{}, err
	}
	for i := 0; i < entriesPerIndexBlock; i++ {
		if b.entries[i].seq == seq { //topic exist in db
			return b.entries[i], nil
		}
	}

	return _IndexEntry{}, errEntryInvalid
}

func (r *_BlockReader) readMessage(e _IndexEntry) ([]byte, []byte, error) {
	if e.cache != nil {
		return e.cache[:idSize], e.cache[e.topicSize+idSize:], nil
	}
	message, err := r.dataFile.slice(e.msgOffset, e.msgOffset+int64(e.mSize()))
	if err != nil {
		return nil, nil, err
	}
	if err := verifyMessage(r.sumFile, r.dataFile, e.seq, e.msgOffset, message); err != nil {
		return nil, nil, err
	}
	return message[:idSize], message[e.topicSize+idSize:], nil
}

func (r *_BlockReader) readTopic(e _IndexEntry) ([]byte, error) {
	if e.cache != nil {
		return e.cache[idSize : e.topicSize+idSize], nil
	}
	return r.dataFile.slice(e.msgOffset+int64(idSize), e.msgOffset+int64(e.topicSize)+int64(idSize))
}
