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
	"encoding/binary"
	"fmt"
)

const (
	blockSize int32 = 4096
)

type (
	_IndexEntry struct {
		seq       uint64
		topicSize uint16
		valueSize uint32
		msgOffset int64

		cache []byte // block from memdb if it exist
	}
	_IndexBlock struct {
		entries  [entriesPerIndexBlock]_IndexEntry
		baseSeq  uint64
		entryIdx uint16

		dirty  bool
		leased bool
	}
)

func blockIndex(seq uint64) int32 {
	return int32(float64(seq-1) / float64(entriesPerIndexBlock))
}

func blockOffset(idx int32) int64 {
	if idx == -1 {
		return int64(0)
	}
	return int64(blockSize * idx)
}

// deleted reports whether the entry is a tombstone. An entry is deleted either
// with msgOffset -1, or, if it holds the topic, with valueSize 0 so the topic
// can still be read when loading the trie. Live entries never have valueSize 0.
func (e _IndexEntry) deleted() bool {
	return e.msgOffset == -1 || e.valueSize == 0
}

func (e _IndexEntry) mSize() uint32 {
	return idSize + uint32(e.topicSize) + e.valueSize
}

func (b _IndexBlock) validation(blockIdx int32) error {
	bIdx := blockIndex(b.entries[0].seq)
	if bIdx != blockIdx {
		return fmt.Errorf("validation failed blockIdx %d, startBlockIdx %d", blockIdx, bIdx)
	}
	return nil
}

// marshalBinary serialized entries block into binary data.
func (b _IndexBlock) marshalBinary() []byte {
	buf := make([]byte, blockSize)
	data := buf

	b.baseSeq = b.entries[0].seq
	binary.LittleEndian.PutUint64(buf[indexBaseOff:], b.baseSeq)
	for i := 0; i < entriesPerIndexBlock; i++ {
		s := b.entries[i]
		e := buf[indexEntriesOff+i*indexEntrySize:]
		seq := uint16(0)
		if s.seq != 0 {
			seq = uint16(int16(s.seq-b.baseSeq) + entriesPerIndexBlock)
		}
		binary.LittleEndian.PutUint16(e[indexRelSeqOff:], seq) // marshal relative seq
		binary.LittleEndian.PutUint16(e[indexTopicSizeOff:], s.topicSize)
		binary.LittleEndian.PutUint32(e[indexValueSizeOff:], s.valueSize)
		binary.LittleEndian.PutUint64(e[indexMsgOffsetOff:], uint64(s.msgOffset))
	}
	binary.LittleEndian.PutUint16(buf[indexEntryIdxOff:], b.entryIdx)
	putChecksum(data, indexChecksumOff)
	return data
}

// unmarshalBinary de-serialized entries block from binary data.
func (b *_IndexBlock) unmarshalBinary(data []byte) error {
	b.baseSeq = binary.LittleEndian.Uint64(data[indexBaseOff:])
	for i := 0; i < entriesPerIndexBlock; i++ {
		e := data[indexEntriesOff+i*indexEntrySize : indexEntriesOff+(i+1)*indexEntrySize]
		seq := int16(binary.LittleEndian.Uint16(e[indexRelSeqOff:]))
		if seq == 0 {
			b.entries[i].seq = uint64(seq)
		} else {
			b.entries[i].seq = b.baseSeq + uint64(seq) - entriesPerIndexBlock // unmarshal from relative sequence
		}
		b.entries[i].topicSize = binary.LittleEndian.Uint16(e[indexTopicSizeOff:])
		b.entries[i].valueSize = binary.LittleEndian.Uint32(e[indexValueSizeOff:])
		b.entries[i].msgOffset = int64(binary.LittleEndian.Uint64(e[indexMsgOffsetOff:]))
	}
	b.entryIdx = binary.LittleEndian.Uint16(data[indexEntryIdxOff:])
	return nil
}
