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

// The on-disk formats, in one place. Integers are little-endian. Encoders
// and decoders take the offsets from here: the encryption flag was written
// at one offset and read at another. format_test.go checks each format
// against saved bytes (testdata/format), which a change to a format must
// update, with a new version where old files must still read.
//
// Zero is "none" wherever a field can be absent, and is never a value:
// sequences start at 1; window block 0 is left unused, so a window offset
// of 0 is no block; a memdb block ID of 0 is a log written before logs
// recorded their block. Data offset 0 holds a message, so the free list
// drops a free block at 0, and an index entry's offset for none is -1.
//
// Info header (unitdb.info), format 3, fixed bytes:
//
//	0   7  signature
//	7   4  version
//	11  1  encryption: 1 if messages are encrypted
//	12  8  sequence: the last sequence given
//	20  8  count: entries on disk not deleted
//	28  8  syncing: the memdb block a sync is writing, or 0
//	36  4  CRC32C of bytes 0 to 36
//
// Format 2 has no syncing; its CRC32C is at 28, of bytes 0 to 28. Format 1
// has no CRC32C.
const (
	infoSignatureOff  = 0
	infoVersionOff    = 7
	infoEncryptionOff = 11
	infoSequenceOff   = 12
	infoCountOff      = 20
	infoSyncingOff    = 28
)

// Index block (index file), blockSize bytes; the entry of sequence s is in
// block (s-1)/entriesPerIndexBlock:
//
//	0      8   base: the sequence of entry 0
//	8      16  entries, entriesPerIndexBlock of them:
//	             0   2  sequence - base + entriesPerIndexBlock; 0 for none
//	             2   2  topic size: the bytes of the topic's name the
//	                    message holds, if it holds it
//	             4   4  value size; 0 for a deleted entry holding its topic
//	             8   8  offset of the message in the data file; -1 deleted
//	4088   2   entries used
//	4090   4   CRC32C of bytes 0 to 4090
const (
	indexBaseOff      = 0
	indexEntriesOff   = 8
	indexEntrySize    = 16
	indexEntryIdxOff  = indexEntriesOff + entriesPerIndexBlock*indexEntrySize
	indexRelSeqOff    = 0
	indexTopicSizeOff = 2
	indexValueSizeOff = 4
	indexMsgOffsetOff = 8
)

// Window block (window file), blockSize bytes; a topic's blocks link from
// its newest, which the trie points at, to its oldest:
//
//	0      12  entries, entriesPerWindowBlock of them:
//	             0   8  sequence
//	             8   4  expiry, Unix seconds; 0 for none
//	4020   8   cutoff time
//	4028   8   topic hash
//	4036   8   offset of the topic's block before; 0 for none
//	4044   2   entries used
//	4046   4   CRC32C of bytes 0 to 4046
const (
	winEntrySize    = 12
	winCutoffOff    = entriesPerWindowBlock * winEntrySize
	winTopicHashOff = winCutoffOff + 8
	winNextOff      = winTopicHashOff + 8
	winEntryIdxOff  = winNextOff + 8
)

// Message (data file), at its index entry's offset:
//
//	0   8  the message ID's prefix
//	8   1  encryption: 1 if the value is encrypted
//	9   t  the topic's name (topic size bytes; see Topic.Marshal), in the
//	       first entry of a topic, and of each topic in a batch
//	9+t v  value: snappy, then encrypted if so
//
// Checksum file: the CRC32C of the message of sequence s at s*4.
//
// Free list (lease file): a count n, 4 bytes; n free blocks of an offset, 8
// bytes, and a size, 4; and the CRC32C of what precedes.
//
// Filter file: the bloom filter of the sequences in the index, then its
// CRC32C.
//
// Entry (memdb value of a sequence), entrySize bytes, then the message:
//
//	0   8  sequence
//	8   2  topic size
//	10  4  value size; 0 for a tombstone (see delete)
//	14  4  expiry
//	18  8  topic hash
//
// memdb and its WAL have their formats in memdb and wal.
