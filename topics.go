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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/unit-io/unitdb/message"
)

// errTopicCollision is returned for a topic whose hash is another topic's:
// its entries would be the other's.
var errTopicCollision = errors.New("topic hash collides with another topic's")

// _TopicNames is the topics file: the name of each topic, as Topic.Marshal
// writes it, recorded before the first entry of the topic is put. A topic's
// name was in its first entry only, and the topic was lost when that entry
// was deleted, or reached disk after others of the topic, or a crash lost
// it while keeping others (format version 4).
//
// A record, appended once per topic:
//
//	0   4  n: the bytes of hash and name
//	4   8  topic hash
//	12  -  name, n-8 bytes
//	-   4  CRC32C of the bytes before it in the record
type _TopicNames struct {
	mu    sync.Mutex
	file  _FileSet
	size  int64
	names map[uint64][]byte
}

const topicRecordHead = 4 + 8

func newTopicNames(file _FileSet) *_TopicNames {
	return &_TopicNames{file: file, names: make(map[uint64][]byte)}
}

// load reads the records. A record cut short or failing its CRC32C at the
// end is a write a crash stopped, of a topic whose entries were not put:
// it is cut off. One before the end is corruption.
func (ts *_TopicNames) load() error {
	size := ts.file.currSize()
	if size == 0 {
		return nil
	}
	raw := make([]byte, size)
	if _, err := ts.file.ReadAt(raw, 0); err != nil {
		return err
	}
	off := 0
	for off < len(raw) {
		hash, name, next, ok := topicRecord(raw, off)
		if !ok {
			if next < len(raw) {
				return corrupted(ts.file._File, int64(off), "topic record")
			}
			logger.Error().Int64("offset", int64(off)).Str("context", "topics.load").Msg("cut off a topic record a crash stopped")
			if err := ts.file.Truncate(int64(off)); err != nil {
				return err
			}
			break
		}
		if prev, ok := ts.names[hash]; ok && !bytes.Equal(prev, name) {
			return fmt.Errorf("%w: topic %d named twice", errCorrupted, hash)
		}
		ts.names[hash] = name
		off = next
	}
	ts.size = int64(off)
	return nil
}

// topicRecord decodes the record at off, and returns the offset of the
// next; ok is false for one cut short or failing its CRC32C.
func topicRecord(raw []byte, off int) (hash uint64, name []byte, next int, ok bool) {
	if off+topicRecordHead > len(raw) {
		return 0, nil, len(raw), false
	}
	n := int(binary.LittleEndian.Uint32(raw[off:]))
	end := off + 4 + n
	if n < 8 || end+checksumSize > len(raw) || end < off {
		return 0, nil, len(raw), false
	}
	next = end + checksumSize
	if !matchesChecksum(raw[off:next], 4+n) {
		return 0, nil, next, false
	}
	name = append([]byte(nil), raw[off+topicRecordHead:end]...)
	return binary.LittleEndian.Uint64(raw[off+4:]), name, next, true
}

// name records the topic's name, if it has none, and syncs it if durable.
// It returns errTopicCollision for a hash named otherwise.
func (ts *_TopicNames) name(hash uint64, name []byte, durable bool) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if prev, ok := ts.names[hash]; ok {
		if !bytes.Equal(prev, name) {
			return errTopicCollision
		}
		return nil
	}
	rec := make([]byte, topicRecordHead+len(name)+checksumSize)
	binary.LittleEndian.PutUint32(rec, uint32(8+len(name)))
	binary.LittleEndian.PutUint64(rec[4:], hash)
	copy(rec[topicRecordHead:], name)
	putChecksum(rec, len(rec)-checksumSize)
	if _, err := ts.file.WriteAt(rec, ts.size); err != nil {
		return err
	}
	if durable {
		if err := ts.file.Sync(); err != nil {
			return err
		}
	}
	ts.size += int64(len(rec))
	ts.names[hash] = append([]byte(nil), name...)
	return nil
}

// sync syncs the records written without.
func (ts *_TopicNames) sync() error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.file.Sync()
}

// get returns the name of the topic hash.
func (ts *_TopicNames) get(hash uint64) ([]byte, bool) {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	name, ok := ts.names[hash]
	return name, ok
}

// all returns the names, by topic hash.
func (ts *_TopicNames) all() map[uint64][]byte {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	all := make(map[uint64][]byte, len(ts.names))
	for h, n := range ts.names {
		all[h] = n
	}
	return all
}

// nameTopic names the topic t of hash, before an entry of it is put: its
// record is written first, and the topic in the trie. It is written as the
// WAL is, to the OS and not synced, which a process crash keeps: the WAL
// promises no more. The DB's syncs sync it with the other files; syncing
// each took milliseconds a topic. A topic named already is checked
// against t.
func (db *DB) nameTopic(hash uint64, t *message.Topic) error {
	raw := t.Marshal()
	if name, ok := db.internal.topics.get(hash); ok {
		if !bytes.Equal(name, raw) {
			return errTopicCollision
		}
		if _, ok := db.internal.trie.getOffset(hash); ok {
			return nil
		}
	} else if err := db.internal.topics.name(hash, raw, false); err != nil {
		return err
	}
	db.internal.trie.add(newTopic(hash, db.internal.unnamed[hash]), t.Parts, t.Depth)
	return nil
}
