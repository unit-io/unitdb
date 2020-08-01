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
	"strconv"
	"time"
	"unsafe"
)

const (
	entrySize = 26
)

type (
	entry struct {
		seq       uint64
		topicSize uint16
		valueSize uint32
		expiresAt uint32 // expiresAt for recovery from log and not persisted to Index file but persisted to the time window file

		parsed     bool
		topicHash  uint64 // topicHash for recovery from log and not persisted to the DB
		cacheEntry []byte // block from memdb if it exist
	}
	// Entry entry is a message entry structure
	Entry struct {
		entry
		// internalEntry
		ID         []byte // The ID of the message
		Topic      []byte // The topic of the message
		Payload    []byte // The payload of the message
		ExpiresAt  uint32 // The time expiry of the message
		Contract   uint32 // The contract is used to as salt to hash topic parts and also used as prefix in the message Id
		Encryption bool
	}
)

// NewEntry creates a new entry structure from the topic.
func NewEntry(topic, payload []byte) *Entry {
	return &Entry{
		Topic:   topic,
		Payload: payload,
	}
}

// WithID sets entry ID
func (e *Entry) WithID(id []byte) *Entry {
	e.ID = id
	return e
}

// WithPayload sets payload to put entry into DB.
func (e *Entry) WithPayload(payload []byte) *Entry {
	e.Payload = payload
	return e
}

// WithContract sets contract on entry.
func (e *Entry) WithContract(contract uint32) *Entry {
	e.Contract = contract
	return e
}

// WithTTL sets TTL for message expiry for the entry.
func (e *Entry) WithTTL(ttl []byte) *Entry {
	val, err := strconv.ParseInt(unsafeToString(ttl), 10, 64)
	if err == nil {
		e.ExpiresAt = uint32(time.Now().Add(time.Duration(int(val)) * time.Second).Unix())
	}
	var duration time.Duration
	duration, _ = time.ParseDuration(unsafeToString(ttl))
	e.ExpiresAt = uint32(time.Now().Add(duration).Unix())
	return e
}

// WithEncryption sets encryption on entry.
func (e *Entry) WithEncryption() *Entry {
	e.Encryption = true
	return e
}

func (e *Entry) reset() {
	e.seq = 0
	e.topicSize = 0
	e.cacheEntry = nil
	e.ID = nil
	e.Payload = nil
}

func (e entry) ExpiresAt() uint32 {
	return e.expiresAt
}

// MarshalBinary serialized entry into binary data
func (e entry) MarshalBinary() ([]byte, error) {
	buf := make([]byte, entrySize)
	data := buf
	binary.LittleEndian.PutUint64(buf[:8], e.seq)
	binary.LittleEndian.PutUint16(buf[8:10], e.topicSize)
	binary.LittleEndian.PutUint32(buf[10:14], e.valueSize)
	binary.LittleEndian.PutUint32(buf[14:18], e.expiresAt)
	binary.LittleEndian.PutUint64(buf[18:26], e.topicHash)
	return data, nil
}

// MarshalBinary de-serialized entry from binary data
func (e *entry) UnmarshalBinary(data []byte) error {
	e.seq = binary.LittleEndian.Uint64(data[:8])
	e.topicSize = binary.LittleEndian.Uint16(data[8:10])
	e.valueSize = binary.LittleEndian.Uint32(data[10:14])
	e.expiresAt = binary.LittleEndian.Uint32(data[14:18])
	e.topicHash = binary.LittleEndian.Uint64(data[18:26])
	return nil
}

// unsafeToString is used to convert a slice
// of bytes to a string without incurring overhead.
func unsafeToString(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}
