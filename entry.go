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
	"strconv"
	"time"
	"unsafe"
)

type topic struct {
	data   []byte
	hash   uint64
	offset int64
	size   uint16
	parsed bool
}

type (
	internalEntry struct {
		topic
		seq        uint64
		id         []byte
		val        []byte
		encryption bool
	}
	// Entry entry is a message entry structure
	Entry struct {
		internalEntry
		ID        []byte // The ID of the message
		Topic     []byte // The topic of the message
		Payload   []byte // The payload of the message
		ExpiresAt uint32 // The time expiry of the message
		Contract  uint32 // The contract is used to as salt to hash topic parts and also used as prefix in the message Id
	}
)

// NewEntry creates a new entry structure from the topic.
func NewEntry(topic []byte) *Entry {
	return &Entry{
		Topic: topic,
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

func (e *Entry) reset() {
	e.topic.size = 0
	e.topic.data = nil
	e.seq = 0
	e.id = nil
	e.val = nil
	e.ID = nil
	e.Payload = nil
}

// unsafeToString is used to convert a slice
// of bytes to a string without incurring overhead.
func unsafeToString(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}
