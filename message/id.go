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

package message

import (
	"encoding/binary"

	"github.com/unit-io/unitdb/uid"
)

// Various constant parts of the ID.
const (
	// MasterContract contract is default contract used for topics if client program does not specify Contract in the request.
	MasterContract = uint32(3376684800)

	fixed = 16
)

// Prefix generates unique ID from topic parts and concatenate contract as first part of the topic.
func Prefix(parts []Part) uint64 {
	if len(parts) == 1 {
		return uint64(parts[0].Hash)
	}
	return uint64(parts[1].Hash)<<32 + uint64(parts[0].Hash)
}

// ID represents a message ID and lexigraphically sortable.
type ID []byte

// NewID generates a new message identifier with a prefix. Master contract is adde to the ID and actual Contract is set later.
func NewID(seq uint64) ID {
	id := make(ID, fixed)
	binary.LittleEndian.PutUint32(id[0:4], uid.NewApoch())
	binary.LittleEndian.PutUint32(id[4:8], MasterContract)
	binary.LittleEndian.PutUint64(id[8:16], seq)

	return id
}

// Size return fixed size of the ID.
func (id ID) Size() int {
	return fixed
}

// Sequence gets the seq for the id.
func (id ID) Sequence() uint64 {
	return binary.LittleEndian.Uint64(id[8:16])
}

// SetContract sets Contract on ID.
func (id *ID) SetContract(contract uint32) {
	newid := make(ID, fixed)
	copy(newid[:fixed], *id)
	binary.LittleEndian.PutUint32(newid[4:8], contract)
	*id = newid
}

// Prefix return message ID only containing prefix.
func (id ID) Prefix() ID {
	prefix := make(ID, 8)
	copy(prefix, id[:8])
	return prefix
}

// EvalPrefix matches the prefix with the cutoff time.
func (id ID) EvalPrefix(contract uint32, cutoff int64) bool {
	// wild card topic (i.e. "*" or "...") will match first 4 byte of contract was added to the ID.
	if cutoff > 0 {
		return uid.Time(id[0:4]) >= cutoff && binary.LittleEndian.Uint32(id[4:8]) == contract
	}
	return binary.LittleEndian.Uint32(id[4:8]) == contract
}
