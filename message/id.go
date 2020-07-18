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
	// MasterContract contract is default contract used for topics if client program does not specify Contract in the request
	MasterContract = uint32(3376684800)

	None      = uint32(0)      // ID has no flags.
	Encrypted = uint32(1 << 0) // ID has encryption set.

	fixed = 12
)

// Prefix generates prefix from parts and concatenate contract as first part of the topic
func Prefix(parts []Part) uint64 {
	if len(parts) == 1 {
		return uint64(parts[0].Hash)
	}
	return uint64(parts[1].Hash)<<32 + uint64(parts[0].Hash)
}

// ID represents a message ID and lexigraphically sortable
type ID []byte

// AddPrefix adds a Prefix to the ID, it is used to validate prefix.
func (id *ID) AddPrefix(prefix uint64) {
	newid := make(ID, fixed+8)
	copy(newid[:fixed], *id)
	binary.LittleEndian.PutUint64(newid[fixed:fixed+8], prefix)
	*id = newid
}

// Prefix gets the prefix of the ID.
func (id ID) Prefix() uint64 {
	if len(id) < fixed+8 {
		return 0
	}
	return binary.LittleEndian.Uint64(id[fixed:])
}

// NewID generates a new message identifier without containing a prefix. Prefix is set later.
func NewID(seq uint64, encrypted bool) ID {
	var eBit int8
	if encrypted {
		eBit = 1
	}
	id := make(ID, fixed)
	binary.LittleEndian.PutUint32(id[0:4], uid.NewApoch())
	binary.LittleEndian.PutUint64(id[4:12], (seq<<8)|uint64(eBit)) //set encryption flag on id
	return id
}

// SetEncryption sets an encryption on ID
func (id ID) SetEncryption() {
	eBit := 1
	id[12] = byte(eBit)
}

// IsEncrypted return if an encryption is set on ID
func (id ID) IsEncrypted() bool {
	num := binary.LittleEndian.Uint64(id[4:12])
	return num&0xff != 0
}

// Seq gets the seq for the id.
func (id ID) Seq() uint64 {
	num := binary.LittleEndian.Uint64(id[4:12])
	return uint64(num >> 8)
}

// EvalPrefix matches the prefix with the cutoff time.
func (id ID) EvalPrefix(contract uint32, cutoff int64) bool {
	// wild card topic (i.e. "*" or "...") will match first 4 byte of contract was added to the ID
	if cutoff > 0 {
		return binary.LittleEndian.Uint32(id[fixed:fixed+4]) == contract && uid.Time(id[0:4]) >= cutoff
	}
	return binary.LittleEndian.Uint32(id[fixed:fixed+4]) == contract
}
