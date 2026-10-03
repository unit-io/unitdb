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

package uid

import (
	"crypto/rand"
	"encoding/binary"
	"errors"

	"github.com/unit-io/unitdb/server/internal/pkg/crypto"
	"github.com/unit-io/unitdb/server/internal/pkg/encoding"
)

// ID represents a unique ID for client connection: 12 bytes, the epoch, the
// primary id, the permissions and the contract, followed in an id made since
// v2 client ids by an 8-byte random uuid. A v1 client id (refused since
// v0.7.0) carried no uuid: opened, it is 12 bytes, and so is a v2 id sealed
// again from a v1 one.
type ID []byte

const (
	AllowNone   = uint32(0)      // ID has no privileges.
	AllowMaster = uint32(1 << 0) // ID should be allowed to generate other IDs.
	// AllowService marks a trusted service's id, such as an API server's
	// acting for its users: its connections, and the ones it vouches for
	// with unitdb/service, skip topic key checks. Only server/cmd/mintid
	// issues it; the server never hands it out.
	AllowService = uint32(1 << 1)

	rawLen    = 12 // binary raw len, without the uuid
	uuidLen   = 8
	idLenV2   = rawLen + uuidLen
	v1TextLen = 52 // encoded len of a v1 client id
)

// Uuid returns the id's random uuid, or 0 for an id without one: one sealed
// again from a v1 id.
func (id ID) Uuid() uint64 {
	if len(id) < idLenV2 {
		return 0
	}
	return binary.BigEndian.Uint64(id[rawLen:idLenV2])
}

// withUuid returns id with a new random uuid, never 0.
func (id ID) withUuid() (ID, error) {
	out := make(ID, idLenV2)
	copy(out, id[:rawLen])
	for binary.BigEndian.Uint64(out[rawLen:]) == 0 {
		if _, err := rand.Read(out[rawLen:]); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// IsPrimary gets whether the ID is a primary client Id.
func (id ID) IsPrimary() bool {
	return id.HasPermission(AllowMaster)
}

// HasPermission reports whether the ID has every permission in flag.
func (id ID) HasPermission(flag uint32) bool {
	return flag != 0 && id.Permissions()&flag == flag
}

// IsService gets whether the ID is a trusted service's (AllowService).
func (id ID) IsService() bool {
	return id.HasPermission(AllowService)
}

// Apoch gets the Apoch for the ID
func (id ID) Epoch() uint32 {
	return uint32(id[0])<<24 | uint32(id[1])<<16 | uint32(id[2])<<8 | uint32(id[3])
}

// SetApoch sets the Apoch for the ID
func (id ID) SetEpoch(value uint32) {
	id[0] = byte(value >> 24)
	id[1] = byte(value >> 16)
	id[2] = byte(value >> 8)
	id[3] = byte(value)
}

// Primary gets the primary client Id. It is stored in id[5:7]: id[4] is
// always zero.
func (id ID) Primary() uint16 {
	return uint16(id[5])<<8 | uint16(id[6])
}

// SetPrimary sets the primary client Id
func (id ID) SetPrimary(value uint16) {
	id[4] = 0
	id[5] = byte(value >> 8)
	id[6] = byte(value)
}

// Permissions gets the permission flags.
func (id ID) Permissions() uint32 {
	return uint32(id[7])
}

// SetPermissions sets the permission flags.
func (id ID) SetPermissions(value uint32) {
	id[7] = byte(value)
}

// Contract gets the contract id.
func (id ID) Contract() uint32 {
	return uint32(id[8])<<24 | uint32(id[9])<<16 | uint32(id[10])<<8 | uint32(id[11])
}

// SetContract sets the contract id.
func (id ID) SetContract(value uint32) {
	id[8] = byte(value >> 24)
	id[9] = byte(value >> 16)
	id[10] = byte(value >> 8)
	id[11] = byte(value)
}

// DecodeV1 opens a v1 client id sealed with mac, for server/cmd/mintid
// -from to seal it again as a v2 id. The server refuses v1 ids since v0.7.0,
// and nothing seals them any more. It decodes in place: hand it a copy.
func DecodeV1(buffer []byte, mac *crypto.MAC) (ID, error) {
	if len(buffer) < v1TextLen {
		return nil, errors.New("Key provided is invalid")
	}

	// Warning: base32 decoding is done in the same underlying buffer, to save up
	// on memory allocations.
	encoding.Decode32(buffer, buffer)
	// Decryption.
	key, err := mac.Decrypt(nil, buffer[:32])
	if err != nil {
		return nil, errors.New("Key provided is invalid")
	}

	// Resize the slice, since it is changed.
	buffer = key[0:rawLen]

	// XOR the entire array with the salt.
	for i := 2; i < rawLen; i += 2 {
		buffer[i] = byte(buffer[i] ^ buffer[0])
		buffer[i+1] = byte(buffer[i+1] ^ buffer[1])
	}

	// Return the key on the decrypted buffer.
	return ID(buffer), nil
}

// reservedContracts are the contracts NewContract never draws: 0, which the
// store keeps the node's own records under; the storage engine's master
// contract, which it stores contract 0 as; and the fixed ids under which
// v0.6.0 and before kept the store's own records, which a newer version reads
// only to move what they hold (store.LegacyStoreIDs).
var reservedContracts = map[uint32]bool{
	0:          true,
	3376684800: true, // the engine's master contract
	4105991048: true, // hash("connectionstore")
	2654435761: true,
	2246822519: true,
	3266489917: true,
	2860486313: true,
	2210380056: true, // hash("securitystore")
}

// IsReservedContract reports whether contract is one NewContract never draws.
func IsReservedContract(contract uint32) bool {
	return reservedContracts[contract]
}

// NewContract returns a random contract from crypto/rand, never a reserved
// one (IsReservedContract).
func NewContract() (uint32, error) {
	var raw [4]byte
	for {
		if _, err := rand.Read(raw[:]); err != nil {
			return 0, err
		}
		if contract := binary.BigEndian.Uint32(raw[:]); !reservedContracts[contract] {
			return contract, nil
		}
	}
}

// MintClientID makes the primary client Id server/cmd/mintid issues: of
// contract, or of a new contract if contract is 0, and marked as a trusted
// service's (AllowService) if service is set.
func MintClientID(contract uint32, service bool) (ID, error) {
	id, err := NewClientID(1)
	if err != nil {
		return ID{}, err
	}
	if contract != 0 {
		id.SetContract(contract)
	}
	if service {
		id.SetPermissions(id.Permissions() | AllowService)
	}
	return id, nil
}

// NewClientID generates a new primary client Id, of a new contract, with a
// random uuid. The contract and the uuid come from crypto/rand; a failing
// source is an error, rather than contract 0.
func NewClientID(master uint16) (ID, error) {
	contract, err := NewContract()
	if err != nil {
		return ID{}, err
	}

	id := ID(make([]byte, rawLen))
	id.SetEpoch(NewApoch())
	id.SetPrimary(master)
	id.SetPermissions(AllowMaster)
	id.SetContract(contract)
	return id.withUuid()
}

// NewSecondaryClientID generates a secondary client Id, of master's contract.
// Its uuid tells it from the other secondary ids of the contract, which v1
// ids issued in the same second could not be.
func NewSecondaryClientID(master ID) (ID, error) {
	id, err := NewClientID(1)
	if err != nil {
		return ID{}, err
	}

	id.SetEpoch(NewApoch())
	id.SetPermissions(AllowNone)
	id.SetContract(master.Contract())
	return id, nil
}

// CachedClientID return cached client Id
func CachedClientID(contract uint32) (ID, error) {
	id, err := NewClientID(1)
	if err != nil {
		return ID{}, err
	}
	id.SetPermissions(AllowNone)
	id.SetContract(contract)
	return id, nil
}
