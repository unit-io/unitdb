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

// ID represents a unique ID for client connection.
type ID []byte

const (
	AllowNone   = uint32(0)      // ID has no privileges.
	AllowMaster = uint32(1 << 0) // ID should be allowed to generate other IDs.
	// AllowService marks a trusted service's id, such as an API server's
	// acting for its users: its connections, and the ones it vouches for
	// with unitdb/service, skip topic key checks. Only server/cmd/mintid
	// issues it; the server never hands it out.
	AllowService = uint32(1 << 1)

	encodedLen = 13 // string encoded len
	rawLen     = 12 // binary raw len
)

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

func (id ID) Encode(mac *crypto.MAC) string {
	buffer := make([]byte, rawLen)
	buffer[0] = id[0]
	buffer[1] = id[1]

	// First XOR the entire array with the salt
	for i := 2; i < rawLen; i += 2 {
		buffer[i] = byte(id[i] ^ buffer[0])
		buffer[i+1] = byte(id[i+1] ^ buffer[1])
	}

	// Encryption.
	ciphertext := mac.Encrypt(nil, buffer)
	text := make([]byte, 52)
	encoding.Encode32(text, ciphertext[:])
	return string(text)
}

func Decode(buffer []byte, mac *crypto.MAC) (ID, error) {
	if len(buffer) < 52 {
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

// NewContract returns a random contract from crypto/rand, never 0.
func NewContract() (uint32, error) {
	var raw [4]byte
	for {
		if _, err := rand.Read(raw[:]); err != nil {
			return 0, err
		}
		if contract := binary.BigEndian.Uint32(raw[:]); contract != 0 {
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

// NewClientID generates a new primary client Id, of a new contract. The
// contract comes from crypto/rand; a failing source is an error, rather
// than contract 0.
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
	return id, nil
}

// NewSecondaryClientID generates a secondary client Id.
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
