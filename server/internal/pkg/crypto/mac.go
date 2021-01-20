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

package crypto

import (
	"crypto/cipher"
	"errors"

	"github.com/unit-io/unitdb/server/internal/pkg/hash"
	"golang.org/x/crypto/chacha20poly1305"
)

const (
	EpochSize     = 4
	MessageOffset = EpochSize + 4
)

// MAC has the ability to encrypt and decrypt (short) messages as long as they
// share the same key and the same epoch.
type MAC struct {
	parent cipher.AEAD
	salt   []byte
}

// New builds a new MAC using a 256-bit/32 byte encryption key, a numeric epoch
// and numeric pseudo-random salt
func New(key []byte) (*MAC, error) {
	parent, err := chacha20poly1305.New(key)
	if err != nil {
		return nil, err
	}

	mac := new(MAC)
	mac.salt = make([]byte, 4)
	mac.parent = parent
	for i := 0; i < 4; i++ {
		mac.salt[i] = (byte(key[(4*i)+0]) << 24) |
			(byte(key[(4*i)+1]) << 16) |
			(byte(key[(4*i)+2]) << 8) |
			byte(key[(4*i)+3])
	}

	return mac, nil
}

// Overhead returns the maximum difference between the lengths of a
// plaintext and its ciphertext.
func (m *MAC) Overhead() int { return m.parent.Overhead() + EpochSize }

func SignatureToUint32(sig []byte) uint32 {
	return uint32(sig[0])<<24 | uint32(sig[1])<<16 | uint32(sig[2])<<8 | uint32(sig[3])
}

func Signature(value uint32) []byte {
	sig := make([]byte, 4)
	sig[0] = byte(value >> 24)
	sig[1] = byte(value >> 16)
	sig[2] = byte(value >> 8)
	sig[3] = byte(value)
	return sig
}

// Encrypt encrypts src and appends to dst, returning the
// resulting byte slice
func (m *MAC) Encrypt(dst, src []byte) []byte {
	//Copy first 4 bytes epoch from source
	dst = append(dst, src[:EpochSize]...)
	h := hash.New(src)
	dst = append(dst, Signature(h)...)
	nonce := append(m.salt, dst[:MessageOffset]...)
	return m.parent.Seal(dst, nonce, src[EpochSize:], nil)
}

// Decrypt decrypts src and appends to dst, returning the
// resulting byte slice or an error if the input cannot be
// authenticated.
func (m *MAC) Decrypt(dst, src []byte) ([]byte, error) {

	if len(src) < m.Overhead() {
		return dst, errors.New("Authentication failed.")
	}

	nonce := append(m.salt, src[:MessageOffset]...)
	dst, err := m.parent.Open(dst, nonce, src[MessageOffset:], nil)
	if err != nil {
		return dst, errors.New("Authentication failed.")
	}
	// Append epoch to dst at the begining
	dst = append(src[:EpochSize], dst...)
	return dst, nil
}
