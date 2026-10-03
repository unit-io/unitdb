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

	"golang.org/x/crypto/chacha20poly1305"
)

const (
	EpochSize     = 4
	MessageOffset = EpochSize + 4
)

// MAC opens what v1 client ids sealed: ChaCha20-Poly1305 under a nonce made
// of the key's salt and the clear start of the message (finding 5 of the
// security review). Since v0.7.0 nothing is sealed with it; it is kept only
// for server/cmd/mintid -from, which seals a v1 id again as a v2 one.
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
	// The salt is the last byte of each of the key's first four words: the
	// nonce prefix every client ID, topic key and encrypted entry was sealed
	// with. It is part of the nonce, not a secret, and changing it would make
	// them all fail to decrypt.
	for i := 0; i < 4; i++ {
		mac.salt[i] = key[(4*i)+3]
	}

	return mac, nil
}

// Overhead returns the maximum difference between the lengths of a
// plaintext and its ciphertext.
func (m *MAC) Overhead() int { return m.parent.Overhead() + EpochSize }

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
