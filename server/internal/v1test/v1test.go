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

// Package v1test mints v1 client ids, v1 signed topic keys and unsigned
// topic keys as servers up to v0.6.0 issued them, for tests only: that the
// server refuses them, and that server/cmd/mintid -from seals a v1 id again
// as v2. Since v0.7.0 the server neither issues nor accepts any of them, so
// nothing but tests imports this package.
package v1test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"encoding/binary"

	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/encoding"
	"github.com/unit-io/unitdb/server/internal/pkg/hash"
	"golang.org/x/crypto/chacha20poly1305"
)

const (
	idLen     = 12 // a v1 id: epoch, primary, permissions, contract
	epochSize = 4
)

// ClientID seals the first 12 bytes of id as a v1 client id with key, the
// 32 bytes of a keyring key as they are: 52 characters of base32. A v1 id
// carries no uuid.
func ClientID(id []byte, key []byte) string {
	aead, err := chacha20poly1305.New(key)
	if err != nil {
		panic(err)
	}
	// The salt XORed over the id is its first two bytes.
	buf := make([]byte, idLen)
	buf[0], buf[1] = id[0], id[1]
	for i := 2; i < idLen; i += 2 {
		buf[i] = id[i] ^ buf[0]
		buf[i+1] = id[i+1] ^ buf[1]
	}
	// The nonce: the last byte of each of the key's first four words, the
	// epoch, and an unkeyed hash of the whole id, the last two sent in the
	// clear (finding 5 of the security review).
	sealed := append([]byte(nil), buf[:epochSize]...)
	sealed = binary.BigEndian.AppendUint32(sealed, hash.New(buf))
	nonce := []byte{key[3], key[7], key[11], key[15]}
	nonce = append(nonce, sealed...)
	sealed = aead.Seal(sealed, nonce, buf[epochSize:], nil)
	text := make([]byte, 52)
	encoding.Encode32(text, sealed)
	return string(text)
}

// SignedTopicKey returns a v1 signed topic key for topic on contract, signed
// as a server up to v0.6.0 signed with key: 26 characters of base32.
func SignedTopicKey(key []byte, contract uint32, topic string, permissions uint32) string {
	raw := unsigned(contract, topic, permissions)
	m := hmac.New(sha256.New, key)
	m.Write([]byte("unitdb topic key v1"))
	signing := m.Sum(nil)
	var c [4]byte
	binary.BigEndian.PutUint32(c[:], contract)
	m = hmac.New(sha256.New, signing)
	m.Write(c[:])
	m.Write(raw)
	buf := append(raw, m.Sum(nil)[:8]...)
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(buf)
}

// UnsignedTopicKey returns an unsigned topic key for topic on contract, as
// servers that set accept_unsigned_keys took: 13 characters, which anyone
// who knows the contract can make.
func UnsignedTopicKey(contract uint32, topic string, permissions uint32) string {
	k, err := security.GenerateKey(contract, topic, permissions)
	if err != nil {
		panic(err)
	}
	return k
}

func unsigned(contract uint32, topic string, permissions uint32) []byte {
	key := security.Key(make([]byte, 8))
	key.SetPermissions(permissions)
	if err := key.SetTarget(contract, topic); err != nil {
		panic(err)
	}
	return key
}
