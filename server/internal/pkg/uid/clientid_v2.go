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
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"errors"

	"golang.org/x/crypto/chacha20poly1305"
)

// A v2 client id is sealed with XChaCha20-Poly1305 under a random nonce, and
// names the key that sealed it:
//
//	version (1) | key id (1) | nonce (24) |
//	sealed: id (12), uuid (8), issued at (4), expires at (4) | tag (16)
//
// in base64url without padding: 94 characters of A-Z, a-z, 0-9, '-' and
// '_', where a v1 id is 52 of base32. The version and the key id are the
// associated data; nothing else is sent in the clear. The id is the v1 id's
// 12 bytes, so it holds the permissions (AllowMaster, AllowService) and the
// contract; the uuid is 0 for an id sealed again from a v1 one.
const (
	versionV2   = 0x02
	headerLenV2 = 2
	plainLenV2  = idLenV2 + 8
	sealedLenV2 = headerLenV2 + chacha20poly1305.NonceSizeX + plainLenV2 + chacha20poly1305.Overhead

	// EncodedLenV2 is the length of a v2 client id.
	EncodedLenV2 = (sealedLenV2*8 + 5) / 6 // 94
	// EncodedLenV1 is the length of a v1 client id.
	EncodedLenV1 = v1TextLen
)

var idEncoding = base64.RawURLEncoding

// ErrInvalidID is the error for a client id that does not open.
var ErrInvalidID = errors.New("Key provided is invalid")

// Claims is what a v2 client id says of itself besides the id.
type Claims struct {
	// KeyID is the key that sealed the id.
	KeyID uint8
	// IssuedAt and ExpiresAt are unix seconds; ExpiresAt 0 is never.
	IssuedAt, ExpiresAt uint32
}

// IssuedAtOrZero returns when the id was issued, in unix seconds, or 0 for
// c nil: a v1 id's, which carries no issue time.
func (c *Claims) IssuedAtOrZero() uint32 {
	if c == nil {
		return 0
	}
	return c.IssuedAt
}

// Expired reports whether the id has expired at now, in unix seconds.
func (c Claims) Expired(now int64) bool {
	return c.ExpiresAt != 0 && now >= int64(c.ExpiresAt)
}

// Sealer seals and opens v2 client ids with one key.
type Sealer struct {
	keyID uint8
	aead  cipher.AEAD
}

// NewSealer returns a Sealer for the 32-byte key with id keyID. key should be
// a subkey kept for client ids.
func NewSealer(keyID uint8, key []byte) (*Sealer, error) {
	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		return nil, err
	}
	return &Sealer{keyID: keyID, aead: aead}, nil
}

// KeyID returns the id of the sealer's key.
func (s *Sealer) KeyID() uint8 { return s.keyID }

// Seal returns id sealed as a v2 client id, issued at issuedAt and expiring
// at expiresAt (0 never), in unix seconds. id is 12 bytes, or 20 with its
// uuid.
func (s *Sealer) Seal(id ID, issuedAt, expiresAt uint32) (string, error) {
	if len(id) != rawLen && len(id) != idLenV2 {
		return "", errors.New("client id: wrong length")
	}
	buf := make([]byte, headerLenV2+chacha20poly1305.NonceSizeX, sealedLenV2)
	buf[0], buf[1] = versionV2, s.keyID
	nonce := buf[headerLenV2:]
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	plain := make([]byte, plainLenV2)
	copy(plain, id)
	binary.BigEndian.PutUint32(plain[idLenV2:], issuedAt)
	binary.BigEndian.PutUint32(plain[idLenV2+4:], expiresAt)
	buf = s.aead.Seal(buf, nonce, plain, buf[:headerLenV2])
	return idEncoding.EncodeToString(buf), nil
}

// IsV2 reports whether text has the length of a v2 client id.
func IsV2(text []byte) bool {
	return len(text) == EncodedLenV2
}

// OpenV2 opens a v2 client id with the sealer for its key id, if any. The id
// is 20 bytes, or 12 for one without a uuid. text is not changed.
func OpenV2(text []byte, sealer func(keyID uint8) *Sealer) (ID, Claims, error) {
	if len(text) != EncodedLenV2 {
		return nil, Claims{}, ErrInvalidID
	}
	buf := make([]byte, idEncoding.DecodedLen(len(text)))
	n, err := idEncoding.Decode(buf, text)
	if err != nil || n != sealedLenV2 || buf[0] != versionV2 {
		return nil, Claims{}, ErrInvalidID
	}
	s := sealer(buf[1])
	if s == nil {
		return nil, Claims{}, ErrInvalidID
	}
	nonce := buf[headerLenV2 : headerLenV2+chacha20poly1305.NonceSizeX]
	plain, err := s.aead.Open(nil, nonce, buf[headerLenV2+chacha20poly1305.NonceSizeX:n], buf[:headerLenV2])
	if err != nil || len(plain) != plainLenV2 {
		return nil, Claims{}, ErrInvalidID
	}
	claims := Claims{
		KeyID:     buf[1],
		IssuedAt:  binary.BigEndian.Uint32(plain[idLenV2:]),
		ExpiresAt: binary.BigEndian.Uint32(plain[idLenV2+4:]),
	}
	id := ID(plain[:idLenV2])
	if id.Uuid() == 0 {
		id = id[:rawLen]
	}
	return id, claims, nil
}
