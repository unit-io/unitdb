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

package security

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
)

// Topic key errors.
var (
	ErrInvalidKey       = errors.New("Key provided is invalid")
	ErrInvalidSignature = errors.New("Key signature is invalid")
)

// V1KeyLen and UnsignedKeyLen are the lengths of a v1 signed topic key and
// of an unsigned one, which servers up to v0.6.0 took. They are refused
// since v0.7.0: the lengths only tell a client why.
const (
	V1KeyLen       = 26
	UnsignedKeyLen = encodedLen
)

// A v2 topic key carries no hash of its topic: its tag covers the whole topic
// string, so it opens exactly the topic it was issued for, and says nothing
// of the contract.
//
//	version (1) | key id (1) | permissions (1) | flags (1) | key uuid (8) |
//	issued at (4) | expires at (4) | tag (16)
//
// in base64url without padding: 48 characters of A-Z, a-z, 0-9, '-' and
// '_', none of them the '/' that separates a key from its topic. (A v1
// signed key was 26 characters, an unsigned one 13; neither is taken since
// v0.7.0.) The tag is HMAC-SHA256,
// under the signer's key, of a label, the contract, the bytes before it, and
// the topic's length and bytes, cut to 16 bytes.
//
// As a v1 key for "..." did, a v2 key issued for "..." opens every topic of
// its contract, to read: it has flagAnyTopic.
const (
	versionV2    = 0x02
	bodyLenV2    = 20
	tagLenV2     = 16
	rawLenV2     = bodyLenV2 + tagLenV2
	flagWildcard = 1 << 0
	flagAnyTopic = 1 << 1

	// KeyLenV2 is the length of an encoded v2 topic key.
	KeyLenV2 = (rawLenV2*8 + 5) / 6 // 48

	anyTopic = "..."
)

var keyEncodingV2 = base64.RawURLEncoding

// KeyV2 is what a v2 topic key grants.
type KeyV2 struct {
	KeyID       uint8
	Permissions uint32
	// Wildcard is set for a key issued for a wildcard pattern: it is not
	// taken to publish.
	Wildcard bool
	// Uuid identifies the key.
	Uuid uint64
	// IssuedAt and ExpiresAt are unix seconds; ExpiresAt 0 is never.
	IssuedAt, ExpiresAt uint32
}

// HasPermission reports whether the key grants every permission in flag.
func (k KeyV2) HasPermission(flag uint32) bool {
	return k.Permissions&flag == flag
}

// Expired reports whether the key has expired at now, in unix seconds.
func (k KeyV2) Expired(now int64) bool {
	return k.ExpiresAt != 0 && now >= int64(k.ExpiresAt)
}

// SignerV2 issues and checks v2 topic keys with one key.
type SignerV2 struct {
	keyID uint8
	key   []byte
}

// NewSignerV2 returns a SignerV2 for key, with id keyID. key should be a
// subkey kept for topic keys.
func NewSignerV2(keyID uint8, key []byte) *SignerV2 {
	return &SignerV2{keyID: keyID, key: append([]byte(nil), key...)}
}

func (s *SignerV2) tag(contract uint32, body []byte, topic string) []byte {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte("unitdb topic key v2"))
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], contract)
	m.Write(b[:])
	m.Write(body)
	binary.BigEndian.PutUint32(b[:], uint32(len(topic)))
	m.Write(b[:])
	m.Write([]byte(topic))
	return m.Sum(nil)[:tagLenV2]
}

// isWildcardPattern reports whether topic is a wildcard pattern: one with a
// part ending in '*', or ending in "...".
func isWildcardPattern(topic string) bool {
	if strings.HasSuffix(topic, anyTopic) {
		return true
	}
	for _, part := range strings.Split(topic, string(TopicSeparator)) {
		if strings.HasSuffix(part, "*") {
			return true
		}
	}
	return false
}

// GenerateKey issues a v2 key for topic, given without options, on contract.
func (s *SignerV2) GenerateKey(contract uint32, topic string, permissions uint32, issuedAt, expiresAt uint32) (string, error) {
	if strings.Count(topic, string(TopicSeparator)) >= 23 {
		return "", ErrTargetTooLong
	}
	buf := make([]byte, bodyLenV2, rawLenV2)
	buf[0], buf[1], buf[2] = versionV2, s.keyID, byte(permissions)
	if isWildcardPattern(topic) {
		buf[3] |= flagWildcard
	}
	if topic == anyTopic {
		buf[3] |= flagAnyTopic
	}
	if _, err := rand.Read(buf[4:12]); err != nil {
		return "", err
	}
	binary.BigEndian.PutUint32(buf[12:16], issuedAt)
	binary.BigEndian.PutUint32(buf[16:20], expiresAt)
	buf = append(buf, s.tag(contract, buf[:bodyLenV2], topic)...)
	return keyEncodingV2.EncodeToString(buf), nil
}

// DecodeKeyV2 checks a v2 key for topic, given without options, on contract,
// with the signer for its key id, and returns what it grants. It returns
// ErrInvalidKey for text that isn't a v2 key, and ErrInvalidSignature for a
// key not issued for this topic and contract, or by none of the signers.
// It does not check the expiry.
func DecodeKeyV2(contract uint32, text, topic string, signer func(keyID uint8) *SignerV2) (KeyV2, error) {
	if len(text) != KeyLenV2 {
		return KeyV2{}, ErrInvalidKey
	}
	buf, err := keyEncodingV2.DecodeString(text)
	if err != nil || len(buf) != rawLenV2 || buf[0] != versionV2 {
		return KeyV2{}, ErrInvalidKey
	}
	s := signer(buf[1])
	if s == nil {
		return KeyV2{}, ErrInvalidSignature
	}
	body, tag := buf[:bodyLenV2], buf[bodyLenV2:]
	if !hmac.Equal(tag, s.tag(contract, body, topic)) &&
		(buf[3]&flagAnyTopic == 0 || !hmac.Equal(tag, s.tag(contract, body, anyTopic))) {
		return KeyV2{}, ErrInvalidSignature
	}
	return KeyV2{
		KeyID:       buf[1],
		Permissions: uint32(buf[2]),
		Wildcard:    buf[3]&flagWildcard != 0,
		Uuid:        binary.BigEndian.Uint64(buf[4:12]),
		IssuedAt:    binary.BigEndian.Uint32(buf[12:16]),
		ExpiresAt:   binary.BigEndian.Uint32(buf[16:20]),
	}, nil
}

// KeyUuidV2 returns the uuid of a v2 key, or 0 for text that isn't one.
func KeyUuidV2(text string) uint64 {
	buf, err := keyEncodingV2.DecodeString(text)
	if err != nil || len(buf) != rawLenV2 || buf[0] != versionV2 {
		return 0
	}
	return binary.BigEndian.Uint64(buf[4:12])
}
