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
	"bytes"
	"errors"

	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/pkg/encoding"
	"github.com/unit-io/unitdb/server/internal/pkg/hash"
)

// Access types for a security key.
const (
	AllowNone      = uint32(0)              // Key has no privileges.
	AllowRead      = uint32(1 << 1)         // Key should be allowed to subscribe to the topic.
	AllowWrite     = uint32(1 << 2)         // Key should be allowed to publish to the topic.
	AllowReadWrite = AllowRead | AllowWrite // Key should be allowed to read and write to the topic.

	// Topic types
	TopicInvalid = uint8(iota)
	TopicStatic
	TopicWildcard

	TopicKeySeparator = '/'
	TopicSeparator    = '.' // The separator character.
	encodedLen        = 13  // string encoded len
	rawLen            = 8   // binary raw len
)

// Key errors
var (
	ErrTargetTooLong = errors.New("topic can not have more than 23 parts")
)

type splitFunc struct{}

func (splitFunc) splitTopic(c rune) bool {
	return c == TopicSeparator
}

func (splitFunc) splitKey(c rune) bool {
	return c == TopicKeySeparator
}

func (splitFunc) options(c rune) bool {
	return c == '?'
}

// Topic represents a parsed topic.
type Topic struct {
	Key       []byte // Gets or sets the API key of the topic.
	Topic     []byte // Gets or sets the topic string.
	TopicType uint8
	Size      int // Topic size without options
}

// Key represents a security key.
type Key []byte

// IsEmpty checks whether the key is empty or not.
func (k Key) IsEmpty() bool {
	return len(k) == 0
}

// Permissions gets the permission flags.
func (k Key) Permissions() uint32 {
	return uint32(k[0])
}

// SetPermissions sets the permission flags.
func (k Key) SetPermissions(value uint32) {
	k[0] = byte(value)
}

// Target returns the topic (first element of the query, second element of an parts)
func (topic *Topic) Target() uint32 {
	return hash.WithSalt(topic.Topic[:topic.Size], message.Contract)
}

// ParseKey attempts to parse the key
func ParseKey(text []byte) (topic *Topic) {
	topic = new(Topic)
	var fn splitFunc

	parts := bytes.FieldsFunc(text, fn.splitKey)
	if parts == nil || len(parts) < 2 {
		// topic.TopicType = TopicInvalid
		topic.Topic = parts[0]
		topic.Size = len(parts[0])
		return topic
	}
	topic.Key = parts[0]
	topic.Topic = parts[1]
	parts = bytes.FieldsFunc(parts[1], fn.options)
	l := len(parts)
	if parts == nil || l < 1 {
		topic.TopicType = TopicInvalid
		return topic
	}
	topic.Size = len(parts[0])

	return topic
}

// ValidateTopic validates the topic string.
func (k Key) ValidateTopic(contract uint32, topic []byte) (ok bool, wildcard bool) {
	// var fn splitFunc
	// Bytes 4-5-6-7 contains target hash
	target := uint32(k[4])<<24 | uint32(k[5])<<16 | uint32(k[6])<<8 | uint32(k[7])
	targetPath := uint32(k[1])<<16 | uint32(k[2])<<8 | uint32(k[3])
	// If there's no depth specified then default it to a single-level validation

	if targetPath == 0 {
		if target == hash.WithSalt([]byte("..."), contract) { // Key target was "..." (1472774773 == hash("..."))
			return true, true
		}
		return target == hash.WithSalt(topic, contract), true
	}

	h := hash.WithSalt(topic, contract)
	return h == target, ((targetPath >> 23) & 1) == 0
}

// SetTarget sets the topic for the key.
func (k Key) SetTarget(contract uint32, topic []byte) error {
	var fn splitFunc
	// 1st bit is 0 for wildcard, 1 for strict type
	bitPath := uint32(1 << 23)
	if bytes.HasSuffix(topic, []byte("...")) {
		bitPath = 0
		//topic = bytes.TrimRight(topic, "...")
	}

	parts := bytes.FieldsFunc(topic, fn.splitTopic)

	// Perform some validation
	if len(parts) > 23 {
		return ErrTargetTooLong
	}

	// Encode all of the parts
	for _, part := range parts {
		if bytes.HasSuffix(part, []byte{'*'}) {
			bitPath = 0
			break
		}
	}

	// Encode all of the parts
	for idx, part := range parts {
		if !bytes.HasSuffix(part, []byte{'*'}) && !bytes.HasSuffix(part, []byte("...")) {
			bitPath |= uint32(1 << (22 - uint16(idx)))
		}
	}
	value := hash.WithSalt(topic, contract)

	// Set the bit path
	k[1] = byte(bitPath >> 16)
	k[2] = byte(bitPath >> 8)
	k[3] = byte(bitPath)

	// Set the hash of the target
	k[4] = byte(value >> 24)
	k[5] = byte(value >> 16)
	k[6] = byte(value >> 8)
	k[7] = byte(value)
	return nil
}

// HasPermission check whether the key provides some permission.
func (k Key) HasPermission(flag uint32) bool {
	p := k.Permissions()
	return (p & flag) == flag
}

// GenerateKey generates a new key.
func GenerateKey(contract uint32, topic []byte, permissions uint32) (string, error) {
	key := Key(make([]byte, rawLen))
	key.SetPermissions(permissions)
	if err := key.SetTarget(contract, topic); err != nil {
		return "", err
	}

	return key.Encode(), nil
}

func (k Key) Encode() string {
	buffer := make([]byte, rawLen)
	buffer[0] = k[0]
	buffer[1] = k[1]

	// XOR the entire array with the salt
	for i := 2; i < rawLen; i += 2 {
		buffer[i] = byte(k[i] ^ buffer[0])
		buffer[i+1] = byte(k[i+1] ^ buffer[1])
	}

	text := make([]byte, encodedLen)
	encoding.Encode8(text, buffer[:])
	return string(text)
}

func DecodeKey(key []byte) (Key, error) {
	if len(key) != encodedLen {
		return Key{}, errors.New("Key provided is invalid")
	}

	// Base8 decoding is done to buffer
	buffer := make([]byte, rawLen)
	encoding.Decode8(buffer, key)

	// Resize the slice, since it is changed.
	//buffer = buffer[0:rawLen]

	// Then XOR the entire array with the salt.
	for i := 2; i < rawLen; i += 2 {
		buffer[i] = byte(buffer[i] ^ buffer[0])
		buffer[i+1] = byte(buffer[i+1] ^ buffer[1])
	}

	// Return the key on the decrypted buffer.
	return Key(buffer), nil
}
