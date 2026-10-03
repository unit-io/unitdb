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

// Package keys issues and reads client ids and topic keys with the server's
// keyring (config.Keyring), for the server and server/cmd/mintid.
//
// It issues with the issue key only, and reads with any key of the keyring:
// v2 client ids and topic keys name the key that issued them, and are sealed
// and signed with its subkeys. v1 client ids and topic keys, and unsigned
// keys, are neither issued nor read since v0.7.0; OpenV1ClientID reads a v1
// id only for server/cmd/mintid -from, to seal it again as a v2 one.
package keys

import (
	"errors"
	"time"

	"github.com/unit-io/unitdb/server/internal/config"
	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/crypto"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
)

// Set is what the server derives from its keyring.
type Set struct {
	issue uint8

	// By key id: subkeys derived per use.
	sealers  map[uint8]*uid.Sealer
	tsigners map[uint8]*security.SignerV2
	stores   map[uint8][]byte
}

// New returns the Set of keyring kr.
func New(kr *config.Keyring) (*Set, error) {
	issue := kr.Issue()
	s := &Set{
		issue:    issue.ID,
		sealers:  make(map[uint8]*uid.Sealer),
		tsigners: make(map[uint8]*security.SignerV2),
		stores:   make(map[uint8][]byte),
	}
	for _, key := range kr.Keys {
		sealer, err := uid.NewSealer(key.ID, key.Subkey(config.SubkeyClientID))
		if err != nil {
			return nil, err
		}
		s.sealers[key.ID] = sealer
		s.tsigners[key.ID] = security.NewSignerV2(key.ID, key.Subkey(config.SubkeyTopicKey))
		s.stores[key.ID] = key.Subkey(config.SubkeyStore)
	}
	return s, nil
}

// IssueKeyID returns the id of the key the set issues with.
func (s *Set) IssueKeyID() uint8 { return s.issue }

// StoreKeys returns the subkeys stored records are sealed with
// (encrypt_at_rest), by key id: the issue key's seals, and every key's
// opens.
func (s *Set) StoreKeys() map[uint8][]byte {
	out := make(map[uint8][]byte, len(s.stores))
	for id, key := range s.stores {
		out[id] = key
	}
	return out
}

// Times returns the unix times an id or key issued at now with ttl is issued
// at and expires at; ttl 0 never expires.
func Times(now time.Time, ttl time.Duration) (issuedAt, expiresAt uint32) {
	issuedAt = uint32(now.Unix())
	if ttl > 0 {
		expiresAt = uint32(now.Add(ttl).Unix())
	}
	return issuedAt, expiresAt
}

// SealClientID seals id as a v2 client id, issued now, that expires after
// ttl (0 never).
func (s *Set) SealClientID(id uid.ID, ttl time.Duration) (string, error) {
	issuedAt, expiresAt := Times(time.Now(), ttl)
	return s.SealClientIDAt(id, issuedAt, expiresAt)
}

// SealClientIDAt seals id as a v2 client id with the given unix times.
func (s *Set) SealClientIDAt(id uid.ID, issuedAt, expiresAt uint32) (string, error) {
	return s.sealers[s.issue].Seal(id, issuedAt, expiresAt)
}

// ErrV1ClientID is the error for a v1 client id, which the server refuses
// since v0.7.0: its owner seals it again as a v2 one (server/cmd/mintid
// -from), or renews it through a v0.6.0 server.
var ErrV1ClientID = errors.New("v1 client ids are no longer accepted")

// OpenClientID opens a v2 client id. It returns ErrV1ClientID for text of a
// v1 id's length, and uid.ErrInvalidID for any other id that does not open.
// It does not check the expiry.
func (s *Set) OpenClientID(text []byte) (uid.ID, uid.Claims, error) {
	if len(text) == uid.EncodedLenV1 {
		return nil, uid.Claims{}, ErrV1ClientID
	}
	return uid.OpenV2(text, func(keyID uint8) *uid.Sealer { return s.sealers[keyID] })
}

// OpenV1ClientID opens a v1 client id with each key of kr, the issue key
// first: v1 ids name no key, and were sealed with a key as it is. It is for
// server/cmd/mintid -from only, which seals the id again as a v2 one with
// the same contract and permissions; the server refuses v1 ids.
func OpenV1ClientID(kr *config.Keyring, text []byte) (uid.ID, error) {
	if len(text) != uid.EncodedLenV1 {
		return nil, uid.ErrInvalidID
	}
	issue := kr.Issue()
	ordered := []config.Key{issue}
	for _, key := range kr.Keys {
		if key.ID != issue.ID {
			ordered = append(ordered, key)
		}
	}
	for _, key := range ordered {
		mac, err := crypto.New(key.Key)
		if err != nil {
			return nil, err
		}
		// uid.DecodeV1 decodes in place: hand it a copy each time.
		if id, err := uid.DecodeV1(append([]byte(nil), text...), mac); err == nil {
			return id, nil
		}
	}
	return nil, uid.ErrInvalidID
}

// TopicKey issues a v2 key for topic, given without options, on contract,
// issued now, that expires after ttl (0 never).
func (s *Set) TopicKey(contract uint32, topic string, permissions uint32, ttl time.Duration) (string, error) {
	issuedAt, expiresAt := Times(time.Now(), ttl)
	return s.tsigners[s.issue].GenerateKey(contract, topic, permissions, issuedAt, expiresAt)
}

// TopicKeyAt issues a v2 key with the given unix times.
func (s *Set) TopicKeyAt(contract uint32, topic string, permissions uint32, issuedAt, expiresAt uint32) (string, error) {
	return s.tsigners[s.issue].GenerateKey(contract, topic, permissions, issuedAt, expiresAt)
}

// DecodeTopicKeyV2 checks a v2 key for topic, given without options, on
// contract (see security.DecodeKeyV2). It does not check the expiry.
func (s *Set) DecodeTopicKeyV2(contract uint32, text, topic string) (security.KeyV2, error) {
	return security.DecodeKeyV2(contract, text, topic, func(keyID uint8) *security.SignerV2 { return s.tsigners[keyID] })
}
