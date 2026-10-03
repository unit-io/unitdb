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

package store

import (
	"bytes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync/atomic"

	"golang.org/x/crypto/chacha20poly1305"

	adapter "github.com/unit-io/unitdb/server/internal/db"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
)

// Encryption at rest (encrypt_at_rest): the store seals each record before
// the adapter sees it, with XChaCha20-Poly1305 under a random nonce, rather
// than use unitdb's own encryption, whose nonce is derived from the
// plaintext and repeats at message volumes (docs/security-review.md,
// findings 5 and 10). A sealed record is
//
//	magic (4) | key id (1) | nonce (24) | sealed record | tag (16)
//
// with the record's contract, or its key, as associated data, so a record
// moved to another contract or key fails to open. (Not its topic: a query
// on a wildcard topic returns records of several topics without saying
// which.) Topics, keys and ids are not sealed, only what is stored under
// them.
//
// Records are sealed with the subkey of the keyring's issue key, and opened
// with the subkey of the key they name, so a key rotated to read only still
// opens what it sealed. Records without the magic are read as they are:
// records stored before sealing was turned on, by an older version, or
// while it is off. Sealed records are opened whether sealing is on or off.
//
// Sealing happens here, at the adapter, so everything above the store, and
// everything the cluster sends between nodes (replicas, hints, session logs
// and rows, history for a rebuild), is plaintext: each node seals what it
// stores with its own setting, and needs no other node's at-rest state.
var sealMagic = [4]byte{0xE5, 0x7A, 0x1C, 0x5E}

const sealHeaderLen = len(sealMagic) + 1 + chacha20poly1305.NonceSizeX

// SealOverhead is what sealing adds to a record.
const SealOverhead = sealHeaderLen + chacha20poly1305.Overhead

var (
	// ErrUnknownSealKey is returned for a sealed record whose key is not in
	// the keyring: the key was removed before every record it sealed
	// expired or was rewritten.
	ErrUnknownSealKey = errors.New("store: the record is sealed with a key that is not in the keyring")
	// ErrSealBroken is returned for a sealed record that does not open with
	// the key it names: it was changed on disk, or moved from another
	// contract or key.
	ErrSealBroken = errors.New("store: a sealed record does not open with the key it names")
)

type sealer struct {
	issue uint8
	aeads map[uint8]cipher.AEAD
	// seal is set when records are sealed as they are written.
	seal bool
}

// SetSealing has the store open sealed records with keys, the store
// subkeys of the keyring by key id, and, if seal is set, seal the records it
// writes with the key of id issue. Call it before Open, so that the topic
// index is read with it; it may be called again, as tests do.
func SetSealing(issue uint8, keys map[uint8][]byte, seal bool) error {
	s := &sealer{issue: issue, aeads: make(map[uint8]cipher.AEAD), seal: seal}
	for id, key := range keys {
		aead, err := chacha20poly1305.NewX(key)
		if err != nil {
			return fmt.Errorf("store: sealing key %d: %v", id, err)
		}
		s.aeads[id] = aead
	}
	if seal && s.aeads[issue] == nil {
		return fmt.Errorf("store: no sealing key of id %d", issue)
	}
	if adp == nil {
		return errors.New("store: database adapter is missing")
	}
	if a, ok := adp.(*sealingAdapter); ok {
		a.s.Store(s)
		return nil
	}
	a := &sealingAdapter{Adapter: adp}
	a.s.Store(s)
	adp = a
	return nil
}

func contractAD(contract uint32) []byte {
	ad := []byte{'c', 0, 0, 0, 0}
	binary.BigEndian.PutUint32(ad[1:], contract)
	return ad
}

func keyAD(key uint64) []byte {
	ad := []byte{'k', 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(ad[1:], key)
	return ad
}

func (s *sealer) sealRecord(b, ad []byte) ([]byte, error) {
	if !s.seal {
		return b, nil
	}
	out := make([]byte, sealHeaderLen)
	copy(out, sealMagic[:])
	out[len(sealMagic)] = s.issue
	nonce := out[len(sealMagic)+1:]
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return s.aeads[s.issue].Seal(out, nonce, b, ad), nil
}

// isSealed reports whether b is a sealed record.
func isSealed(b []byte) bool {
	return len(b) >= SealOverhead && bytes.Equal(b[:len(sealMagic)], sealMagic[:])
}

// openRecord opens b if it is sealed, and returns it as it is if not.
func (s *sealer) openRecord(b, ad []byte) ([]byte, error) {
	if !isSealed(b) {
		return b, nil
	}
	id := b[len(sealMagic)]
	aead := s.aeads[id]
	if aead == nil {
		return nil, fmt.Errorf("%w (key id %d)", ErrUnknownSealKey, id)
	}
	plain, err := aead.Open(nil, b[len(sealMagic)+1:sealHeaderLen], b[sealHeaderLen:], ad)
	if err != nil {
		return nil, fmt.Errorf("%w (key id %d)", ErrSealBroken, id)
	}
	return plain, nil
}

// sealingAdapter seals what it stores and opens what it reads.
type sealingAdapter struct {
	adapter.Adapter
	s atomic.Pointer[sealer]
}

func (a *sealingAdapter) Put(contract uint32, topic string, payload []byte, ttl string) error {
	b, err := a.s.Load().sealRecord(payload, contractAD(contract))
	if err != nil {
		return err
	}
	return a.Adapter.Put(contract, topic, b, ttl)
}

func (a *sealingAdapter) PutWithID(contract uint32, messageId []byte, topic string, payload []byte, ttl string) error {
	b, err := a.s.Load().sealRecord(payload, contractAD(contract))
	if err != nil {
		return err
	}
	return a.Adapter.PutWithID(contract, messageId, topic, b, ttl)
}

// Get opens the records it gets, and skips, and logs, the ones that do not
// open.
func (a *sealingAdapter) Get(contract uint32, topic string, last string) ([][]byte, error) {
	raw, err := a.Adapter.Get(contract, topic, last)
	if err != nil {
		return raw, err
	}
	s := a.s.Load()
	out := raw[:0]
	var skipped int
	var first error
	for _, b := range raw {
		plain, err := s.openRecord(b, contractAD(contract))
		if err != nil {
			if skipped == 0 {
				first = err
			}
			skipped++
			continue
		}
		out = append(out, plain)
	}
	if skipped > 0 {
		log.ErrLogger.Error().Err(first).Str("context", "store.Get").Uint32("contract", contract).Int("skipped", skipped).Msg("skipped sealed records that do not open")
	}
	return out, nil
}

func (a *sealingAdapter) PutMessage(key uint64, payload []byte) error {
	b, err := a.s.Load().sealRecord(payload, keyAD(key))
	if err != nil {
		return err
	}
	return a.Adapter.PutMessage(key, b)
}

func (a *sealingAdapter) GetMessage(key uint64) ([]byte, error) {
	b, err := a.Adapter.GetMessage(key)
	if err != nil {
		return b, err
	}
	plain, err := a.s.Load().openRecord(b, keyAD(key))
	if err != nil {
		log.ErrLogger.Error().Err(err).Str("context", "store.GetMessage").Uint64("key", key).Msg("a sealed record does not open")
	}
	return plain, err
}
