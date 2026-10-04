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
	"crypto/rand"
	"errors"
	"fmt"
	"hash/fnv"

	adapter "github.com/unit-io/unitdb/server/internal/db"
)

// securityTopic is the topic the cluster's security state is kept under, in
// the node's own namespace (namespaces.go).
var securityTopic = sysTopic(sysSecurity, "state")

// SecurityStore keeps the cluster's security state: what was revoked in each
// contract. Each record is a whole copy of the state, which the caller
// encodes; a new one is written on each change, and the ones before deleted
// after it. Records merge whatever their order, so a crash between the write
// and the deletes, which leaves several, loses nothing.
type SecurityStore struct{}

// Security is the anchor for the security state.
var Security SecurityStore

// NewID returns an id to store a record of the state under.
func (SecurityStore) NewID() ([]byte, error) {
	return adp.NewID()
}

// Put stores a record of the state under id, and waits for it to be written
// to the store's log, so that it is recovered after a crash.
func (SecurityStore) Put(id, payload []byte) error {
	if err := adp.PutWithID(sysContract, id, securityTopic, payload, ""); err != nil {
		return err
	}
	return adp.Flush()
}

// All returns the stored records of the state, up to the store's query
// limit: one, or a few after a crash.
func (SecurityStore) All() ([][]byte, error) {
	return adp.Get(sysContract, securityTopic, "")
}

// Delete deletes the record stored under id.
func (SecurityStore) Delete(id []byte) error {
	return adp.Delete(sysContract, id, securityTopic)
}

// Legacy returns the records of the state a v0.6.0 node stored, under a
// fixed id, and their ids. The caller merges them into the state, writes it
// with Put, and then deletes them with DeleteLegacy: a crash in between
// leaves them to be merged again, which changes nothing.
func (SecurityStore) Legacy() (ids, records [][]byte, err error) {
	return adp.GetWithIDs(legacySecurityStoreId, legacySecurityTopic)
}

// DeleteLegacy deletes a record of the state a v0.6.0 node stored, by its id
// as Legacy returns it.
func (SecurityStore) DeleteLegacy(id []byte) error {
	return adp.Delete(legacySecurityStoreId, id, legacySecurityTopic)
}

// probeKey is the memdb key the health probe writes: a hash of a string no
// other record's key comes from.
var probeKey = func() uint64 {
	h := fnv.New64a()
	h.Write([]byte("\x00unitdb-health-probe"))
	return h.Sum64()
}()

// Probe writes a record and reads it back, for health checks. It writes this
// node's store only, through the adapter, so nothing replicates it, and under
// a key of its own, so it can't clash with a real one. Sealing applies, as
// to every record.
func Probe() error {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return err
	}
	adp.DeleteMessage(probeKey)
	if err := adp.PutMessage(probeKey, b[:]); err != nil {
		return fmt.Errorf("store probe: write: %v", err)
	}
	got, err := adp.GetMessage(probeKey)
	if err != nil {
		return fmt.Errorf("store probe: read: %v", err)
	}
	if !bytes.Equal(got, b[:]) {
		return errors.New("store probe: read back something else than it wrote")
	}
	return nil
}

// Stats is the size of the store.
type Stats = adapter.Stats

// Checkpoint writes a copy of the store into dst, a directory that doesn't
// exist or is empty, that opens as the store was at one moment (db_path set
// to dst). Writes wait while it runs. Records sealed at rest stay sealed: the
// copy opens with the same keyring only. Errors start "store checkpoint: ".
func Checkpoint(dst string) error {
	return adp.Checkpoint(dst)
}

// StoreStats returns the size of the store, cheaply: for metrics.
func StoreStats() Stats {
	return adp.Stats()
}
