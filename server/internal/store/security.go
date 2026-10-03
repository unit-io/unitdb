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
