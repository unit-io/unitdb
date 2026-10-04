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

package adapter

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/unit-io/unitdb"
	"github.com/unit-io/unitdb/memdb"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
)

const (
	defaultDatabase = "unitdb"
	// defaultMessageStore = "messages"

	dbVersion = 1.0

	adapterName = "unitdb"

	// logPostfix = ".log"
)

type configType struct {
	Size int64 `json:"mem_size"`
}

const (
	// Maximum number of records to return
	maxResults = 1024
	// Maximum TTL for message
	maxTTL = "24h"
)

// Store represents an SSD-optimized storage store.
type adapter struct {
	db      *unitdb.DB // The underlying database to store messages.
	mem     *memdb.DB  // The underlying memdb to store messages.
	config  *configType
	version int

	// close
	closer io.Closer
}

// Open initializes database connection
func (a *adapter) Open(path, jsonconfig string, reset bool) error {
	if a.db != nil {
		return errors.New("unitdb adapter is already connected")
	}

	var err error
	var config configType

	if err = json.Unmarshal([]byte(jsonconfig), &config); err != nil {
		return errors.New("unitdb adapter failed to parse config: " + err.Error())
	}

	// Make sure we have a directory
	if err := os.MkdirAll(path, 0750); err != nil {
		log.Error("adapter.Open", "Unable to create db dir")
	}

	// Attempt to open the database
	a.db, err = unitdb.Open(path+"/"+defaultDatabase, nil, unitdb.WithMutable())
	if err != nil {
		log.Error("adapter.Open", "Unable to open db")
		return err
	}
	// Attempt to open the memdb
	var opts memdb.Options
	if reset {
		opts = memdb.WithLogReset()
	}
	a.mem, err = memdb.Open(opts, memdb.WithLogFilePath(path), memdb.WithBufferSize(config.Size))
	if err != nil {
		return err
	}

	a.config = &config

	return nil
}

// Close closes the underlying database connection
func (a *adapter) Close() error {
	var err error
	if a.db != nil {
		err = a.db.Close()
		a.db = nil
		a.version = -1
	}
	if a.mem != nil {
		if err1 := a.mem.Close(); err == nil {
			err = err1
		}
		a.mem = nil
	}
	return err
}

// errClosed is returned by the calls made once the adapter is closed: they
// dereferenced the closed store, and panicked.
var errClosed = errors.New("unitdb adapter: closed")

// IsOpen returns true if connection to database has been established. It does not check if
// connection is actually live.
func (a *adapter) IsOpen() bool {
	return a.db != nil
}

// GetName returns string that adapter uses to register itself with store.
func (a *adapter) GetName() string {
	return adapterName
}

// Put appends the messages to the store.
func (a *adapter) Put(contract uint32, topic string, payload []byte, ttl string) error {
	if a.db == nil {
		return errClosed
	}
	entry := unitdb.NewEntry([]byte(topic), payload).WithContract(contract)
	if ttl != "" {
		entry.WithTTL(ttl)
	}
	return a.db.PutEntry(entry)
}

// PutWithID appends the messages to the store using a pre generated messageId.
func (a *adapter) PutWithID(contract uint32, messageId []byte, topic string, payload []byte, ttl string) error {
	if a.db == nil {
		return errClosed
	}
	entry := unitdb.NewEntry([]byte(topic), payload).WithContract(contract).WithID(messageId)
	if ttl != "" {
		entry.WithTTL(ttl)
	}
	return a.db.PutEntry(entry)
}

// Get performs a query and attempts to fetch last messages where
// last is specified by last duration argument.
func (a *adapter) Get(contract uint32, topic string, last string) (matches [][]byte, err error) {
	if a.db == nil {
		return nil, errClosed
	}
	// Iterating over key/value pairs.
	query := unitdb.NewQuery([]byte(topic)).WithContract(contract)
	if last != "" {
		query.WithLast(last)
	}

	return a.db.Get(query)
}

// GetWithIDs gets the messages stored on contract under topic, and the id of
// each, with which Delete deletes it.
func (a *adapter) GetWithIDs(contract uint32, topic string) (ids, payloads [][]byte, err error) {
	if a.db == nil {
		return nil, nil, errClosed
	}
	return a.db.GetWithIDs(unitdb.NewQuery([]byte(topic)).WithContract(contract))
}

// Count returns the number of messages in the message store.
func (a *adapter) Count() uint64 {
	if a.db == nil {
		return 0
	}
	return a.db.Count()
}

// NewID generates a new messageId.
func (a *adapter) NewID() ([]byte, error) {
	if a.db == nil {
		return nil, errClosed
	}
	id := a.db.NewID()
	if id == nil {
		return nil, errors.New("Key is empty.")
	}
	return id, nil
}

// Put appends the messages to the store.
func (a *adapter) Delete(contract uint32, messageId []byte, topic string) error {
	if a.db == nil {
		return errClosed
	}
	entry := unitdb.NewEntry([]byte(topic), nil)
	entry.WithContract(contract)
	return a.db.DeleteEntry(entry.WithID(messageId))
}

// PutMessage appends the messages to the store.
//
// memdb keeps a version of a key for each time block the key was put in, and
// a get returns the latest: the older versions are deleted first, so that a
// later delete removes the key.
func (a *adapter) PutMessage(key uint64, payload []byte) error {
	if a.mem == nil {
		return errClosed
	}
	if err := a.deleteVersions(key); err != nil {
		return err
	}
	if _, err := a.mem.Put(key, payload); err != nil {
		return err
	}
	return nil
}

// GetMessage performs a query and attempts to fetch message for the given key
func (a *adapter) GetMessage(key uint64) (matches []byte, err error) {
	if a.mem == nil {
		return nil, errClosed
	}
	matches, err = a.mem.Get(key)
	if err != nil {
		return nil, err
	}
	return matches, nil
}

// Keys performs a query and attempts to fetch all keys.
func (a *adapter) Keys() []uint64 {
	if a.mem == nil {
		return nil
	}
	// memdb lists a key once for each version of it.
	keys := a.mem.Keys()
	seen := make(map[uint64]bool, len(keys))
	unique := keys[:0]
	for _, key := range keys {
		if !seen[key] {
			seen[key] = true
			unique = append(unique, key)
		}
	}
	return unique
}

// Flush waits for the messages put before it to reach the store's log.
func (a *adapter) Flush() error {
	if a.db == nil {
		return errClosed
	}
	return a.db.Flush()
}

// DeleteMessage deletes message from memdb store.
func (a *adapter) DeleteMessage(key uint64) error {
	if a.mem == nil {
		return errClosed
	}
	return a.deleteVersions(key)
}

// maxKeyVersions bounds the versions of a key deleteVersions deletes.
const maxKeyVersions = 64

// deleteVersions deletes every version of key: memdb deletes the latest one
// only, and a get then returns the one before. PutMessage deletes a key's
// versions before it puts one, so a key has one, or a few put at once.
//
// It returned nil past maxKeyVersions, and on any error of Get, such as the
// store closed: a delete reported done left versions a get still found.
func (a *adapter) deleteVersions(key uint64) error {
	for i := 0; i < maxKeyVersions; i++ {
		err := a.mem.Delete(key)
		if errors.Is(err, memdb.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
	}
	if _, err := a.mem.Get(key); errors.Is(err, memdb.ErrNotFound) {
		return nil
	}
	return fmt.Errorf("unitdb adapter: key %d has more than %d versions; deleted %d", key, maxKeyVersions, maxKeyVersions)
}

func init() {
	adp := &adapter{}
	store.RegisterAdapter(adapterName, adp)
}
