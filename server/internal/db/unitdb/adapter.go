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
	if err := os.MkdirAll(path, 0777); err != nil {
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
		err = a.mem.Close()
		a.mem = nil
	}
	return err
}

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
	entry := unitdb.NewEntry([]byte(topic), payload).WithContract(contract)
	if ttl != "" {
		entry.WithTTL(ttl)
	}
	return a.db.PutEntry(entry)
}

// PutWithID appends the messages to the store using a pre generated messageId.
func (a *adapter) PutWithID(contract uint32, messageId []byte, topic string, payload []byte, ttl string) error {
	entry := unitdb.NewEntry([]byte(topic), payload).WithContract(contract).WithID(messageId)
	if ttl != "" {
		entry.WithTTL(ttl)
	}
	return a.db.PutEntry(entry)
}

// Get performs a query and attempts to fetch last messages where
// last is specified by last duration argument.
func (a *adapter) Get(contract uint32, topic string, last string) (matches [][]byte, err error) {
	// Iterating over key/value pairs.
	query := unitdb.NewQuery([]byte(topic)).WithContract(contract)
	if last != "" {
		query.WithLast(last)
	}

	return a.db.Get(query)
}

// NewID generates a new messageId.
func (a *adapter) NewID() ([]byte, error) {
	id := a.db.NewID()
	if id == nil {
		return nil, errors.New("Key is empty.")
	}
	return id, nil
}

// Put appends the messages to the store.
func (a *adapter) Delete(contract uint32, messageId []byte, topic string) error {
	entry := unitdb.NewEntry([]byte(topic), nil)
	entry.WithContract(contract)
	return a.db.DeleteEntry(entry.WithID(messageId))
}

// PutMessage appends the messages to the store.
func (a *adapter) PutMessage(key uint64, payload []byte) error {
	if _, err := a.mem.Put(key, payload); err != nil {
		return err
	}
	return nil
}

// GetMessage performs a query and attempts to fetch message for the given key
func (a *adapter) GetMessage(key uint64) (matches []byte, err error) {
	matches, err = a.mem.Get(key)
	if err != nil {
		return nil, err
	}
	return matches, nil
}

// Keys performs a query and attempts to fetch all keys.
func (a *adapter) Keys() []uint64 {
	return a.mem.Keys()
}

// DeleteMessage deletes message from memdb store.
func (a *adapter) DeleteMessage(key uint64) error {
	if err := a.mem.Delete(key); err != nil {
		return err
	}
	return nil
}

func init() {
	adp := &adapter{}
	store.RegisterAdapter(adapterName, adp)
}
