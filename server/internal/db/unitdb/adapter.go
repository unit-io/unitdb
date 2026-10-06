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
	"sync"
	"sync/atomic"
	"time"

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
	// path is the directory the store is in.
	path string
	// wmu is held for reading by every write, and for writing by Checkpoint,
	// so that a checkpoint copies the store between writes, and by Close.
	wmu sync.RWMutex
	// lastWrite is when the DB was last written to, in unix nanoseconds.
	lastWrite atomic.Int64
	// stop stops the compactor (startCompactor).
	stop chan struct{}

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
	a.path = path
	a.compact()
	a.startCompactor(compactEvery)

	return nil
}

// compactEvery is how often a running store compacts its message log.
var compactEvery = time.Minute

// startCompactor compacts the message log every interval, until Close: a
// message kept for long, a session's row or a publish waiting on a
// subscriber away, holds its block, and the WAL keeps every log chaining
// deletes back to it (memdb's Compact).
func (a *adapter) startCompactor(interval time.Duration) {
	stop := make(chan struct{})
	a.stop = stop
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				a.compact()
			}
		}
	}()
}

// compact moves the messages left in mostly dead blocks, so that the blocks,
// and the WAL's logs of them, go. It is a write: a checkpoint waits for it.
func (a *adapter) compact() {
	a.wmu.RLock()
	defer a.wmu.RUnlock()
	if a.mem == nil {
		return
	}
	if n, err := a.mem.Compact(); err != nil {
		log.Error("adapter.compact", err.Error())
	} else if n > 0 {
		log.Info("adapter.compact", fmt.Sprintf("moved %d messages", n))
	}
}

// Close closes the underlying database connection
func (a *adapter) Close() error {
	a.wmu.Lock()
	defer a.wmu.Unlock()
	if a.stop != nil {
		close(a.stop)
		a.stop = nil
	}
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
	a.wmu.RLock()
	defer a.wmu.RUnlock()
	if a.db == nil {
		return errClosed
	}
	defer a.lastWrite.Store(time.Now().UnixNano())
	entry := unitdb.NewEntry([]byte(topic), payload).WithContract(contract)
	if ttl != "" {
		entry.WithTTL(ttl)
	}
	return a.db.PutEntry(entry)
}

// PutWithID appends the messages to the store using a pre generated messageId.
func (a *adapter) PutWithID(contract uint32, messageId []byte, topic string, payload []byte, ttl string) error {
	a.wmu.RLock()
	defer a.wmu.RUnlock()
	if a.db == nil {
		return errClosed
	}
	defer a.lastWrite.Store(time.Now().UnixNano())
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
	a.wmu.RLock()
	defer a.wmu.RUnlock()
	if a.db == nil {
		return errClosed
	}
	defer a.lastWrite.Store(time.Now().UnixNano())
	entry := unitdb.NewEntry([]byte(topic), nil)
	entry.WithContract(contract)
	return a.db.DeleteEntry(entry.WithID(messageId))
}

// PutMessage puts payload as the key's value, in place of its last.
func (a *adapter) PutMessage(key uint64, payload []byte) error {
	a.wmu.RLock()
	defer a.wmu.RUnlock()
	if a.mem == nil {
		return errClosed
	}
	_, err := a.mem.Put(key, payload)
	return err
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
	return a.mem.Keys()
}

// Flush waits for the messages put before it to reach the store's log.
func (a *adapter) Flush() error {
	if a.db == nil {
		return errClosed
	}
	return a.db.Flush()
}

// DeleteMessage deletes the key's value; a key with none is deleted.
func (a *adapter) DeleteMessage(key uint64) error {
	a.wmu.RLock()
	defer a.wmu.RUnlock()
	if a.mem == nil {
		return errClosed
	}
	if err := a.mem.Delete(key); err != nil && !errors.Is(err, memdb.ErrNotFound) {
		return err
	}
	return nil
}

func init() {
	adp := &adapter{}
	store.RegisterAdapter(adapterName, adp)
}
