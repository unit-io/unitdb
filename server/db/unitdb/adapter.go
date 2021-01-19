package adapter

import (
	"encoding/json"
	"errors"
	"io"
	"os"

	"github.com/unit-io/unitdb"
	"github.com/unit-io/unitdb/memdb"
	"github.com/unit-io/unitdb/server/pkg/log"
	"github.com/unit-io/unitdb/server/store"
)

const (
	defaultDatabase = "unitdb"
	// defaultMessageStore = "messages"

	dbVersion = 1.0

	adapterName = "unitdb"

	// logPostfix = ".log"
)

type configType struct {
	Dir  string `json:"dir,omitempty"`
	Size int64  `json:"mem_size"`
	// LogReleaseDur string `json:"log_release_duration,omitempty"`
	// dur time.Duration
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
func (a *adapter) Open(jsonconfig string, reset bool) error {
	if a.db != nil {
		return errors.New("unitdb adapter is already connected")
	}

	var err error
	var config configType

	if err = json.Unmarshal([]byte(jsonconfig), &config); err != nil {
		return errors.New("unitdb adapter failed to parse config: " + err.Error())
	}

	// Make sure we have a directory
	if err := os.MkdirAll(config.Dir, 0777); err != nil {
		log.Error("adapter.Open", "Unable to create db dir")
	}

	// Attempt to open the database
	a.db, err = unitdb.Open(config.Dir+"/"+defaultDatabase, nil, unitdb.WithMutable())
	if err != nil {
		log.Error("adapter.Open", "Unable to open db")
		return err
	}
	// Attempt to open the memdb
	var opts memdb.Options
	if reset {
		opts = memdb.WithLogReset()
	}
	a.mem, err = memdb.Open(opts, memdb.WithLogFilePath(config.Dir), memdb.WithBufferSize(config.Size))
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
func (a *adapter) Put(contract uint32, topic, payload []byte) error {
	entry := unitdb.NewEntry(topic, payload)
	entry.WithContract(contract)
	return a.db.PutEntry(entry)
}

// PutWithID appends the messages to the store using a pre generated messageId.
func (a *adapter) PutWithID(contract uint32, messageId, topic, payload []byte) error {
	entry := unitdb.NewEntry(topic, payload)
	entry.WithContract(contract)
	return a.db.PutEntry(entry.WithID(messageId))
}

// Get performs a query and attempts to fetch last n messages where
// n is specified by limit argument. From and until times can also be specified
// for time-series retrieval.
func (a *adapter) Get(contract uint32, topic []byte) (matches [][]byte, err error) {
	// Iterating over key/value pairs.
	query := unitdb.NewQuery(topic)
	query.WithContract(contract)
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
func (a *adapter) Delete(contract uint32, messageId, topic []byte) error {
	entry := unitdb.NewEntry(topic, nil)
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
