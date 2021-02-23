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
	"encoding/json"
	"errors"
	"fmt"

	adapter "github.com/unit-io/unitdb/server/internal/db"
	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
)

const (
	// Maximum number of records to return
	maxResults         = 1024
	connStoreId uint32 = 4105991048 // hash("connectionstore")
)

var adp adapter.Adapter

type configType struct {
	// Configurations for individual adapters.
	Adapters map[string]json.RawMessage `json:"adapters"`
}

func openAdapter(path, jsonconf string, reset bool) error {
	var config configType
	if err := json.Unmarshal([]byte(jsonconf), &config); err != nil {
		return errors.New("store: failed to parse config: " + err.Error() + "(" + jsonconf + ")")
	}

	if adp == nil {
		return errors.New("store: database adapter is missing")
	}

	if adp.IsOpen() {
		return errors.New("store: connection is already opened")
	}

	var adapterConfig string
	if config.Adapters != nil {
		adapterConfig = string(config.Adapters[adp.GetName()])
	}

	return adp.Open(path, adapterConfig, reset)
}

// Open initializes the persistence system. Adapter holds a connection pool for a database instance.
// 	 name - name of the adapter rquested in the config file
//   jsonconf - configuration string
func Open(path, jsonconf string, reset bool) error {
	if err := openAdapter(path, jsonconf, reset); err != nil {
		return err
	}

	return nil
}

// Close terminates connection to persistent storage.
func Close() error {
	if adp.IsOpen() {
		return adp.Close()
	}

	return nil
}

// IsOpen checks if persistent storage connection has been initialized.
func IsOpen() bool {
	if adp != nil {
		return adp.IsOpen()
	}

	return false
}

// GetAdapterName returns the name of the current adater.
func GetAdapterName() string {
	if adp != nil {
		return adp.GetName()
	}

	return ""
}

// InitDb open the db connection. If jsconf is nil it will assume that the connection is already open.
// If it's non-nil, it will use the config string to open the DB connection first.
func InitDb(path, jsonconf string, reset bool) error {
	if !IsOpen() {
		if err := openAdapter(path, jsonconf, reset); err != nil {
			return err
		}
	}
	panic("store: Init DB error")
}

// RegisterAdapter makes a persistence adapter available.
// If Register is called twice or if the adapter is nil, it panics.
func RegisterAdapter(name string, a adapter.Adapter) {
	if a == nil {
		panic("store: Register adapter is nil")
	}

	if adp != nil {
		panic("store: adapter '" + adp.GetName() + "' is already registered")
	}

	adp = a
}

// SubscriptionStore is a Subscription struct to hold methods for persistence mapping for the subscription.
// Note, do not use same contract as messagestore
type SubscriptionStore struct{}

// Message is the ancor for storing/retrieving Message objects
var Subscription SubscriptionStore

func (s *SubscriptionStore) Put(contract uint32, messageId, topic, payload []byte) error {
	return adp.PutWithID(contract^connStoreId, messageId, topic, payload, "")
}

func (s *SubscriptionStore) Get(contract uint32, topic []byte) (matches [][]byte, err error) {
	resp, err := adp.Get(contract^connStoreId, topic, "")
	for _, payload := range resp {
		if payload == nil {
			continue
		}
		matches = append(matches, payload)
	}

	return matches, err
}

func (s *SubscriptionStore) NewID() ([]byte, error) {
	return adp.NewID()
}

func (s *SubscriptionStore) Delete(contract uint32, messageId, topic []byte) error {
	return adp.Delete(contract^connStoreId, messageId, topic)
}

// MessageStore is a Message struct to hold methods for persistence mapping for the Message object.
type MessageStore struct{}

// Message is the anchor for storing/retrieving Message objects
var Message MessageStore

func (m *MessageStore) Put(contract uint32, topic, payload []byte, ttl string) error {
	return adp.Put(contract, topic, payload, ttl)
}

func (m *MessageStore) Get(contract uint32, topic []byte, last string) (matches []message.Message, err error) {
	resp, err := adp.Get(contract, topic, last)
	for _, payload := range resp {
		msg := message.Message{
			Topic:   topic,
			Payload: payload,
		}
		matches = append(matches, msg)
	}

	return matches, err
}

// MessageLog is a Message struct to hold methods for persistence mapping for the Message object.
type MessageLog struct{}

// Log is the anchor for storing/retrieving Message objects
var Log MessageLog

// PersistOutbound handles which outgoing messages are stored
func (l *MessageLog) PersistOutbound(proto net.ProtoAdapter, blockID uint32, msg net.LineProtocol) {
	switch msg.Info().DeliveryMode {
	case 0:
		switch msg.(type) {
		case *net.Pubcomplete:
			// Sending pubcomp. delete matching publish for EXPRESS delivery mode
			// or pubrecv for RELIABLE delivery mode from ibound
			ikey := uint64(blockID)<<32 + uint64(msg.Info().MessageID)
			adp.DeleteMessage(ikey)
		}
	case 1, 2:
		switch msg.(type) {
		case *net.Pubreceipt, *net.Publish:
			// Sending publish. store in obound
			// until puback received
			okey := uint64(msg.Info().MessageID)<<32 + uint64(blockID)
			m, err := net.Encode(proto, msg)
			if err != nil {
				log.ErrLogger.Err(err).Str("context", "store.PersistOutbound")
				return
			}
			adp.PutMessage(okey, m.Bytes())
		default:
			fmt.Println("Store::PersistOutbound: Invalid message type")
		}
	}
}

// PersistInbound handles which incoming messages are stored
func (l *MessageLog) PersistInbound(proto net.ProtoAdapter, blockID uint32, msg net.LineProtocol) {
	switch msg.Info().DeliveryMode {
	case 0:
		switch msg.(type) {
		case *net.Pubcomplete:
			// Received a pubcomp. delete matching publish for EXPRESS delivery mode
			// or pubrec for RELIABLE delivert mode from obound
			okey := uint64(msg.Info().MessageID)<<32 + uint64(blockID)
			adp.DeleteMessage(okey)
		case *net.Publish:
			// Received a publish. store it in ibound
			// until pubcomp sent
			ikey := uint64(blockID)<<32 + uint64(msg.Info().MessageID)
			m, err := net.Encode(proto, msg)
			if err != nil {
				log.ErrLogger.Err(err).Str("context", "store.PersistInbound")
				return
			}
			adp.PutMessage(ikey, m.Bytes())
		case *net.Pingreq, *net.Connect, *net.Disconnect:
		default:
			fmt.Println("Store::PersistInbound: Invalid message type")
		}
	case 1, 2:
		switch msg.(type) {
		case *net.Pubreceipt:
			// Received a pubrec. delete matching publish
			// from obound
			okey := uint64(msg.Info().MessageID)<<32 + uint64(blockID)
			adp.DeleteMessage(okey)
		case *net.Pubreceive, *net.Subscribe, *net.Unsubscribe:
			// Received a pubrecv. store it in ibound
			// until pubcomp sent
			ikey := uint64(blockID)<<32 + uint64(msg.Info().MessageID)
			m, err := net.Encode(proto, msg)
			if err != nil {
				log.ErrLogger.Err(err).Str("context", "store.PersistInbound")
				return
			}
			adp.PutMessage(ikey, m.Bytes())
		case *net.Publish:
		default:
			fmt.Println("Store::PersistInbound: Invalid message type")
		}
	}
}

// Get performs a query and attempts to fetch message for the given blockId and key
func (l *MessageLog) Get(proto net.ProtoAdapter, key uint64) net.LineProtocol {
	if raw, err := adp.GetMessage(key); raw != nil && err == nil {
		r := bytes.NewReader(raw)
		if msg, err := net.Read(proto, r); err == nil {
			return msg
		}

	}
	return nil
}

// Keys performs a query and attempts to fetch all keys for given blockId and key prefix.
func (l *MessageLog) Keys() []uint64 {
	return adp.Keys()
}

// Delete is used to delete message.
func (l *MessageLog) Delete(key uint64) {
	adp.DeleteMessage(key)
}

// Reset removes all keys from store
func (l *MessageLog) Reset() {
	keys := adp.Keys()
	for _, key := range keys {
		adp.DeleteMessage(key)
	}
}
