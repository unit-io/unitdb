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

	adapter "github.com/unit-io/unitdb/server/internal/db"
	"github.com/unit-io/unitdb/server/internal/message"
	lp "github.com/unit-io/unitdb/server/internal/net"
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

// SessionStore is a Session struct to hold methods for persistence mapping for the Session object.
type SessionStore struct{}

// Session is the anchor for storing/retrieving Session objects
var Session SessionStore

func (s *SessionStore) Put(key uint64, payload []byte) error {
	return adp.PutMessage(key, payload)
}

func (s *SessionStore) Get(key uint64) (raw []byte, err error) {
	return adp.GetMessage(key)
}

// MessageLog is a Message struct to hold methods for persistence mapping for the Message object.
type MessageLog struct{}

// Log is the anchor for storing/retrieving Message objects
var Log MessageLog

// PersistOutbound handles which outgoing messages are stored
func (l *MessageLog) PersistOutbound(proto lp.ProtoAdapter, blockID uint32, outMsg lp.LineProtocol) {
	switch outMsg.(type) {
	case *lp.Publish:
		// Received a publish. store it in ibound
		// until ACKNOWLEDGE or RECEIPT is received.
		key := uint64(outMsg.Info().MessageID)<<32 + uint64(blockID)
		m, err := lp.Encode(proto, outMsg)
		if err != nil {
			log.ErrLogger.Err(err).Str("context", "store.PersistInbound")
			return
		}
		adp.PutMessage(key, m.Bytes())
	}
	if outMsg.Type()==lp.FLOWCONTROL {
		msg := *outMsg.(*lp.ControlMessage)
		switch msg.FlowControl {
		case lp.COMPLETE:
			// Sending ACKNOWLEDGE, delete matching PUBLISH for EXPRESS delivery mode
			// or sending COMPLETE, delete matching RECEIVE for RELIABLE delivery mode from ibound
			key := uint64(outMsg.Info().MessageID)<<32 + uint64(blockID)
			adp.DeleteMessage(key)
		}
	}
}

// PersistInbound handles which incoming messages are stored
func (l *MessageLog) PersistInbound(proto lp.ProtoAdapter, blockID uint32, inMsg lp.LineProtocol) {
	if inMsg.Type()==lp.FLOWCONTROL {
		msg := *inMsg.(*lp.ControlMessage)
		switch msg.FlowControl {
		case lp.RECEIPT:
			// Sending RECEIPT. store in ibound
			// until COMPLETE is sent.
			key := uint64(inMsg.Info().MessageID)<<32 + uint64(blockID)
			m, err := lp.Encode(proto, inMsg)
			if err != nil {
				log.ErrLogger.Err(err).Str("context", "store.PersistOutbound")
				return
			}
			adp.PutMessage(key, m.Bytes())
	}
}
	}

// Get performs a query and attempts to fetch message for the given blockId and key
func (l *MessageLog) Get(proto lp.ProtoAdapter, key uint64) lp.LineProtocol {
	if raw, err := adp.GetMessage(key); raw != nil && err == nil {
		r := bytes.NewReader(raw)
		if msg, err := lp.Read(proto, r); err == nil {
			return msg
		}

	}
	return nil
}

// Keys performs a query and attempts to fetch all keys with the prefix.
func (l *MessageLog) Keys(prefix uint32) []uint64 {
	matches := make([]uint64, 0)
	keys := adp.Keys()
	for _, key := range keys {
		if evalPrefix(prefix, key) {
			matches = append(matches, key)
		}
	}
	return matches
}

// Delete is used to delete message.
func (l *MessageLog) Delete(key uint64) {
	adp.DeleteMessage(key)
}

// Reset removes all keys with the prefix from store
func (l *MessageLog) Reset(prefix uint32) {
	keys := adp.Keys()
	for _, key := range keys {
		if evalPrefix(prefix, key) {
			adp.DeleteMessage(key)
		}
	}
}

func evalPrefix(prefix uint32, key uint64) bool {
	return uint64(prefix) == key&0xFFFFFFFF
}
