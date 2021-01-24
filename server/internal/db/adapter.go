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
	"errors"
)

var (
	errNotFound = errors.New("no messages were found")
)

// Adapter represents a message storage contract that message storage provides
// must fulfill.
type Adapter interface {
	// General

	// Open and configure the adapter
	Open(path, config string, reset bool) error
	// Close the adapter
	Close() error
	// IsOpen checks if the adapter is ready for use
	IsOpen() bool
	// // CheckDbVersion checks if the actual database version matches adapter version.
	// CheckDbVersion() error
	// GetName returns the name of the adapter
	GetName() string

	// Put is used to store a message, the SSID provided must be a full SSID
	// SSID, where first element should be a contract ID. The time resolution
	// for TTL will be in seconds. The function is executed synchronously and
	// it returns an error if some error was encountered during storage.
	Put(contract uint32, topic, payload []byte) error

	// PutWithID is used to store a message using a pre generated ID, the SSID provided must be a full SSID
	// SSID, where first element should be a contract ID. The time resolution
	// for TTL will be in seconds. The function is executed synchronously and
	// it returns an error if some error was encountered during storage.
	PutWithID(contract uint32, messageId, topic, payload []byte) error

	// Get performs a query and attempts to fetch last n messages where
	// n is specified by limit argument. From and until times can also be specified
	// for time-series retrieval.
	Get(contract uint32, topic []byte) ([][]byte, error)

	// NewID generate messageId that can later used to store and delete message from message store
	NewID() ([]byte, error)

	// Delete is used to delete entry, the SSID provided must be a full SSID
	// SSID, where first element should be a contract ID. The function is executed synchronously and
	// it returns an error if some error was encountered during delete.
	Delete(contract uint32, messageId, topic []byte) error

	// PutMessage is used to store a message.
	// it returns an error if some error was encountered during storage.
	PutMessage(key uint64, payload []byte) error

	// GetMessage performs a query and attempts to fetch message for the given key
	GetMessage(key uint64) ([]byte, error)

	// DeleteMessage is used to delete message.
	// it returns an error if some error was encountered during delete.
	DeleteMessage(key uint64) error

	// Keys performs a query and attempts to fetch all keys.
	Keys() []uint64
}
