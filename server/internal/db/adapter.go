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
	Put(contract uint32, topic string, payload []byte, ttl string) error

	// PutWithID is used to store a message using a pre generated ID, the SSID provided must be a full SSID
	// SSID, where first element should be a contract ID. The time resolution
	// for TTL will be in seconds. The function is executed synchronously and
	// it returns an error if some error was encountered during storage.
	PutWithID(contract uint32, messageId []byte, topic string, payload []byte, ttl string) error

	// Get performs a query and attempts to fetch last messages where
	// last is specified by last duration argument.
	Get(contract uint32, topic string, last string) ([][]byte, error)

	// GetWithIDs gets the messages stored on contract under topic, as Get
	// does without last, and the id of each, with which Delete deletes it.
	GetWithIDs(contract uint32, topic string) (ids, payloads [][]byte, err error)

	// NewID generate messageId that can later used to store and delete message from message store
	NewID() ([]byte, error)

	// Count returns the number of messages in the message store.
	Count() uint64

	// Delete is used to delete entry, the SSID provided must be a full SSID
	// SSID, where first element should be a contract ID. The function is executed synchronously and
	// it returns an error if some error was encountered during delete.
	Delete(contract uint32, messageId []byte, topic string) error

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

	// Flush waits for the messages put before it to be written to the
	// store's log, from which they are recovered after a crash.
	Flush() error

	// Checkpoint writes a copy of the store into dst, a directory that
	// doesn't exist or is empty. The copy opens as the store was at one
	// moment, as after a clean shutdown. Writes wait while it runs. Last, it
	// writes info into the copy as CheckpointInfoFile, with the copy's
	// stats and the engine's version, and returns what it wrote.
	Checkpoint(dst string, info CheckpointInfo) (CheckpointInfo, error)

	// Stats returns the size of the store.
	Stats() Stats
}

// Stats is the size of a store.
type Stats struct {
	// Messages is the number of messages in the store.
	Messages uint64 `json:"messages"`
	// DiskBytes is the size of the store's files.
	DiskBytes int64 `json:"disk_bytes"`
	// MemEntries is the number of records in memory (sessions, logs and the
	// like), and MemSize the configured size of that memory (mem_size); 0
	// if not configured.
	MemEntries int64 `json:"mem_entries"`
	MemSize    int64 `json:"mem_size"`
}
