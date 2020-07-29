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

package unitdb

import (
	"time"

	"github.com/unit-io/unitdb/fs"
	"github.com/unit-io/unitdb/message"
)

// flags holds various DB flags.
type flags struct {
	// Immutable set immutable flag on database
	Immutable bool

	// Encryption flag to encrypt keys
	Encryption bool

	// BackgroundKeyExpiry sets flag to run key expirer
	BackgroundKeyExpiry bool
}

// Options it contains configurable options and flags for DB
type Options interface {
	set(*options)
}

// fOption wraps a function that modifies options and flags into an
// implementation of the Options interface.
type fOption struct {
	f func(*options)
}

func (fo *fOption) set(o *options) {
	fo.f(o)
}

func newFuncOption(f func(*options)) *fOption {
	return &fOption{
		f: f,
	}
}

// WithDefaultFlags will open DB with some default values.
//   Immutable: True
//   Encryption: False
//   BackgroundKeyExpiry: False
func WithDefaultFlags() Options {
	return newFuncOption(func(o *options) {
		o.Immutable = true
		o.Encryption = false
		o.BackgroundKeyExpiry = false
	})
}

// WithMutable sets Immutable flag to false
func WithMutable() Options {
	return newFuncOption(func(o *options) {
		o.Immutable = false
	})
}

// WithEncryption sets encryption on DB
func WithEncryption() Options {
	return newFuncOption(func(o *options) {
		o.Encryption = true
	})
}

// WithBackgroundKeyExpiry sets background key expiry for DB
func WithBackgroundKeyExpiry() Options {
	return newFuncOption(func(o *options) {
		o.Immutable = false
	})
}

// options holds the optional DB parameters.
type options struct {
	flags
	// BackgroundSyncInterval sets the amount of time between background fsync() calls.
	//
	// Setting the value to 0 disables the automatic background synchronization.
	// Setting the value to -1 makes the DB call fsync() after every write operation.
	BackgroundSyncInterval time.Duration

	// EncryptionKey
	EncryptionKey []byte

	// TinyBatchWriteInterval interval to group tiny batches and write into db on tiny batch interval
	// Setting the value to 0 immediately writes entries into db.
	TinyBatchWriteInterval time.Duration

	// DefaultQueryLimit limits maximum number of records to fetch if the DB Get or DB Iterator method does not specify a limit.
	DefaultQueryLimit int

	// MaxQueryLimit limits maximum number of records to fetch if the DB Get or DB Iterator method does not specify a limit or specify a limit larger than MaxQueryResults.
	MaxQueryLimit int

	// BufferSize sets Size of buffer to use for pooling
	BufferSize int64

	// MemdbSize sets Size of memory db
	MemdbSize int64

	// LogSize sets Size of write ahead log
	LogSize int64

	// MinimumFreeBlocksSize minimum freeblocks size before free blocks are allocated and reused.
	MinimumFreeBlocksSize int64

	// FileSystem file storage type
	FileSystem fs.FileSystem
}

// BatchOptions is used to set options when using batch operation
type BatchOptions struct {
	// In concurrent batch writes order determines how to handle conflicts
	Order           int8
	Contract        uint32
	Immutable       bool
	Encryption      bool
	AllowDuplicates bool
}

// QueryOptions is used to set options for DB query
type QueryOptions struct {
	DefaultQueryLimit int
	MaxQueryLimit     int
}

// WithDefaultBatchOptions contains default options when writing batches to unitdb topic=>key-value store.
var WithDefaultBatchOptions = &BatchOptions{
	Order:           0,
	Contract:        message.MasterContract,
	Encryption:      false,
	AllowDuplicates: false,
}

func (o *BatchOptions) WithContract(contract uint32) *BatchOptions {
	o.Contract = contract
	return o
}

func (o *BatchOptions) WithEncryption() *BatchOptions {
	o.Encryption = true
	return o
}

func (o *BatchOptions) WithAllowDuplicates() *BatchOptions {
	o.AllowDuplicates = true
	return o
}

// WithDefaultOptions will open DB with some default values.
func WithDefaultOptions() Options {
	return newFuncOption(func(o *options) {
		if o.FileSystem == nil {
			o.FileSystem = fs.FileIO
		}
		if o.BackgroundSyncInterval == 0 {
			o.BackgroundSyncInterval = 1 * time.Second
		}
		if o.TinyBatchWriteInterval == 0 {
			o.TinyBatchWriteInterval = 15 * time.Millisecond
		}
		if o.DefaultQueryLimit == 0 {
			o.DefaultQueryLimit = 1000
		}
		if o.MaxQueryLimit == 0 {
			o.MaxQueryLimit = 100000
		}
		if o.BufferSize == 0 {
			o.BufferSize = 1 << 27 // maximum size of a buffer in bufferpool (128MB).
		}
		if o.MemdbSize == 0 {
			o.MemdbSize = 1 << 31 // maximum size of memdb (2GB).
		}
		if o.LogSize == 0 {
			o.LogSize = 1 << 30 // maximum size of log to grow before freelist allocation is started (1GB).
		}
		if o.MinimumFreeBlocksSize == 0 {
			o.MinimumFreeBlocksSize = 1 << 27 // minimum size of (128MB)
		}
		if o.EncryptionKey == nil {
			o.EncryptionKey = []byte("4BWm1vZletvrCDGWsF6mex8oBSd59m6I")
		}
	})
}

// WithBackgroundSyncInterval sets the amount of time between background fsync() calls.
func WithBackgroundSyncInterval(dur time.Duration) Options {
	return newFuncOption(func(o *options) {
		o.BackgroundSyncInterval = dur
	})
}

// WithTinyBatchWriteInterval sets interval to group tiny batches and write into db on tiny batch interval
func TinyBatchWriteInterval(dur time.Duration) Options {
	return newFuncOption(func(o *options) {
		o.TinyBatchWriteInterval = dur
	})
}

// WithDefaultQueryLimit limits maximum number of records to fetch
// if the DB Get or DB Iterator method does not specify a limit.
func WithDefaultQueryLimit(limit int) Options {
	return newFuncOption(func(o *options) {
		o.DefaultQueryLimit = limit
	})
}

// WithMaxQueryLimit limits maximum number of records to fetch
// if the DB Get or DB Iterator method does not specify
// a limit or specify a limit larger than MaxQueryResults.
func WithMaxQueryLimit(limit int) Options {
	return newFuncOption(func(o *options) {
		o.MaxQueryLimit = limit
	})
}

// WithBufferSize sets Size of buffer to use for pooling
func WithBufferSize(size int64) Options {
	return newFuncOption(func(o *options) {
		o.BufferSize = size
	})
}

// WithMemdbSize sets Size of memory db
func WithMemdbSize(size int64) Options {
	return newFuncOption(func(o *options) {
		o.MemdbSize = size
	})
}

// WithLogSize sets Size of write ahead log
func WithLogSize(size int64) Options {
	return newFuncOption(func(o *options) {
		o.LogSize = size
	})
}

// WithMinimumFreeBlocksSize sets minimum freeblocks size
// before free blocks are allocated and reused.
func WithMinimumFreeBlocksSize(size int64) Options {
	return newFuncOption(func(o *options) {
		o.MinimumFreeBlocksSize = size
	})
}

// WithEncryptionKey sets encryption key to use for data encryption
func WithEncryptionKey(key []byte) Options {
	return newFuncOption(func(o *options) {
		o.EncryptionKey = key
	})
}
