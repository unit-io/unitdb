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

// Flags it contains configurable flags for DB
type Flags interface {
	set(*flags)
}

// fFlag wraps a function that modifies flags into an
// implementation of the Flags interface.
type fFlag struct {
	f func(*flags)
}

func (fo *fFlag) set(f *flags) {
	fo.f(f)
}

func newFuncFlag(f func(*flags)) *fFlag {
	return &fFlag{
		f: f,
	}
}

// WithDefaultFlags will open DBwith some default values.
//   Immutable: True
//   Encryption: False
//   BackgroundKeyExpiry: False
func WithDefaultFlags() Flags {
	return newFuncFlag(func(f *flags) {
		f.Immutable = true
		f.Encryption = false
		f.BackgroundKeyExpiry = false
	})
}

// WithMutable sets Immutable flag to false
func WithMutable() Flags {
	return newFuncFlag(func(f *flags) {
		f.Immutable = false
	})
}

// WithEncryption sets encryption on DB
func WithEncryption() Flags {
	return newFuncFlag(func(f *flags) {
		f.Encryption = true
	})
}

// WithBackgroundKeyExpiry sets background key expiry for DB
func WithBackgroundKeyExpiry() Flags {
	return newFuncFlag(func(f *flags) {
		f.Immutable = false
	})
}

// Options holds the optional DB parameters.
type Options struct {
	// BackgroundSyncInterval sets the amount of time between background fsync() calls.
	//
	// Setting the value to 0 disables the automatic background synchronization.
	// Setting the value to -1 makes the DB call fsync() after every write operation.
	BackgroundSyncInterval time.Duration

	// Encryption Key
	EncryptionKey []byte

	// Tiny Batch interval to group tiny batches and write into db on tiny batch interval
	// Setting the value to 0 immediately writes entries into db.
	TinyBatchWriteInterval time.Duration

	// DefaultQueryLimit limits maximum number of records to fetch if the DB Get or DB Iterator method does not specify a limit.
	DefaultQueryLimit int

	// MaxQueryLimit limits maximum number of records to fetch if the DB Get or DB Iterator method does not specify a limit or specify a limit larger than MaxQueryResults.
	MaxQueryLimit int

	// Size of buffer to use for pooling
	BufferSize int64

	// Size of memory db
	MemdbSize int64

	// Size of write ahead log
	LogSize int64

	// Minimum freeblocks size before free blocks are allocated and reused.
	MinimumFreeBlocksSize int64

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

func (src *Options) copyWithDefaults() *Options {
	opts := Options{}
	if src != nil {
		opts = *src
	}
	if opts.FileSystem == nil {
		opts.FileSystem = fs.FileIO
	}
	if opts.BackgroundSyncInterval == 0 {
		opts.BackgroundSyncInterval = 1 * time.Second
	}
	if opts.TinyBatchWriteInterval == 0 {
		opts.TinyBatchWriteInterval = 15 * time.Millisecond
	}
	if opts.DefaultQueryLimit == 0 {
		opts.DefaultQueryLimit = 1000
	}
	if opts.MaxQueryLimit == 0 {
		opts.MaxQueryLimit = 100000
	}
	if opts.BufferSize == 0 {
		opts.BufferSize = 1 << 27 // maximum size of a buffer in bufferpool (128MB).
	}
	if opts.MemdbSize == 0 {
		opts.MemdbSize = 1 << 31 // maximum size of memdb (2GB).
	}
	if opts.LogSize == 0 {
		opts.LogSize = 1 << 30 // maximum size of log to grow before freelist allocation is started (1GB).
	}
	if opts.MinimumFreeBlocksSize == 0 {
		opts.MinimumFreeBlocksSize = 1 << 27 // minimum size of (128MB)
	}
	if opts.EncryptionKey == nil {
		opts.EncryptionKey = []byte("4BWm1vZletvrCDGWsF6mex8oBSd59m6I")
	}
	return &opts
}

// DefaultBatchOptions contains default options when writing batches to unitdb topicc=>key-value store.
var DefaultBatchOptions = &BatchOptions{
	Order:           0,
	Contract:        message.MasterContract,
	Encryption:      false,
	AllowDuplicates: false,
}
