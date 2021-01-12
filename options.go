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

	"github.com/unit-io/unitdb/message"
)

// _Flags holds various DB flags.
type _Flags struct {
	// immutable set immutable flag on database.
	immutable bool

	// encryption flag to encrypt keys.
	encryption bool

	// backgroundKeyExpiry sets flag to run key expirer.
	backgroundKeyExpiry bool
}

// _BatchOptions is used to set options when using batch operation.
type _BatchOptions struct {
	contract      uint32
	encryption    bool
	writeInterval time.Duration
}

// _QueryOptions is used to set options for DB query.
type _QueryOptions struct {
	// defaultQueryLimit limits maximum number of records to fetch if the DB Get or DB Iterator method does not specify a limit.
	defaultQueryLimit int

	// maxQueryLimit limits maximum number of records to fetch if the DB Get or DB Iterator method does not specify a limit or specify a limit larger than MaxQueryResults.
	maxQueryLimit int
}

// _Options holds the optional DB parameters.
type _Options struct {
	flags        _Flags
	batchOptions _BatchOptions
	queryOptions _QueryOptions
	// maxSyncDurations sets the amount of time between background fsync() calls.
	//
	// Setting the value to 0 disables the automatic background synchronization.
	// Setting the value to -1 makes the DB call fsync() after every write operation.
	maxSyncDurations int

	// syncDurationType set duration type to run sync for example syncDurationType is Second and maxSyncDuration is 5 then
	// all entries are sync to DB in 5 seconds.
	syncDurationType time.Duration

	// encryptionKey is used for message encryption.
	encryptionKey []byte

	// bufferSize sets Size of buffer to use for pooling.
	bufferSize int64

	// memdbSize sets Size of blockcache.
	memdbSize int64

	// freeBlockSize minimum freeblocks size before free blocks are allocated and reused.
	freeBlockSize int64
}

// Options it contains configurable options and flags for DB.
type Options interface {
	set(*_Options)
}

// fOption wraps a function that modifies options and flags into an
// implementation of the Options interface.
type fOption struct {
	f func(*_Options)
}

func (fo *fOption) set(o *_Options) {
	fo.f(o)
}

func newFuncOption(f func(*_Options)) *fOption {
	return &fOption{
		f: f,
	}
}

// WithDefaultFlags will open DB with some default values.
//   immutable: True
//   encryption: False
//   backgroundKeyExpiry: False
func WithDefaultFlags() Options {
	return newFuncOption(func(o *_Options) {
		o.flags.immutable = true
		o.flags.encryption = false
		o.flags.backgroundKeyExpiry = false
	})
}

// WithMutable sets Immutable flag to false.
func WithMutable() Options {
	return newFuncOption(func(o *_Options) {
		o.flags.immutable = false
	})
}

// WithEncryption sets encryption on DB.
func WithEncryption() Options {
	return newFuncOption(func(o *_Options) {
		o.flags.encryption = true
	})
}

// WithBackgroundKeyExpiry sets background key expiry for DB.
func WithBackgroundKeyExpiry() Options {
	return newFuncOption(func(o *_Options) {
		o.flags.backgroundKeyExpiry = true
	})
}

// WithDefaultBatchOptions will set some default values for Batch operation.
//   contract: MasterContract
//   encryption: False
func WithDefaultBatchOptions() Options {
	return newFuncOption(func(o *_Options) {
		o.batchOptions.contract = message.MasterContract
		o.batchOptions.encryption = false
	})
}

// WithBatchContract sets contract for batch operation.
func WithBatchContract(contract uint32) Options {
	return newFuncOption(func(o *_Options) {
		o.batchOptions.contract = contract
	})
}

// WithBatchEncryption sets encryption on batch operation.
func WithBatchEncryption() Options {
	return newFuncOption(func(o *_Options) {
		o.batchOptions.encryption = true
	})
}

// WithBatchWriteInterval sets batch write interval to partial write large batch.
func WithBatchWriteInterval(dur time.Duration) Options {
	return newFuncOption(func(o *_Options) {
		o.batchOptions.writeInterval = dur
	})
}

// WithDefaultOptions will open DB with some default values.
func WithDefaultOptions() Options {
	return newFuncOption(func(o *_Options) {
		if o.maxSyncDurations == 0 {
			o.maxSyncDurations = 1
		}
		if o.syncDurationType == 0 {
			o.syncDurationType = time.Second
		}
		if o.queryOptions.defaultQueryLimit == 0 {
			o.queryOptions.defaultQueryLimit = 1000
		}
		if o.queryOptions.maxQueryLimit == 0 {
			o.queryOptions.maxQueryLimit = 100000
		}
		if o.bufferSize == 0 {
			o.bufferSize = 1 << 32 // maximum size of a buffer to use in bufferpool (4GB).
		}
		if o.memdbSize == 0 {
			o.memdbSize = 1 << 33 // maximum size of blockcache (8GB).
		}
		if o.freeBlockSize == 0 {
			o.freeBlockSize = 1 << 27 // minimum size of (128MB).
		}
		if o.encryptionKey == nil {
			o.encryptionKey = []byte("4BWm1vZletvrCDGWsF6mex8oBSd59m6I")
		}
	})
}

// WithMaxSyncDuration sets the amount of time between background fsync() calls.
func WithMaxSyncDuration(dur time.Duration, interval int) Options {
	return newFuncOption(func(o *_Options) {
		o.maxSyncDurations = interval
		o.syncDurationType = dur
	})
}

// WithDefaultQueryLimit limits maximum number of records to fetch
// if the DB Get or DB Iterator method does not specify a limit.
func WithDefaultQueryLimit(limit int) Options {
	return newFuncOption(func(o *_Options) {
		o.queryOptions.defaultQueryLimit = limit
	})
}

// WithMaxQueryLimit limits maximum number of records to fetch
// if the DB Get or DB Iterator method does not specify
// a limit or specify a limit larger than MaxQueryResults.
func WithMaxQueryLimit(limit int) Options {
	return newFuncOption(func(o *_Options) {
		o.queryOptions.maxQueryLimit = limit
	})
}

// WithBufferSize sets Size of buffer to use for pooling.
func WithBufferSize(size int64) Options {
	return newFuncOption(func(o *_Options) {
		o.bufferSize = size
	})
}

// WithMemdbSize sets Size of blockcache.
func WithMemdbSize(size int64) Options {
	return newFuncOption(func(o *_Options) {
		o.memdbSize = size
	})
}

// WithFreeBlockSize sets minimum freeblocks size
// before free blocks are allocated and reused.
func WithFreeBlockSize(size int64) Options {
	return newFuncOption(func(o *_Options) {
		o.freeBlockSize = size
	})
}

// WithEncryptionKey sets encryption key to use for data encryption.
func WithEncryptionKey(key []byte) Options {
	return newFuncOption(func(o *_Options) {
		o.encryptionKey = key
	})
}
