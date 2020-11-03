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

package memdb

import (
	"time"
)

type _Options struct {
	logFilePath string

	// memdbSize sets Size of memory db.
	memdbSize int64

	// bufferSize sets Size of buffer to use for pooling.
	bufferSize int64

	// logSize sets Size of write ahead log.
	logSize int64

	// resetFlag skips recovery on DB open and reset WAL if reset flag is set true.
	resetFlag bool

	tinyBatchWriteInterval time.Duration
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

// WithDefaultOptions will open DB with some default values.
func WithDefaultOptions() Options {
	return newFuncOption(func(o *_Options) {
		if o.logFilePath == "" {
			o.logFilePath = "/tmp/unitdb"
		}
		if o.memdbSize == 0 {
			o.memdbSize = defaultMemSize
		}
		if o.bufferSize == 0 {
			o.bufferSize = defaultBufferSize
		}
		if o.logSize == 0 {
			o.logSize = defaultLogSize
		}
		if o.tinyBatchWriteInterval == 0 {
			o.tinyBatchWriteInterval = 15 * time.Millisecond
		}
	})
}

// WithLogFilePath sets database directory.
func WithLogFilePath(path string) Options {
	return newFuncOption(func(o *_Options) {
		o.logFilePath = path
	})
}

// WithMemdbSize sets Size of memory DB.
func WithMemdbSize(size int64) Options {
	return newFuncOption(func(o *_Options) {
		o.memdbSize = size
	})
}

// WithBufferSize sets Size of buffer to use for pooling.
func WithBufferSize(size int64) Options {
	return newFuncOption(func(o *_Options) {
		o.bufferSize = size
	})
}

// WithLogSize sets Size of write ahead log.
func WithLogSize(size int64) Options {
	return newFuncOption(func(o *_Options) {
		o.logSize = size
	})
}

// WithResetFlag skips recovery on DB open and reset WAL if reset flag is set.
func WithResetFlag() Options {
	return newFuncOption(func(o *_Options) {
		o.resetFlag = true
	})
}

// WithTinyBatchWriteInterval sets interval to group tiny batches and write into db on tiny batch interval.
func WithTinyBatchWriteInterval(dur time.Duration) Options {
	return newFuncOption(func(o *_Options) {
		o.tinyBatchWriteInterval = dur
	})
}
