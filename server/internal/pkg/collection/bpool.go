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

package collection

import (
	"bytes"
	"sync"
)

// BufferPool represents a thread safe buffer pool
type BufferPool struct {
	internal sync.Pool
}

// NewBufferPool creates a new BufferPool bounded to the given size.
func NewBufferPool() (bp *BufferPool) {
	return &BufferPool{
		sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
}

// Get gets a Buffer from the SizedBufferPool, or creates a new one if none are
// available in the pool. Buffers have a pre-allocated capacity.
func (pool *BufferPool) Get() (buffer *bytes.Buffer) {
	return pool.internal.Get().(*bytes.Buffer)
}

// Put returns the given Buffer to the SizedBufferPool.
func (pool *BufferPool) Put(buffer *bytes.Buffer) {
	// See https://golang.org/issue/23199
	const maxSize = 1 << 16 // 64KiB
	if buffer.Len() > maxSize {
		return
	}
	buffer.Reset()
	pool.internal.Put(buffer)
}
