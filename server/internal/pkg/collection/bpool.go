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
