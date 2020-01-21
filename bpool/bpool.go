package bpool

import (
	"sync"
	"time"
)

const (
	nShards = 2048
	// maxBufferSize value for maximum memory use for the buffer.
	maxBufferSize = (int64(1) << 34) - 1
)

type Buffer struct {
	internal     buffer
	sync.RWMutex // Read Write mutex, guards access to internal buffer.
}

// Get returns buffer if any in pool or create a new buffer
func (pool *BufferPool) Get() *Buffer {
	var buf *Buffer
	select {
	case buf = <-pool.buf:
	default:
	}
	if buf == nil {
		buf = &Buffer{internal: buffer{maxSize: pool.targetSize}}
	}
	return buf
}

// Put reset the buffer and put it to the pool
func (pool *BufferPool) Put(buf *Buffer) {
	buf.internal.reset()
	select {
	case pool.buf <- buf:
	default:
	}
}

// Write writes to buffer
func (buf *Buffer) Write(p []byte) (int, error) {
	buf.Lock()
	defer buf.Unlock()
	off, err := buf.internal.allocate(uint32(len(p)))
	if err != nil {
		return 0, err
	}
	if _, err := buf.internal.writeAt(p, off); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Bytes gets data for from the internal buffer
func (buf *Buffer) Bytes() []byte {
	buf.RLock()
	defer buf.RUnlock()
	data, _ := buf.internal.bytes()
	return data
}

// Reset reset the buffer
func (buf *Buffer) Reset() {
	buf.Lock()
	defer buf.Unlock()
	buf.internal.reset()
}

// Truncate truncates buffer from front and free it
func (buf *Buffer) Truncate(off int64) {
	buf.Lock()
	defer buf.Unlock()
	buf.internal.truncateFront(off)
}

// Size internal buffer size
func (buf *Buffer) Size() int64 {
	buf.RLock()
	defer buf.RUnlock()
	return buf.internal.size
}

// BufferPool represents the thread safe buffer pool.
// All BufferPool methods are safe for concurrent use by multiple goroutines.
type BufferPool struct {
	targetSize int64
	buf        chan *Buffer
}

// NewBufferPool creates a new buffer pool. Minimum memroy size is 1GB
func NewBufferPool(size int64) *BufferPool {
	if size > maxBufferSize {
		size = maxBufferSize
	}

	pool := &BufferPool{
		targetSize: size,
		buf:        make(chan *Buffer, nShards),
	}

	go pool.drain()

	return pool
}

func (pool *BufferPool) drain() {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			select {
			case <-pool.buf:
			default:
			}
		}
	}
}
