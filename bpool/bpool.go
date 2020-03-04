package bpool

import (
	"sync"
	"time"
)

const (
	maxPoolSize = 2048
	// maxBufferSize value to limit maximum memory for the buffer.
	maxBufferSize = (int64(1) << 34) - 1
)

type Buffer struct {
	internal     buffer
	sync.RWMutex // Read Write mutex, guards access to internal buffer.
}

// Get returns buffer if any in the pool or creates a new buffer
func (pool *BufferPool) Get() (buf *Buffer) {
	select {
	case buf = <-pool.buf:
	default:
		buf = &Buffer{}
	}
	return
}

// Put resets the buffer and put it to the pool
func (pool *BufferPool) Put(buf *Buffer) {
	buf.internal.reset()
	select {
	case pool.buf <- buf:
	default:
	}
}

// Write writes to the buffer
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

// Bytes gets data from internal buffer
func (buf *Buffer) Bytes() []byte {
	buf.RLock()
	defer buf.RUnlock()
	data, _ := buf.internal.bytes()
	return data
}

// Reset resets the buffer
func (buf *Buffer) Reset() {
	buf.Lock()
	defer buf.Unlock()
	buf.internal.reset()
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

// NewBufferPool creates a new buffer pool.
func NewBufferPool(size int64) *BufferPool {
	if size > maxBufferSize {
		size = maxBufferSize
	}

	pool := &BufferPool{
		targetSize: size,
		buf:        make(chan *Buffer, maxPoolSize),
	}

	go pool.drain()

	return pool
}

func (pool *BufferPool) drain() {
	ticker := time.NewTicker(10 * time.Second)
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
