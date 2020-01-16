package bpool

import (
	"sync"
	"sync/atomic"

	"github.com/unit-io/tracedb/hash"
)

const (
	nShards = 1000

	// maxBufferSize value for maximum memory use for the buffer.
	maxBufferSize = (int64(1) << 34) - 1
)

type Buffer struct {
	internal     buffer
	sync.RWMutex // Read Write mutex, guards access to internal buffer.
}

// Get returns shard under given key
func (pool *BufferPool) Get() *Buffer {
	key := pool.Next()
	return pool.buf[pool.consistent.FindBlock(key)]
}

// Put reset data under shard for given key
func (pool *BufferPool) Put(buf *Buffer) {
	buf.internal.reset()
}

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

// Get gets data for the provided key
func (buf *Buffer) Bytes() []byte {
	buf.RLock()
	defer buf.RUnlock()
	data, _ := buf.internal.bytes()
	return data
}

func (buf *Buffer) Size() int64 {
	buf.RLock()
	defer buf.RUnlock()
	return buf.internal.size
}

// BufferPool represents the thread safe circular buffer pool.
// All BufferPool methods are safe for concurrent use by multiple goroutines.
type BufferPool struct {
	next       uint64
	buf        []*Buffer
	consistent *hash.Consistent
}

// NewBufferPool creates a new buffer pool. Minimum memroy size is 1GB
func NewBufferPool(size int64) *BufferPool {
	if size > maxBufferSize {
		size = maxBufferSize
	}

	b := &BufferPool{
		buf:        make([]*Buffer, nShards),
		consistent: hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
		buff, err := newTable(size)
		if err != nil {
			return b
		}
		b.buf[i] = &Buffer{internal: buffer{bufTable: buff}}
	}

	return b
}

func (pool *BufferPool) Size() int64 {
	size := int64(0)
	for i := 0; i < nShards; i++ {
		shard := pool.buf[i]
		shard.RLock()
		size += shard.internal.size
		shard.RUnlock()
	}

	return size
}

func (pool *BufferPool) Next() uint64 {
	return atomic.AddUint64(&pool.next, 1)
}
