package bpool

import (
	"sync"

	"github.com/unit-io/tracedb/hash"
)

const (
	nShards = 500

	// maxBufferSize value for maximum memory use for the buffer.
	maxBufferSize = (int64(1) << 34) - 1
)

type Buffer struct {
	data         data
	sync.RWMutex // Read Write mutex, guards access to internal buffer.
}

// Get returns shard under given key
func (pool *BufferPool) Get(key uint64) *Buffer {
	return pool.buff[pool.consistent.FindBlock(key)]
}

// Put returns shard under given key
func (pool *BufferPool) Put(key uint64) {
	buff := pool.buff[pool.consistent.FindBlock(key)]
	buff.Lock()
	defer buff.Unlock()
	buff.data.reset()
}

func (buff *Buffer) Write(p []byte) (int, error) {
	buff.Lock()
	defer buff.Unlock()
	off, err := buff.data.allocate(uint32(len(p)))
	if err != nil {
		return 0, err
	}
	if _, err := buff.data.writeAt(p, off); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Get gets data for the provided key
func (buff *Buffer) Bytes() []byte {
	buff.RLock()
	defer buff.RUnlock()
	data, _ := buff.data.bytes()
	return data
}

func (buff *Buffer) Size() int64 {
	buff.RLock()
	defer buff.RUnlock()
	return buff.data.size
}

// BufferPool represents the thread safe circular buffer pool.
// All BufferPool methods are safe for concurrent use by multiple goroutines.
type BufferPool struct {
	buff       []*Buffer
	consistent *hash.Consistent
}

// NewBufferPool creates a new buffer pool. Minimum memroy size is 1GB
func NewBufferPool(size int64) *BufferPool {
	if size > maxBufferSize {
		size = maxBufferSize
	}

	b := &BufferPool{
		buff:       make([]*Buffer, nShards),
		consistent: hash.InitConsistent(int(nShards), int(nShards)),
	}
	for i := 0; i < nShards; i++ {
		buff, err := newTable(size / nShards)
		if err != nil {
			return b
		}
		b.buff[i] = &Buffer{data: data{buffTable: buff}}
	}

	return b
}

func (pool *BufferPool) Write(key uint64, p []byte) (int, error) {
	// Get buff shard.
	shard := pool.Get(key)
	shard.Lock()
	defer shard.Unlock()
	off, err := shard.data.allocate(uint32(len(p)))
	if err != nil {
		return 0, err
	}
	if _, err := shard.data.writeAt(p, off); err != nil {
		return 0, err
	}
	return len(p), nil
}

// Get gets data for the provided key
func (pool *BufferPool) Bytes(key uint64) []byte {
	// Get shard
	shard := pool.Get(key)
	shard.RLock()
	// Get item from shard.
	defer shard.RUnlock()
	data, _ := shard.data.bytes()
	return data
}

func (pool *BufferPool) Size() int64 {
	size := int64(0)
	for i := 0; i < nShards; i++ {
		shard := pool.buff[i]
		shard.RLock()
		size += shard.data.size
		shard.RUnlock()
	}

	return size
}
