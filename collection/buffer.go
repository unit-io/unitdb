package collection

import (
	"fmt"
	"sync"

	"github.com/unit-io/tracedb/hash"
)

const (
	nShards = 32 // TODO implelemt sharding based on total Contracts in db

	// maxBufferSize value for maximum memory use for the buffer.
	maxBufferSize = (int64(1) << 34) - 1
)

// BlockBuffer A "thread" safe circular buffer.
// To avoid lock bottlenecks this block buffer is dived to several (SHARD_COUNT) shards.
type BlockBuffer struct {
	blocks     []*Buffer
	consistent *hash.Consistent
	// mu         sync.RWMutex
}

type Buffer struct {
	data         data
	sync.RWMutex // Read Write mutex, guards access to internal buffer.
}

// NewBlockBuffer creates new block buffer for concurrent read/write.
func NewBlockBuffer(name string, size int64) BlockBuffer {
	b := BlockBuffer{
		blocks:     make([]*Buffer, nShards),
		consistent: hash.InitConsistent(int(nShards), int(nShards)),
	}
	// b.mu.Lock()
	// defer b.mu.Unlock()
	for i := 0; i < nShards; i++ {
		name := fmt.Sprintf("%s-%d.data", name, i)
		buff, err := newTable(name, size)
		if err != nil {
			return b
		}
		b.blocks[i] = &Buffer{data: data{buffTable: buff}}
	}

	return b
}

// BufferPool represents the thread safe circular buffer pool.
// All BufferPool methods are safe for concurrent use by multiple goroutines.
type BuffPool struct {
	targetSize int64
	// block buffer
	BlockBuffer
}

// NewBuffPool creates a new buffer pool. Minimum memroy size is 1GB
func NewBuffPool(name string, size int64) *BuffPool {
	if size > maxBufferSize {
		size = maxBufferSize
	}
	return &BuffPool{
		targetSize:  size,
		BlockBuffer: NewBlockBuffer(name, size),
	}
}

// getShard returns shard under given contract
func (pool *BuffPool) getShard(key uint64) *Buffer {
	return pool.blocks[pool.consistent.FindBlock(key)]
}

func (pool *BuffPool) Write(key uint64, p []byte) (int, error) {
	// Get buff shard.
	shard := pool.getShard(key)
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
func (pool *BuffPool) Bytes(key uint64) []byte {
	if key == 0 {
		size := int64(0)
		buff := make([]byte, pool.Size())
		for i := 0; i < nShards; i++ {
			shard := pool.blocks[i]
			shard.RLock()
			data, _ := shard.data.bytes()
			copy(buff[size:], data)
			size += shard.data.size
			shard.RUnlock()
		}
		return buff
	}
	// Get shard
	shard := pool.getShard(key)
	shard.RLock()
	// Get item from shard.
	defer shard.RUnlock()
	data, _ := shard.data.bytes()
	return data
}

func (pool *BuffPool) Size() int64 {
	size := int64(0)
	for i := 0; i < nShards; i++ {
		shard := pool.blocks[i]
		shard.RLock()
		size += shard.data.size
		shard.RUnlock()
	}

	return size
}
