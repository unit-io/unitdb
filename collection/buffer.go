package collection

import (
	"sync"

	"github.com/unit-io/tracedb/hash"
)

const (
	nShards = 32 // TODO implelemt sharding based on total Contracts in db

	// maxBufferSize value for maximum memory use for the buffer.
	maxBufferSize = (int64(1) << 33) - 1
)

// BlockBuffer A "thread" safe circular buffer.
// To avoid lock bottlenecks this block buffer is dived to several (SHARD_COUNT) shards.
type BlockBuffer []*Buffer

type Buffer struct {
	data         data
	freeOffset   int64 // cache keep lowest offset that can be free.
	sync.RWMutex       // Read Write mutex, guards access to internal map.
}

// new creates a new buffer.
func NewBlockBuffer(name string, size int64) BlockBuffer {
	b := make(BlockBuffer, nShards)
	for i := 0; i < nShards; i++ {
		buff, err := buff.newTable(name, size)
		if err != nil {
			return nil
		}
		b[i] = &Buffer{data: data{bufferManager: buff}}
	}
	return b
}

// BufferPool represents the thread safe circular buffer pool.
// All BufferPool methods are safe for concurrent use by multiple goroutines.
type BuffPool struct {
	targetSize int64
	// block cache
	consistent  *hash.Consistent
	blockBuffer BlockBuffer
}

// NewBuffPool creates a new buffer pool. Minimum memroy size is 1GB
func NewBuffPool(name string, size int64) *BuffPool {
	if size > maxBufferSize {
		size = maxBufferSize
	}
	return &BuffPool{
		targetSize:  size,
		consistent:  hash.InitConsistent(int(nShards), int(nShards)),
		blockBuffer: NewBlockBuffer(name, size),
	}
}

// // BuffPool represents a thread safe buffer pool
// type BuffPool struct {
// 	internal sync.Pool
// }

// // NewBuffPool creates a new BufferPool bounded to the given size.
// func NewBuffPool() (bp *BufferPool) {
// 	return &BufferPool{
// 		sync.Pool{
// 			New: func() interface{} {
// 				return new(bytes.Buffer)
// 			},
// 		},
// 	}
// }

// // Get gets a Buffer from the SizedBufferPool, or creates a new one if none are
// // available in the pool. Buffers have a pre-allocated capacity.
// func (pool *BuffPool) Get() (buffer *bytes.Buffer) {
// 	return pool.internal.Get().(*bytes.Buffer)
// }

// // Put returns the given Buffer to the SizedBufferPool.
// func (pool *BuffPool) Put(buffer *bytes.Buffer) {
// 	// See https://golang.org/issue/23199
// 	const maxSize = 1 << 16 // 64KiB
// 	if buffer.Len() > maxSize {
// 		return
// 	}
// 	buffer.Reset()
// 	pool.internal.Put(buffer)
// }
