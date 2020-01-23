package tracedb

import (
	"time"

	"github.com/unit-io/tracedb/fs"
)

// Options holds the optional DB parameters.
type Options struct {
	// BackgroundSyncInterval sets the amount of time between background fsync() calls.
	//
	// Setting the value to 0 disables the automatic background synchronization.
	// Setting the value to -1 makes the DB call fsync() after every write operation.
	BackgroundSyncInterval time.Duration

	// BackgroundKeyExpiry sets flag to run key expirer
	BackgroundKeyExpiry bool

	// Encryption flag to encrypt keys
	Encryption bool

	// Encryption Key
	EncryptionKey []byte

	// Block cache size
	BlockCacheSize int64

	//Tiny Batch Size to group tiny batches and write into db on tiny batch interval
	TinyBatchSize int

	//Tiny Batch interval to group tiny batches and write into db on tiny batch interval
	// Setting the value to 0 immediately writes entries into db.
	TinyBatchWriteInterval time.Duration

	// Size of buffer to use for pooling
	BufferSize int64

	// Size of memory db
	MemdbSize int64

	// Size of write ahead log
	LogSize int64

	// Minimum freeblocks size before free blocks are allocated and reused.
	MinimumFreeBlocksSize int64

	FileSystem fs.FileSystem
}

func (src *Options) copyWithDefaults() *Options {
	opts := Options{}
	if src != nil {
		opts = *src
	}
	opts.BackgroundKeyExpiry = true
	if opts.FileSystem == nil {
		opts.FileSystem = fs.FileIO
	}
	if opts.BackgroundSyncInterval == 0 {
		opts.BackgroundSyncInterval = 1 * time.Second
	}
	if opts.BlockCacheSize == 0 {
		opts.BlockCacheSize = 1 << 20 // maximum cost of cache (1MB).
	}
	if opts.TinyBatchSize == 0 {
		opts.TinyBatchSize = 100
	}
	if opts.TinyBatchWriteInterval == 0 {
		opts.TinyBatchWriteInterval = 15 * time.Millisecond
	}
	if opts.BufferSize == 0 {
		opts.BufferSize = 1 << 33 // maximum size of memdb (8GB).
	}
	if opts.MemdbSize == 0 {
		opts.MemdbSize = 1<<33 + 1<<32 // maximum size of memdb (12GB).
	}
	if opts.LogSize == 0 {
		opts.LogSize = 1 << 33 // maximum size of memdb (8GB).
	}
	if opts.MinimumFreeBlocksSize == 0 {
		opts.MinimumFreeBlocksSize = 1 << 24 // minimum size of (16MB)
	}
	if opts.EncryptionKey == nil {
		opts.EncryptionKey = []byte("4BWm1vZletvrCDGWsF6mex8oBSd59m6I")
	}
	return &opts
}
