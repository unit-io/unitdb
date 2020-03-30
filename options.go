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

	// Cache capacity is number of seq to cache per topic in a seq set of a topic
	CacheCap int64

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
	opts.BackgroundKeyExpiry = false
	if opts.FileSystem == nil {
		opts.FileSystem = fs.FileIO
	}
	if opts.BackgroundSyncInterval == 0 {
		opts.BackgroundSyncInterval = 1 * time.Second
	}
	if opts.CacheCap == 0 {
		opts.CacheCap = 500
	}
	if opts.TinyBatchSize == 0 {
		opts.TinyBatchSize = 500
	}
	if opts.TinyBatchWriteInterval == 0 {
		opts.TinyBatchWriteInterval = 15 * time.Millisecond
	}
	if opts.BufferSize == 0 {
		opts.BufferSize = 1 << 27 // maximum size of a buffer in bufferpool (128MB).
	}
	if opts.MemdbSize == 0 {
		opts.MemdbSize = 1 << 31 // maximum size of memdb (2GB).
	}
	if opts.LogSize == 0 {
		opts.LogSize = 1 << 30 // maximum size of log to grow before freelist allocation is started (1GB).
	}
	if opts.MinimumFreeBlocksSize == 0 {
		opts.MinimumFreeBlocksSize = 1 << 27 // minimum size of (128MB)
	}
	if opts.EncryptionKey == nil {
		opts.EncryptionKey = []byte("4BWm1vZletvrCDGWsF6mex8oBSd59m6I")
	}
	return &opts
}
