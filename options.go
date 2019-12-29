package tracedb

import (
	"time"

	"github.com/unit-io/tracedb/fs"
)

// Options holds the optional DB parameters.
type Options struct {
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

	// Size of memory db
	MemdbSize int64

	// Size of write ahead log
	LogSize int64

	// LogSyncInterval sync write ahead log in background
	LogSyncInterval time.Duration

	FileSystem fs.FileSystem
}

func (src *Options) copyWithDefaults() *Options {
	opts := Options{}
	if src != nil {
		opts = *src
	}
	// opts.BackgroundKeyExpiry = true
	if opts.FileSystem == nil {
		opts.FileSystem = fs.FileIO
	}
	if opts.BlockCacheSize == 0 {
		opts.BlockCacheSize = 1 << 30 // maximum cost of cache (1GB).
	}
	if opts.TinyBatchSize == 0 {
		opts.TinyBatchSize = 100
	}
	if opts.TinyBatchWriteInterval == 0 {
		opts.TinyBatchWriteInterval = 15 * time.Millisecond
	}
	if opts.MemdbSize == 0 {
		opts.MemdbSize = 1 << 33 // maximum size of memdb (1GB).
	}
	if opts.LogSize == 0 {
		opts.LogSize = 1 << 20 // maximum size of memdb (1MB).
	}
	if opts.LogSyncInterval == 0 {
		opts.LogSyncInterval = 15 * time.Second
	}
	if opts.EncryptionKey == nil {
		opts.EncryptionKey = []byte("4BWm1vZletvrCDGWsF6mex8oBSd59m6I")
	}
	return &opts
}
