package tracedb

import (
	"time"

	"github.com/saffat-in/tracedb/fs"
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

	// Size of memory db
	MemdbSize int64

	// Size of write ahead log
	LogSize int64

	FileSystem fs.FileSystem
}

func (src *Options) copyWithDefaults() *Options {
	opts := Options{}
	if src != nil {
		opts = *src
	}
	if opts.FileSystem == nil {
		opts.FileSystem = fs.FileIO
	}
	if opts.BackgroundSyncInterval == 0 {
		opts.BackgroundSyncInterval = 15 * time.Second
	}
	if opts.BlockCacheSize == 0 {
		opts.BlockCacheSize = 1 << 30 // maximum cost of cache (1GB).
	}
	if opts.MemdbSize == 0 {
		opts.MemdbSize = 1 << 33 // maximum size of memdb (1GB).
	}
	if opts.LogSize == 0 {
		opts.LogSize = 1 << 20 // maximum size of memdb (1MB).
	}
	if opts.EncryptionKey == nil {
		opts.EncryptionKey = []byte("4BWm1vZletvrCDGWsF6mex8oBSd59m6I")
	}
	return &opts
}
