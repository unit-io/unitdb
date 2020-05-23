package unitdb

import (
	"time"

	"github.com/unit-io/unitdb/fs"
)

// Flags holds various DB flags.
type Flags struct {
	// Immutable set immutable flag on database
	Immutable int8

	// Encryption flag to encrypt keys
	Encryption int8

	// BackgroundKeyExpiry sets flag to run key expirer
	BackgroundKeyExpiry int8
}

func (src *Flags) copyWithDefaults() *Flags {
	flgs := Flags{}
	if src != nil {
		flgs = *src
	}

	if flgs.Immutable == 0 {
		flgs.Immutable = 1
	}
	if flgs.Encryption == 0 {
		flgs.Encryption = -1
	}
	if flgs.BackgroundKeyExpiry == 0 {
		flgs.BackgroundKeyExpiry = -1
	}

	return &flgs
}

// Options holds the optional DB parameters.
type Options struct {
	// BackgroundSyncInterval sets the amount of time between background fsync() calls.
	//
	// Setting the value to 0 disables the automatic background synchronization.
	// Setting the value to -1 makes the DB call fsync() after every write operation.
	BackgroundSyncInterval time.Duration

	// Encryption Key
	EncryptionKey []byte

	// Tiny Batch interval to group tiny batches and write into db on tiny batch interval
	// Setting the value to 0 immediately writes entries into db.
	TinyBatchWriteInterval time.Duration

	// DefaultQueryLimit limits maximum number of records to fetch if the DB Get or DB Iterator method does not specify a limit.
	DefaultQueryLimit int

	// MaxQueryLimit limits maximum number of records to fetch if the DB Get or DB Iterator method does not specify a limit or specify a limit larger than MaxQueryResults.
	MaxQueryLimit int

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
	if opts.FileSystem == nil {
		opts.FileSystem = fs.FileIO
	}
	if opts.BackgroundSyncInterval == 0 {
		opts.BackgroundSyncInterval = 1 * time.Second
	}
	if opts.TinyBatchWriteInterval == 0 {
		opts.TinyBatchWriteInterval = 15 * time.Millisecond
	}
	if opts.DefaultQueryLimit == 0 {
		opts.DefaultQueryLimit = 1000
	}
	if opts.MaxQueryLimit == 0 {
		opts.MaxQueryLimit = 100000
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
