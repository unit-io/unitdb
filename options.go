package tracedb

import (
	"time"

	"github.com/frontnet/tracedb/fs"
)

// Options holds the optional DB parameters.
type Options struct {
	// BackgroundSyncInterval sets the amount of time between background fsync() calls.
	//
	// Setting the value to 0 disables the automatic background synchronization.
	// Setting the value to -1 makes the DB call fsync() after every write operation.
	BackgroundSyncInterval time.Duration

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
	return &opts
}

func (src *Options) memWithDefaults() *Options {
	opts := Options{}
	if src != nil {
		opts = *src
	}
	if opts.FileSystem == nil {
		opts.FileSystem = fs.Mem
	}

	if opts.BackgroundSyncInterval == 0 {
		opts.BackgroundSyncInterval = 15 * time.Second
	}

	return &opts
}
