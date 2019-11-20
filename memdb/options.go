package memdb

import (
	"github.com/saffat-in/tracedb/fs"
)

// Options holds the optional DB parameters.
type Options struct {
	FileSystem fs.FileSystem
}

func (src *Options) CopyWithDefaults() *Options {
	opts := Options{}
	if src != nil {
		opts = *src
	}
	if opts.FileSystem == nil {
		opts.FileSystem = fs.Mem
	}
	return &opts
}
