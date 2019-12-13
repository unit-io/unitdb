package main

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/saffat-in/tracedb"
)

type kvEngine interface {
	Batch(func(b *tracedb.Batch) error) error
	Get(key []byte) ([]byte, error)
	Close() error
	FileSize() (int64, error)
}

type engineCtr func(string) (kvEngine, error)

var engines = map[string]engineCtr{
	"tracedb": newTracedb,
}

func getEngineCtr(name string) (engineCtr, error) {
	if ctr, ok := engines[name]; ok {
		return ctr, nil
	}
	return nil, errors.New("unknown engine")
}

func dirSize(path string) (int64, error) {
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			size += info.Size()
		}
		return err
	})
	return size, err
}

func newTracedb(path string) (kvEngine, error) {
	opts := tracedb.Options{}
	// opts.BackgroundSyncInterval = 15 * time.Second
	return tracedb.Open(path, &opts)
}
