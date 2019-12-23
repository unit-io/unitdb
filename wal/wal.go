package wal

import (
	"errors"
	"sort"
	"sync"
)

const (
	logHeaderSize = 26
)

const (
	// logStatusInvalid indicates an incorrectly initialized block.
	logStatusInvalid = iota

	// logStatusCommitted indicates that the transaction has been committed,
	// but not completed. During recovery, transactions with this status
	// should be loaded and their updates should be provided to the user.
	logStatusWritten = iota

	// logStatusApplied indicates that the transaction has been committed and
	// applied. Transactions with this status can be ignored during recovery,
	// and their associated blocks can be reclaimed.
	logStatusApplied
)

type (
	walInfo struct {
		upperSeq uint64
		nBlocks  uint32
	}

	WAL struct {
		// wg is a WaitGroup that allows us to wait for the syncThread to finish to
		// ensure a clean shutdown
		wg sync.WaitGroup

		startSeq uint64

		opts           Options
		unreleasedLogs []unreleasedLog
		logFile        file
	}

	Options struct {
		Path       string
		TargetSize int64
	}

	unreleasedLog struct {
		seq    uint64
		offset int64
		size   uint32
	}
)

func newWal(opts Options) (wal *WAL, err error) {
	// Create a new WAL.
	wal = &WAL{
		opts: opts,
	}
	wal.logFile, err = openFile(opts.Path)
	if err != nil {
		return wal, err
	}

	return wal, nil
}

func (wal *WAL) search(seq uint64) int {
	return sort.Search(len(wal.unreleasedLogs), func(i int) bool {
		return wal.unreleasedLogs[i].seq == seq
	})
}

func (wal *WAL) Scan() (seqs []uint64, err error) {
	l := len(wal.unreleasedLogs)

	for s := 0; s < l; s++ {
		seqs[s] = wal.unreleasedLogs[s].seq
	}
	sort.Slice(seqs[:], func(i, j int) bool {
		return seqs[i] < seqs[j]
	})
	return seqs, nil
}

func (wal *WAL) Read(seq uint64) ([]byte, error) {
	s := wal.search(seq)
	if s >= len(wal.unreleasedLogs) {
		return nil, errors.New("wal read error: unreleased log for seq not found")
	}
	ul := wal.unreleasedLogs[s]
	data, err := wal.logFile.readRaw(ul.offset, ul.offset+int64(ul.size))
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (wal *WAL) Sync() error {
	wal.wg.Add(1)
	defer wal.wg.Done()

	return wal.logFile.Sync()
}

// Close closes the wal, frees used resources and checks for active
// logs.
func (wal *WAL) Close() error {
	// Make sure sync thread isn't running
	wal.wg.Wait()

	// Close the logFile
	if err := wal.logFile.Sync(); err != nil {
		return err
	}
	return wal.logFile.Close()
}

// New will open a WAL. If the previous run did not shut down cleanly, a set of
// upper seq will be returned which got committed successfully to the WAL, but
// were never signaled as fully completed.
//
// If no WAL exists, a new one will be created.
//
func New(opts Options) (*WAL, error) {
	// Create a wal
	return newWal(opts)
}
