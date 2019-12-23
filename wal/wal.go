package wal

import (
	"errors"
	"sync"

	"github.com/saffat-in/tracedb/fs"
)

const (
	// logStatusInvalid indicates an incorrectly initialized block.
	logStatusInvalid = iota

	// logStatusCommitted indicates that the log has been written,
	// but not completed. During recovery, logs with this status
	// should be loaded and their updates should be provided to the user.
	logStatusWritten = iota

	// logStatusApplied indicates that the logs has been written and
	// applied. Logs with this status can be ignored during recovery,
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

		opts Options
		logs []logInfo

		logFile file
		lock    fs.LockFile
	}

	Options struct {
		Path       string
		TargetSize int64
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

func (wal *WAL) put(log logInfo) error {
	l := len(wal.logs)
	for i := 0; i < l; i++ {
		if wal.logs[i].seq == log.seq {
			wal.logs[i].status = log.status
			// wal.logFile.writeMarshalableAt(wal.logs[i], wal.logs[i].offset)
			return nil
		}
	}

	// wal.logFile.writeMarshalableAt(log, log.offset)
	wal.logs = append(wal.logs, log)
	return nil
}

func (wal *WAL) Scan() (seqs []uint64, err error) {
	l := len(wal.logs)
	// seqs = make([]uint64, l)
	for i := 0; i < l; i++ {
		if wal.logs[i].status == logStatusWritten {
			seqs = append(seqs, wal.logs[i].seq)
		}
	}

	return seqs, nil
}

func (wal *WAL) Read(seq uint64) ([]byte, error) {
	l := len(wal.logs)
	for i := 0; i < l; i++ {
		if wal.logs[i].seq == seq {
			ul := wal.logs[i]
			data, err := wal.logFile.readRaw(ul.offset+int64(headerSize), int64(ul.size))
			if err != nil {
				return nil, err
			}
			return data, nil
		}
	}

	return nil, errors.New("wal read error: log for seq not found")
}

// SignalLogApplied informs the WAL that it is safe to reuse blocks.
func (wal *WAL) SignalLogApplied(seq uint64) error {
	wal.wg.Add(1)
	defer wal.wg.Done()

	l := len(wal.logs)
	for i := 0; i < l; i++ {
		if wal.logs[i].seq == seq {
			h := logInfo{
				status: logStatusApplied,
				seq:    seq,
				size:   wal.logs[i].size,
				offset: wal.logs[i].offset,
			}
			wal.put(h)
			wal.logFile.writeMarshalableAt(h, wal.logs[i].offset)
			return nil
		}
	}

	return errors.New("wal error: log for seq not found")
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
