package wal

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
)

// LogStatus represents the state of log, written to applied
type LogStatus uint16

const (
	// logStatusWritten indicates that the log has been written,
	// but not completed. During recovery, logs with this status
	// should be loaded and their updates should be provided to the user.
	logStatusWritten LogStatus = iota

	// logStatusApplied indicates that the logs has been written and
	// applied. Logs with this status can be ignored during recovery,
	// and their associated blocks can be reclaimed.
	logStatusApplied

	// logStatusReleased indicates that the logs has been applied and merged with freeblocks.
	// Logs with this status are removed from the WAL.
	logStatusReleased

	defaultBufferSize = 1 << 27
	version           = 1 // file format version
)

type (
	// WAL write ahead logs to recover db commit failure dues to db crash or other unexpected errors
	WAL struct {
		// wg is a WaitGroup that allows us to wait for the syncThread to finish to
		// ensure a clean shutdown
		wg sync.WaitGroup
		mu sync.RWMutex

		// DB seq successfully written to the logfile in wal
		seq uint64

		logs []logInfo

		bufPool *bpool.BufferPool
		logFile file

		opts Options
		// close
		closed uint32
		closeC chan struct{}
	}

	// Options wal options to create new WAL. WAL logs uses cyclic rotation to avoid fragmentation.
	// It allocates free blocks only when log reaches target size
	Options struct {
		Path       string
		TargetSize int64
		BufferSize int64
	}
)

func newWal(opts Options) (wal *WAL, needsRecovery bool, err error) {
	// Create a new WAL.
	if opts.BufferSize == 0 {
		opts.BufferSize = defaultBufferSize
	}
	wal = &WAL{
		bufPool: bpool.NewBufferPool(opts.TargetSize, nil),
		opts:    opts,
		// close
		closeC: make(chan struct{}, 1),
	}
	wal.logFile, err = openFile(opts.Path, opts.TargetSize)
	if err != nil {
		return wal, false, err
	}
	if wal.logFile.size == 0 {
		if _, err = wal.logFile.allocate(headerSize); err != nil {
			return nil, false, err
		}
		wal.logFile.fb = newFreeBlock()
		if err := wal.writeHeader(); err != nil {
			return nil, false, err
		}
	} else {
		if err := wal.readHeader(); err != nil {
			if err := wal.Close(); err != nil {
				return nil, false, errors.New("newWal error: unable to read wal header")
			}
			return nil, false, err
		}
		if err := wal.recoverWal(); err != nil {
			return nil, false, err
		}
	}

	go wal.startLogReleaser()
	return wal, len(wal.logs) != 0, nil
}

func (wal *WAL) writeHeader() error {
	h := header{
		signature: signature,
		version:   version,
		seq:       atomic.LoadUint64(&wal.seq),
		fb:        wal.logFile.fb,
	}
	return wal.logFile.writeMarshalableAt(h, 0)
}

func (wal *WAL) readHeader() error {
	h := &header{}
	if err := wal.logFile.readUnmarshalableAt(h, headerSize, 0); err != nil {
		return err
	}
	wal.seq = h.seq
	wal.logFile.fb = h.fb
	return nil
}

func (wal *WAL) recoverLogHeaders() error {
	offset := int64(headerSize)
	l := logInfo{}
	for {
		offset = wal.logFile.fb.recoveryOffset(offset)
		if err := wal.logFile.readUnmarshalableAt(&l, uint32(logHeaderSize), offset); err != nil {
			if err == io.EOF {
				// Expected error.
				return nil
			}
			return err
		}
		if l.offset < 0 || l.status > logStatusReleased {
			fmt.Println("wal.recoverLogHeader: logInfo ", wal.logFile.fb, l)
			return nil
		}
		wal.logs = append(wal.logs, l)
		offset = l.offset + l.size
	}
}

// recoverWal recovers a WAL for the log written but not released. It also updates free blocks
func (wal *WAL) recoverWal() error {
	// Truncate log file.
	if err := wal.logFile.Truncate(wal.logFile.size); err != nil {
		return err
	}

	if err := wal.recoverLogHeaders(); err != nil {
		return err
	}

	return wal.releaseLogs()
}

func (wal *WAL) put(log logInfo) error {
	// l := len(wal.logs)
	log.version = version
	if wal.seq < log.seq {
		wal.seq = log.seq
	}
	wal.logs = append(wal.logs, log)
	return nil
}

func (wal *WAL) logMerge(log logInfo) (released bool, err error) {
	if log.status == logStatusWritten {
		return false, nil
	}
	released = wal.logFile.fb.free(log.offset, log.size)
	wal.logFile.fb.swap(wal.opts.TargetSize)
	return released, nil
}

func (wal *WAL) signalLogApplied(i int) error {
	wal.logs[i].status = logStatusApplied
	released, err := wal.logMerge(wal.logs[i])
	if err != nil {
		return err
	}
	if released {
		wal.logs[i].status = logStatusReleased
	} else {
		wal.logFile.writeMarshalableAt(wal.logs[i], wal.logs[i].offset)
	}
	return nil
}

// SignalLogApplied informs the WAL that it is safe to reuse blocks.
func (wal *WAL) SignalLogApplied(upperSeq uint64) error {
	wal.mu.Lock()
	wal.wg.Add(1)
	defer func() {
		wal.wg.Done()
		wal.mu.Unlock()
	}()

	// sort wal logs by offset so that adjacent free blocks can be merged
	sort.Slice(wal.logs[:], func(i, j int) bool {
		return wal.logs[i].offset < wal.logs[j].offset
	})
	l := len(wal.logs)
	for i := 0; i < l; i++ {
		if wal.logs[i].status == logStatusWritten && wal.logs[i].seq <= upperSeq {
			if err := wal.signalLogApplied(i); err != nil {
				return err
			}
		}
	}
	for i := 0; i < l; i++ {
		if wal.logs[i].status == logStatusReleased {
			// Remove log from wal
			wal.logs = wal.logs[:i+copy(wal.logs[i:], wal.logs[i+1:])]
			l -= 1
			i--
		}
	}
	return wal.writeHeader()
}

// Seq returns upper DB sequence successfully written to the logfile in wal
func (wal *WAL) Seq() uint64 {
	return atomic.LoadUint64(&wal.seq)
}

//Sync syncs log entries to disk
func (wal *WAL) Sync() error {
	wal.writeHeader()
	return wal.logFile.Sync()
}

// Close closes the wal, frees used resources and checks for active
// logs.
func (wal *WAL) Close() error {
	if !wal.setClosed() {
		return errors.New("wal is closed")
	}
	close(wal.closeC)
	// Make sure sync thread isn't running
	wal.wg.Wait()
	return wal.logFile.Close()
}

// Set closed flag; return true if not already closed.
func (wal *WAL) setClosed() bool {
	if wal == nil {
		return false
	}
	return atomic.CompareAndSwapUint32(&wal.closed, 0, 1)
}

// Check whether WAL was closed.
func (wal *WAL) isClosed() bool {
	return atomic.LoadUint32(&wal.closed) != 0
}

// Check read ok status.
func (wal *WAL) ok() error {
	if wal.isClosed() {
		return errors.New("wal is closed.")
	}
	return nil
}

func (wal *WAL) releaseLogs() error {
	wal.mu.Lock()
	wal.wg.Add(1)
	defer func() {
		wal.wg.Done()
		wal.mu.Unlock()
	}()

	// sort wal logs by offset so that adjacent free blocks can be merged
	sort.Slice(wal.logs[:], func(i, j int) bool {
		return wal.logs[i].offset < wal.logs[j].offset
	})
	l := len(wal.logs)
	for i := 0; i < l; i++ {
		released, err := wal.logMerge(wal.logs[i])
		if err != nil {
			return err
		}
		if released {
			wal.logs[i].status = logStatusReleased
		}
	}

	if err := wal.writeHeader(); err != nil {
		return err
	}

	for i := 0; i < l; i++ {
		if wal.logs[i].status == logStatusReleased {
			// Remove log from wal
			wal.logs = wal.logs[:i+copy(wal.logs[i:], wal.logs[i+1:])]
			l -= 1
			i--
		}
	}
	return nil
}

func (wal *WAL) startLogReleaser() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case <-wal.closeC:
			return
		case <-ticker.C:
			wal.releaseLogs()
		}
	}
}

// New will open a WAL. If the previous run did not shut down cleanly, a set of
// upper seq will be returned which got committed successfully to the WAL, but
// were never signaled as fully completed.
//
// If no WAL exists, a new one will be created.
//
func New(opts Options) (*WAL, bool, error) {
	// Create a wal
	return newWal(opts)
}
