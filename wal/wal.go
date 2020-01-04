package wal

import (
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/unit-io/tracedb/fs"
)

const (
	// logStatusWritten indicates that the log has been written,
	// but not completed. During recovery, logs with this status
	// should be loaded and their updates should be provided to the user.
	logStatusWritten = iota

	// logStatusApplied indicates that the logs has been written and
	// applied. Logs with this status can be ignored during recovery,
	// and their associated blocks can be reclaimed.
	logStatusApplied

	version = 1 // file format version
)

const (
	stateInvalid = iota
	stateClean
	stateDirty
)

type (
	freeBlock struct {
		offset int64
		size   int64
	}

	// WAL write ahead logs to recover db commit failure dues to db crash or other unexpected errors
	WAL struct {
		// wg is a WaitGroup that allows us to wait for the syncThread to finish to
		// ensure a clean shutdown
		wg         sync.WaitGroup
		writeLockC chan struct{}

		// nextSeq is recoved sequence
		seq uint64

		opts Options
		logs []logInfo

		logFile file
		lock    fs.LockFile

		closed uint32
	}

	// Options wal options to create new WAL. WAL logs uses cyclic rotation to avoid fragmentation.
	// It allocates free blocks only when log reaches target size
	Options struct {
		Path       string
		TargetSize int64
	}
)

func newWal(opts Options) (wal *WAL, needRecovery bool, err error) {
	// Create a new WAL.
	wal = &WAL{
		writeLockC: make(chan struct{}, 1),
		opts:       opts,
	}
	wal.logFile, err = openFile(opts.Path, opts.TargetSize)
	if err != nil {
		return wal, false, err
	}
	if wal.logFile.size == 0 {
		if _, err = wal.logFile.allocate(headerSize); err != nil {
			return nil, false, err
		}
		wal.logFile.fb = freeBlock{
			offset: int64(headerSize),
			size:   0,
		}
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

	return wal, len(wal.logs) != 0, nil
}

func (wal *WAL) writeHeader() error {
	h := header{
		signature:     signature,
		version:       version,
		upperSequence: wal.seq,
		freeBlock: freeBlock{
			size:   wal.logFile.fb.size,
			offset: wal.logFile.fb.offset,
		},
	}
	return wal.logFile.writeMarshalableAt(h, 0)
}

func (wal *WAL) readHeader() error {
	h := &header{}
	if err := wal.logFile.readUnmarshalableAt(h, headerSize, 0); err != nil {
		return err
	}
	// if !bytes.Equal(h.signature[:], signature[:]) {
	// 	return errCorrupted
	// }
	wal.seq = h.upperSequence
	wal.logFile.fb = h.freeBlock
	return nil
}

func (wal *WAL) recoverLogHeaders() error {
	offset := int64(headerSize)
	l := &logInfo{}
	for {
		if err := wal.logFile.readUnmarshalableAt(l, uint32(logHeaderSize), offset); err != nil {
			if err == io.EOF {
				// Expected error.
				return nil
			}
			return err
		}
		if l.status == logStatusWritten {
			wal.logs = append(wal.logs, *l)
		}
		if l.seq >= wal.seq {
			break
		}
		offset = l.offset + align512(l.size+int64(logHeaderSize))
		if offset == wal.logFile.fb.offset {
			offset += wal.logFile.fb.size
		}
	}
	return nil
}

// recoverWal recovers a WAL that was written but not released logs and updates free blocks
func (wal *WAL) recoverWal() error {
	// Truncate log file.
	wal.logFile.size = align512(wal.logFile.size)
	if err := wal.logFile.Truncate(wal.logFile.size); err != nil {
		return err
	}

	return wal.recoverLogHeaders()
}

func (wal *WAL) put(log logInfo) error {
	l := len(wal.logs)
	for i := 0; i < l; i++ {
		if wal.logs[i].offset == log.offset {
			wal.logs[i].status = log.status
			wal.logs[i].entryCount = log.entryCount
			wal.logs[i].seq = log.seq
			wal.logs[i].size = log.size
			// wal.logFile.writeMarshalableAt(wal.logs[i], wal.logs[i].offset)
			return nil
		}
	}

	// wal.logFile.writeMarshalableAt(log, log.offset)
	wal.logs = append(wal.logs, log)
	return nil
}

// Scan provides list of sequeces written to log but not yet fully applied
func (wal *WAL) Scan() (seqs []uint64, err error) {
	l := len(wal.logs)
	for i := 0; i < l; i++ {
		if wal.logs[i].status == logStatusWritten {
			seqs = append(seqs, wal.logs[i].seq)
		}
	}

	return seqs, nil
}

// Reader reader is a simple iterator over log data
type Reader struct {
	entryCount  uint32
	logData     []byte
	blockOffset int64
}

func (wal *WAL) Read(seq uint64) (*Reader, error) {
	l := len(wal.logs)
	for i := 0; i < l; i++ {
		if wal.logs[i].seq == seq && wal.logs[i].entryCount > 0 {
			ul := wal.logs[i]
			data, err := wal.logFile.readRaw(ul.offset+int64(logHeaderSize), int64(ul.size))
			if err != nil {
				return nil, err
			}

			return &Reader{entryCount: wal.logs[i].entryCount, logData: data, blockOffset: 0}, nil
		}
	}

	return nil, errors.New("wal read error: log for seq not found")
}

// Next it returns netx record from the log data iterator of false if iteration is done
func (r *Reader) Next() ([]byte, bool) {
	if r.entryCount == 0 {
		return nil, false
	}
	r.entryCount--
	logData := r.logData[r.blockOffset:]
	dataLen := binary.LittleEndian.Uint32(logData[0:4])
	r.blockOffset += int64(dataLen)
	return logData[4:dataLen], true
}

// SignalLogApplied informs the WAL that it is safe to reuse blocks.
func (wal *WAL) SignalLogApplied(seq uint64) error {
	wal.wg.Add(1)
	defer wal.wg.Done()

	// sort wal logs by offset so that adjacent free blocks can be merged
	sort.Slice(wal.logs[:], func(i, j int) bool {
		return wal.logs[i].offset < wal.logs[j].offset
	})
	l := len(wal.logs)
	idx := 0
	for i := 0; i < l; i++ {
		if wal.logs[i].seq == seq {
			idx = i
			wal.logs[i].status = logStatusApplied
			wal.logFile.writeMarshalableAt(wal.logs[i], wal.logs[i].offset)
		}
	}
	if idx == 0 {
		wal.logFile.fb.offset = wal.logs[idx].offset
		wal.logFile.fb.size = align512(wal.logs[idx].size + int64(logHeaderSize))
	}
	for i := idx; i < l; i++ {
		if wal.logs[i].status == logStatusApplied && wal.logFile.fb.offset+wal.logFile.fb.size == wal.logs[i].offset {
			wal.logFile.fb.size += align512(wal.logs[i].size + int64(logHeaderSize))
		}
	}
	wal.writeHeader()

	return nil
}

// NextSeq next sequence to use in log write function
func (wal *WAL) NextSeq() uint64 {
	return atomic.AddUint64(&wal.seq, 1)
}

//Sync syncs log entries to disk
func (wal *WAL) Sync() error {
	// wal.wg.Add(1)
	// defer wal.wg.Done()

	wal.writeHeader()
	return wal.logFile.Sync()
}

// Close closes the wal, frees used resources and checks for active
// logs.
func (wal *WAL) Close() error {
	if !wal.setClosed() {
		return errors.New("wal is closed")
	}
	defer wal.logFile.Close()
	// Make sure sync thread isn't running
	wal.wg.Wait()

	return wal.logFile.Sync()
}

// Set closed flag; return true if not already closed.
func (wal *WAL) setClosed() bool {
	// TODO fixe issue with newWal code
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
