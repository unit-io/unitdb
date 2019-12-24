package wal

import (
	"encoding/binary"
	"errors"
	"sort"
	"sync"
	"sync/atomic"

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

	version = 1 // file format version
)

type (
	freeBlock struct {
		offset int64
		size   int64
	}

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
	}

	Options struct {
		Path       string
		TargetSize int64
	}
)

func newWal(opts Options) (wal *WAL, err error) {
	// Create a new WAL.
	wal = &WAL{
		writeLockC: make(chan struct{}, 1),
		opts:       opts,
	}
	wal.logFile, err = openFile(opts.Path)
	if err != nil {
		return wal, err
	}
	return wal, nil
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

func (wal *WAL) readHeader(readFreeList bool) error {
	h := &header{}
	if err := wal.logFile.readUnmarshalableAt(h, headerSize, 0); err != nil {
		return err
	}
	// if !bytes.Equal(h.signature[:], signature[:]) {
	// 	return errCorrupted
	// }
	wal.logFile.fb = h.freeBlock
	return nil
}

// recoverWal recovers a WAL that was written but not released logs and updates free blocks
func (wal *WAL) recoverWal() {

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

type Reader struct {
	entryCount  uint32
	logData     []byte
	blockOffset int64
}

func (wal *WAL) Read(seq uint64) (*Reader, error) {
	l := len(wal.logs)
	for i := 0; i < l; i++ {
		if wal.logs[i].seq == seq {
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

func (r *Reader) Next() ([]byte, bool) {
	if r.entryCount == 0 {
		return nil, false
	}
	r.entryCount--
	logData := r.logData[r.blockOffset:]
	dataLen := binary.LittleEndian.Uint32(logData[0:4])
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
	for i := 0; i < l; i++ {
		if wal.logs[i].seq == seq {
			wal.logs[i].status = logStatusApplied
			switch {
			case i > 0 && wal.logs[i-1].status == logStatusApplied:
				wal.logs[i-1].size += wal.logs[i].size
				copy(wal.logs[i:], wal.logs[i+1:])
				wal.logs = wal.logs[:len(wal.logs)-1]
				wal.logFile.fb = freeBlock{
					offset: wal.logs[i-1].offset,
					size:   wal.logs[i-1].size,
				}
				wal.logFile.writeMarshalableAt(wal.logs[i-1], wal.logs[i-1].offset)
			default:
				wal.logFile.writeMarshalableAt(wal.logs[i], wal.logs[i].offset)
			}
			return nil
		}
	}

	return errors.New("wal error: log for seq not found")
}

func (wal *WAL) nextSeq() uint64 {
	return atomic.AddUint64(&wal.seq, 1)
}

func (wal *WAL) Sync() error {
	wal.wg.Add(1)
	defer wal.wg.Done()

	// wal.writeHeader()
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
