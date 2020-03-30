package wal

import (
	"encoding/binary"
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

	DefaultBufferSize = 1 << 27
	version           = 1 // file format version
)

type (
	freeBlock struct {
		offset     int64
		size       int64
		currOffset int64
		currSize   int64
	}

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
		opts.BufferSize = DefaultBufferSize
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
		wal.logFile.fb = freeBlock{
			offset:     int64(headerSize),
			size:       0,
			currOffset: int64(headerSize),
			currSize:   0,
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

	go wal.startDefrag()
	return wal, len(wal.logs) != 0, nil
}

func (wal *WAL) writeHeader() error {
	h := header{
		signature: signature,
		version:   version,
		seq:       atomic.LoadUint64(&wal.seq),
		freeBlock: freeBlock{
			offset:     wal.logFile.fb.offset,
			size:       wal.logFile.fb.size,
			currOffset: wal.logFile.fb.currOffset,
			currSize:   wal.logFile.fb.currSize,
		},
	}
	return wal.logFile.writeMarshalableAt(h, 0)
}

func (wal *WAL) readHeader() error {
	h := &header{}
	if err := wal.logFile.readUnmarshalableAt(h, headerSize, 0); err != nil {
		return err
	}
	wal.seq = h.seq
	wal.logFile.fb = h.freeBlock
	return nil
}

func (wal *WAL) recoverLogHeaders() error {
	offset := int64(headerSize)
	l := logInfo{}
	for {
		if offset == wal.logFile.fb.currOffset {
			offset += wal.logFile.fb.currSize
		}
		if err := wal.logFile.readUnmarshalableAt(&l, uint32(logHeaderSize), offset); err != nil {
			if err == io.EOF {
				// Expected error.
				return nil
			}
			return err
		}
		if l.offset < 0 || l.status > logStatusApplied {
			fmt.Println("wal.recoverLogHeader: logInfo ", l)
			return nil
		}
		if ok, err := wal.logMerge(l); !ok {
			if err != nil {
				return err
			}
			wal.logs = append(wal.logs, l)
		}
		offset = l.offset + align(l.size+int64(logHeaderSize))
	}
}

// recoverWal recovers a WAL for the log written but not released. It also updates free blocks
func (wal *WAL) recoverWal() error {
	// Truncate log file.
	wal.logFile.size = align(wal.logFile.size)
	if err := wal.logFile.Truncate(wal.logFile.size); err != nil {
		return err
	}

	return wal.recoverLogHeaders()
}

func (wal *WAL) put(log logInfo) error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	l := len(wal.logs)
	log.version = version
	if wal.seq < log.seq {
		wal.seq = log.seq
	}
	for i := 0; i < l; i++ {
		if wal.logs[i].offset == log.offset {
			wal.logs[i].status = log.status
			wal.logs[i].entryCount = log.entryCount
			wal.logs[i].seq = log.seq
			wal.logs[i].size = log.size
			return nil
		}
	}
	wal.logs = append(wal.logs, log)
	return nil
}

// Reader reader is a simple iterator over log data
type Reader struct {
	entryCount  uint32
	logData     []byte
	blockOffset int64
}

// Read reads log written to the WAL but fully applied. It returns Reader iterator
func (wal *WAL) Read(f func(uint64, bool, *Reader) (bool, error)) (err error) {
	// func (wal *WAL) Read() (*Reader, error) {
	wal.mu.RLock()
	defer wal.mu.RUnlock()
	// mergedLogs := wal.defrag()
	idx := 0
	l := len(wal.logs)
	bufSize := wal.opts.BufferSize
	fileOff := wal.logs[0].offset
	size := wal.logFile.Size() - fileOff
	buffer := wal.bufPool.Get()
	defer wal.bufPool.Put(buffer)
	// for _, log := range mergedLogs {

	for {
		buffer.Reset()
		offset := int64(logHeaderSize)
		if size <= bufSize {
			bufSize = size
		}
		if _, err := buffer.Extend(bufSize); err != nil {
			return err
		}
		if _, err := wal.logFile.readAt(buffer.Internal(), fileOff); err != nil {
			return err
		}
		for i := idx; i < l; i++ {
			ul := wal.logs[i]
			if bufSize < ul.size {
				bufSize += ul.size
				break
			}
			if ul.offset == wal.logFile.fb.currOffset+wal.logFile.fb.currSize && ul.offset != fileOff {
				offset += wal.logFile.fb.currSize
			}
			if bufSize-offset < ul.size {
				fileOff = ul.offset
				size = wal.logFile.Size() - ul.offset
				break
			}
			data, err := buffer.Slice(offset, offset+ul.size)
			if err != nil {
				return err
			}
			r := &Reader{entryCount: ul.entryCount, logData: data, blockOffset: 0}
			if stop, err := f(ul.seq, idx == l-1, r); stop || err != nil {
				return err
			}
			offset += align(ul.size + int64(logHeaderSize))

			idx++
		}
		if idx == l {
			break
		}
	}
	return nil
}

// Count returns entry count in the current reader
func (r *Reader) Count() uint32 {
	return r.entryCount
}

// Next returns next record from the log data iterator or false if iteration is done
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

func (wal *WAL) logMerge(log logInfo) (merged bool, err error) {
	if log.status != logStatusApplied {
		return false, nil
	}
	if wal.logFile.fb.currOffset+wal.logFile.fb.currSize == log.offset {
		wal.logFile.fb.currSize += align(log.size + int64(logHeaderSize))
	} else {
		if wal.logFile.fb.offset+wal.logFile.fb.size == log.offset {
			wal.logFile.fb.size += align(log.size + int64(logHeaderSize))
		}
		// reset current free block
		if wal.logFile.fb.size != 0 && wal.logFile.fb.offset+wal.logFile.fb.size >= wal.logFile.fb.currOffset {
			wal.logFile.fb.currOffset = wal.logFile.fb.offset
			wal.logFile.fb.currSize += align(wal.logFile.fb.size)
			wal.logFile.fb.size = 0
		}
	}
	// fmt.Println("wal.logMerge: fb ", wal.logFile.fb)
	return true, nil
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
			wal.logs[i].status = logStatusApplied
			if _, err := wal.logMerge(wal.logs[i]); err != nil {
				return err
			}
			wal.logFile.writeMarshalableAt(wal.logs[i], wal.logs[i].offset)
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

func (wal *WAL) defrag() error {
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
		ok, err := wal.logMerge(wal.logs[i])
		if err != nil {
			return err
		}
		if ok {
			wal.logFile.writeMarshalableAt(wal.logs[i], wal.logs[i].offset)
		}
	}

	if err := wal.writeHeader(); err != nil {
		return err
	}

	for i := 0; i < l; i++ {
		if wal.logs[i].status == logStatusApplied {
			// Remove log from wal
			wal.logs = wal.logs[:i+copy(wal.logs[i:], wal.logs[i+1:])]
			l -= 1
			i--
		}
	}
	return nil
}

func (wal *WAL) startDefrag() {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case <-wal.closeC:
			return
		case <-ticker.C:
			wal.defrag()
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
