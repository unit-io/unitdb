package wal

import (
	"encoding/binary"
	"errors"
	"sync"

	"github.com/saffat-in/tracedb/collection"
)

type logHeader struct {
	status  uint16
	seq     uint64
	lSize   int64
	lOffset int64
	_       [logHeaderSize]byte
}

func (h logHeader) MarshalBinary() ([]byte, error) {
	buf := collection.NewByteWriter()
	buf.WriteUint16(h.status)
	buf.WriteUint(8, h.seq)
	buf.WriteUint(8, uint64(h.lSize))
	buf.WriteUint(8, uint64(h.lOffset))
	return buf.Bytes(), nil
}

func (h *logHeader) UnmarshalBinary(data []byte) error {
	h.status = binary.LittleEndian.Uint16(data[:2])
	h.seq = binary.LittleEndian.Uint64(data[2:10])
	h.lSize = int64(binary.LittleEndian.Uint64(data[10:18]))
	h.lOffset = int64(binary.LittleEndian.Uint64(data[18:26]))
	return nil
}

// Writer writes entries to the write ahead log.
// Thread-safe.
type Writer struct {
	writeComplete bool
	// commitComplete  bool
	releaseComplete bool

	// status indicates the status of the log. It is marshalled to
	// disk.
	status uint16

	// seq is a unique identifier for the log that orders
	// it in relation to other logs. It is marshalled to disk.
	nextSeq uint64

	// offset keeps datafile offset
	offset int64

	// log data
	logData []byte
	// log data size
	size int64

	wal *WAL
	mu  sync.Mutex

	// writeCompleted is used to signal if log is fully written
	writeCompleted chan struct{}
}

func (wal *WAL) NewWriter() (Writer, error) {
	writer := Writer{
		nextSeq:        wal.startSeq,
		wal:            wal,
		writeCompleted: make(chan struct{}),
	}

	return writer, nil
}

var bufPool = collection.NewBufferPool()

func (w *Writer) append(data []byte) error {
	if len(data) == 0 {
		return nil
	}
	w.nextSeq++
	w.size += int64(len(data))
	w.logData = append(w.logData, data...)
	return nil
}

func (w *Writer) Append(data []byte) <-chan error {
	done := make(chan error, 1)
	w.wal.wg.Add(1)
	defer w.wal.wg.Done()

	if w.writeComplete || w.releaseComplete {
		done <- errors.New("logWriter error - can't append to log once it is committed/released")
		return done
	}
	go func() {
		done <- w.append(data)
	}()
	return done
}

// writeLog writes log by setting correct header and status
func (w *Writer) writeLog() error {
	buf := bufPool.Get()
	defer bufPool.Put(buf)
	defer close(w.writeCompleted)

	// Set the transaction status
	w.status = logStatusWritten

	dataLen := align512(uint32(len(w.logData) + logHeaderSize))
	off, err := w.wal.logFile.allocate(uint32(dataLen))
	if err != nil {
		return err
	}
	w.offset = off
	h := logHeader{
		status:  w.status,
		seq:     w.nextSeq,
		lSize:   int64(len(w.logData)),
		lOffset: int64(w.offset),
	}
	logHeader, err := h.MarshalBinary()
	if err != nil {
		return err
	}
	buf.Write(logHeader)
	buf.Write(w.logData)
	if err := w.wal.logFile.append(buf.Bytes()); err != nil {
		return err
	}
	if err := w.wal.Sync(); err != nil {
		return err
	}
	w.writeComplete = true
	return nil
}

// SignalInitWrite will signal to the WAL that log append has
// completed, and that the WAL can safely write log and being
// applied atomically.
func (w *Writer) SignalInitWrite() <-chan error {
	done := make(chan error, 1)
	if w.writeComplete || w.releaseComplete {
		done <- errors.New("misuse of transaction - call each of the signaling methods exactly ones, in serial, in order")
		return done
	}
	w.wal.wg.Add(1)
	defer w.wal.wg.Done()
	// Write the log non-blocking
	go func() {
		done <- w.writeLog()
	}()
	return done
}

// SignalLogApplied informs the WAL that it is safe to reuse t's pages.
func (w *Writer) SignalLogApplied() error {
	// Make sure that the writing of log finished
	<-w.writeCompleted

	if !w.writeComplete || w.releaseComplete {
		return errors.New("WAL error - call each of the signaling methods exactly once, in serial, in order")
	}
	w.wal.wg.Add(1)
	defer w.wal.wg.Done()
	w.releaseComplete = true

	// Set the status to applied
	w.status = logStatusApplied

	logHeader := logHeader{
		status:  w.status,
		seq:     w.nextSeq,
		lSize:   w.size,
		lOffset: w.offset,
	}
	return w.wal.logFile.writeMarshalableAt(logHeader, w.offset)
}

func align512(n uint32) uint32 {
	return (n + 511) &^ 511
}
