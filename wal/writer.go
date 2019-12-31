package wal

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/unit-io/tracedb/collection"
)

// Writer writes entries to the write ahead log.
// Thread-safe.
type Writer struct {
	writeComplete bool
	// commitComplete  bool
	releaseComplete bool

	// status indicates the status of the log. It is marshalled to
	// disk.
	status uint16

	// startSeq is a unique identifier for the log that orders
	// it in relation to other logs. It is marshalled to disk.
	startSeq   uint64
	entryCount uint32

	buffer  *bytes.Buffer
	logSize int64

	wal *WAL

	// writeCompleted is used to signal if log is fully written
	writeCompleted chan struct{}
}

func (wal *WAL) NewWriter() (writer Writer, err error) {
	if err := wal.ok(); err != nil {
		return writer, err
	}
	writer = Writer{
		startSeq:       wal.seq,
		buffer:         bufPool.Get(),
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

	w.entryCount++

	var scratch [4]byte
	dataLen := uint32(len(data) + 4)
	binary.LittleEndian.PutUint32(scratch[0:4], dataLen)

	if _, err := w.buffer.Write(scratch[:]); err != nil {
		return err
	}
	w.logSize += int64(dataLen)

	if _, err := w.buffer.Write(data); err != nil {
		return err
	}

	return nil
}

func (w *Writer) Append(data []byte) <-chan error {
	done := make(chan error, 1)
	// w.wal.wg.Add(1)
	// defer w.wal.wg.Done()

	if w.writeComplete || w.releaseComplete {
		done <- errors.New("logWriter error - can't append to log once it is written/released")
		return done
	}
	go func() {
		done <- w.append(data)
	}()
	return done
}

// writeLog writes log by setting correct header and status
func (w *Writer) writeLog() error {
	defer close(w.writeCompleted)
	defer bufPool.Put(w.buffer)

	// Set the transaction status
	w.status = logStatusWritten
	if w.logSize == 0 {
		return nil
	}
	dataLen := align512(w.logSize + int64(logHeaderSize))
	off, err := w.wal.logFile.allocate(uint32(dataLen))
	if off < int64(headerSize) || err != nil {
		return err
	}
	h := logInfo{
		status:     logStatusWritten,
		entryCount: w.entryCount,
		seq:        w.wal.nextSeq(),
		size:       int64(w.logSize),
		offset:     int64(off),
	}
	w.wal.put(h)
	if err := w.wal.logFile.writeMarshalableAt(h, off); err != nil {
		return err
	}
	if _, err := w.wal.logFile.WriteAt(w.buffer.Bytes(), off+int64(logHeaderSize)); err != nil {
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
		done <- errors.New("misuse of log write - call each of the signaling methods exactly ones, in serial, in order")
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

func align512(n int64) int64 {
	return (n + 511) &^ 511
}
