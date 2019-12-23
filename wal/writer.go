package wal

import (
	"errors"
	"sync"

	"github.com/saffat-in/tracedb/collection"
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

	// nextSeq is a unique identifier for the log that orders
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

	dataLen := align512(uint32(len(data)) + headerSize)
	off, err := w.wal.logFile.allocate(uint32(dataLen))
	w.offset = off
	if err != nil {
		return err
	}
	h := logInfo{
		status: logStatusWritten,
		seq:    w.nextSeq,
		size:   int64(len(data)),
		offset: int64(off),
	}
	w.wal.put(h)
	header, err := h.MarshalBinary()
	if _, err := w.wal.logFile.WriteAt(header, w.offset); err != nil {
		return err
	}
	if _, err := w.wal.logFile.WriteAt(data, w.offset+int64(headerSize)); err != nil {
		return err
	}
	return nil
}

func (w *Writer) Append(data []byte) <-chan error {
	done := make(chan error, 1)
	w.wal.wg.Add(1)
	defer w.wal.wg.Done()

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

	// Set the transaction status
	w.status = logStatusWritten
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

func align512(n uint32) uint32 {
	return (n + 511) &^ 511
}
