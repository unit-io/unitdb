package wal

import (
	"encoding/binary"
	"errors"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/uid"
)

// Writer writes entries to the write ahead log.
// Thread-safe.
type Writer struct {
	Id              uid.LID
	writeComplete   bool
	releaseComplete bool

	entryCount uint32

	buffer  *bpool.Buffer
	logSize int64

	wal *WAL

	// writeCompleted is used to signal if log is fully written
	writeCompleted chan struct{}
}

// NewWriter returns new log writer to write to WAL
func (wal *WAL) NewWriter() (*Writer, error) {
	if err := wal.ok(); err != nil {
		return &Writer{wal: wal}, err
	}
	w := &Writer{
		Id:             uid.NewLID(),
		wal:            wal,
		writeCompleted: make(chan struct{}, 1),
	}

	w.buffer = wal.bufPool.Get()
	return w, nil
}

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

// Append appends records into WAL
func (w *Writer) Append(data []byte) <-chan error {
	done := make(chan error, 1)

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
func (w *Writer) writeLog(seq uint64) error {
	w.writeCompleted <- struct{}{}
	w.wal.wg.Add(1)
	defer func() {
		w.wal.wg.Done()
		<-w.writeCompleted
		w.wal.bufPool.Put(w.buffer)
	}()

	if w.logSize == 0 {
		return nil
	}
	dataLen := w.logSize + int64(logHeaderSize)
	off, err := w.wal.logFile.allocate(uint32(dataLen))
	if off < int64(headerSize) || err != nil {
		return err
	}
	h := logInfo{
		status:     logStatusWritten,
		entryCount: w.entryCount,
		seq:        seq,
		size:       dataLen,
		offset:     int64(off),
	}
	if err := w.wal.put(h); err != nil {
		return err
	}
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
func (w *Writer) SignalInitWrite(seq uint64) <-chan error {
	done := make(chan error, 1)
	if w.writeComplete || w.releaseComplete {
		done <- errors.New("misuse of log write - call each of the signaling methods exactly ones, in serial, in order")
		return done
	}

	// Write the log non-blocking
	go func() {
		done <- w.writeLog(seq)
	}()
	return done
}

// func align(n int64) int64 {
// 	return (n + 511) &^ 511
// }
