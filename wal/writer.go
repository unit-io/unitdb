package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type logHeader struct {
	status uint64
	seq    uint64
	_      [logHeaderSize]byte
}

func (h logHeader) MarshalBinary() ([]byte, error) {
	buf := make([]byte, logHeaderSize)
	binary.LittleEndian.PutUint64(buf[:8], h.status)
	binary.LittleEndian.PutUint64(buf[8:16], h.seq)
	return buf, nil
}

func (h *logHeader) UnmarshalBinary(data []byte) error {
	h.status = binary.LittleEndian.Uint64(data[:8])
	h.seq = binary.LittleEndian.Uint64(data[8:16])
	return nil
}

// Writer writes entries to the write ahead log.
// Thread-safe.
type Writer struct {
	writeComplete   bool
	releaseComplete bool

	// status indicates the status of the log. It is marshalled to
	// disk.
	status uint64

	// seq is a unique identifier for the log that orders
	// it in relation to other logs. It is marshalled to disk.
	seq uint64

	// offset keeps datafile offset
	offset int64

	wal *WAL
	mu  sync.Mutex
}

func (wal *WAL) NewWriter() (Writer, error) {
	writer := Writer{
		seq: wal.startSeq,
		wal: wal,
	}

	if wal.index.FileManager == nil || wal.data.FileManager == nil {
		indexFile := indexName(wal.startSeq, wal.opts)
		ensureDir(indexFile)
		index, err := openFile(indexFile)
		if err != nil {
			return writer, err
		}
		dataFile := logName(wal.startSeq, wal.opts)
		ensureDir(dataFile)
		data, err := openFile(dataFile)
		if err != nil {
			return writer, err
		}
		wal.index = index
		wal.data = data
	}
	return writer, nil
}

func (w *Writer) WriteBlock(block []byte) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- w.wal.index.append(block)
	}()
	return done
}

func (w *Writer) WriteData(data []byte) <-chan error {
	w.wal.wg.Add(1)
	defer w.wal.wg.Done()

	// Set the transaction status
	w.status = logStatusWritten
	done := make(chan error, 1)
	dataLen := align512(uint32(len(data) + 16))
	buf := make([]byte, dataLen)
	scratch := make([]byte, 16)
	binary.LittleEndian.PutUint64(scratch[0:8], w.status)
	binary.LittleEndian.PutUint64(scratch[8:16], w.seq)
	copy(buf, scratch)
	copy(buf[16:], data)
	off, err := w.wal.data.allocate(uint32(dataLen))
	if err != nil {
		done <- err
		return done
	}
	w.offset = off
	go func() {
		done <- w.wal.data.append(buf)
	}()
	w.writeComplete = true
	return done
}

// SignalUpdatesApplied informs the WAL that it is safe to reuse t's pages.
func (w *Writer) SignalBatchCommited() error {
	if !w.writeComplete || w.releaseComplete {
		return errors.New("WAL error - call each of the signaling methods exactly once, in serial, in order")
	}
	w.wal.wg.Add(1)
	defer w.wal.wg.Done()
	w.releaseComplete = true

	// Set the status to applied
	w.status = logStatusApplied

	scratch := logHeader{
		status: w.status,
		seq:    w.seq,
	}
	return w.wal.data.writeMarshalableAt(scratch, w.offset)
}

func indexName(nextSeq uint64, o Options) string {
	return fmt.Sprintf("%s_log%c%s-%d.index", o.Path, os.PathSeparator, o.Path, nextSeq)
}

func logName(nextSeq uint64, o Options) string {
	return fmt.Sprintf("%s_log%c%s-%d.data", o.Path, os.PathSeparator, o.Path, nextSeq)
}

func (w *Writer) Sync() error {
	w.wal.wg.Add(1)
	defer w.wal.wg.Done()
	if err := w.wal.index.Sync(); err != nil {
		return err
	}
	return w.wal.data.Sync()
}

func (w *Writer) Close() error {
	// Make sure sync thread isn't running
	w.wal.wg.Wait()

	// Close the logFile
	if err := w.wal.index.Sync(); err != nil {
		return err
	}
	return w.wal.data.Sync()
}

func align512(n uint32) uint32 {
	return (n + 511) &^ 511
}

func ensureDir(fileName string) {
	dirName := filepath.Dir(fileName)
	if _, serr := os.Stat(dirName); serr != nil {
		merr := os.MkdirAll(dirName, os.ModePerm)
		if merr != nil {
			panic(merr)
		}
	}
}
