package memdb

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

const (
	// MaxRecordBytes is the largest size a single record can be.
	MaxRecordBytes uint32 = 100 * 1024 * 1024
)

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// Writer writes entries to the write ahead log.
// Thread-safe.
type writer struct {
	blockIndex uint32
	crc        hash.Hash32
	filename   string
	size       int64
	opts       options
	mu         sync.Mutex
	// wg is a WaitGroup that allows us to wait for the syncThread to finish to
	// ensure a clean shutdown
	wg sync.WaitGroup

	f         *os.File
	h         *os.File
	bufWriter *bufio.Writer
}

type options struct {
	Dirname    string
	TargetSize int64
}

func newWriter(blockIndex uint32, opts options) (*writer, error) {
	w := &writer{
		blockIndex: blockIndex,
		crc:        crc32.New(crcTable),
		opts:       opts,
	}
	if err := w.rollover(blockIndex); err != nil {
		return nil, err
	}
	return w, nil
}

type rawRecord struct {
	blockIndex uint32
	data       []byte
	checkSum   uint32
}

func (w *writer) writeHeader(r rawRecord) error {
	w.wg.Add(1)
	defer w.wg.Done()
	fn := fmt.Sprintf("%s%cwal-header.log", w.opts.Dirname, os.PathSeparator)
	ensureDir(fn)

	f, err := os.Create(fn)
	if err != nil {
		return err
	}

	w.filename = fn
	w.f = f
	w.bufWriter = bufio.NewWriter(f)
	w.size = 0

	if _, err := w.bufWriter.Write(r.data); err != nil {
		return err
	}
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

// Append appends a log record to the WAL. The log record is modified with the log sequence number.
// cb is invoked serially, in log sequence number order.
func (w *writer) append(blockIndex uint32, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	dataLen := len(data)
	if uint32(dataLen) > MaxRecordBytes {
		return fmt.Errorf("log record has encoded size %d that exceeds %d", dataLen, MaxRecordBytes)
	}

	w.crc.Reset()
	if _, err := w.crc.Write(data); err != nil {
		return err
	}
	c := w.crc.Sum32()

	dataCopy := make([]byte, dataLen)
	copy(dataCopy, data)

	r := rawRecord{
		blockIndex: blockIndex,
		data:       dataCopy,
		checkSum:   c,
	}
	if err := w.writeRawRecord(r); err != nil {
		return err
	}
	return nil
}

func logHeader(o options) string {
	return fmt.Sprintf("%s%cwal-header.log", o.Dirname, os.PathSeparator)
}

func logName(blockIndex uint32, o options) string {
	return fmt.Sprintf("%s%cwal-%d.log", o.Dirname, os.PathSeparator, blockIndex)
}

func (w *writer) rollover(blockIndex uint32) error {
	fn := logName(blockIndex, w.opts)
	ensureDir(fn)
	if w.bufWriter != nil {
		if err := w.bufWriter.Flush(); err != nil {
			return err
		}
		if err := w.f.Sync(); err != nil {
			return err
		}
		if err := w.f.Close(); err != nil {
			return err
		}
	}
	f, err := os.Create(fn)
	if err != nil {
		return err
	}

	w.blockIndex = blockIndex
	w.filename = fn
	w.f = f
	w.bufWriter = bufio.NewWriter(f)
	w.size = 0

	return nil
}

func (w *writer) writeRawRecord(r rawRecord) error {
	w.wg.Add(1)
	defer w.wg.Done()
	if w.blockIndex < r.blockIndex || w.size > w.opts.TargetSize {
		if err := w.rollover(r.blockIndex); err != nil {
			return err
		}
	}

	var scratch [8]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(r.data)))
	binary.LittleEndian.PutUint32(scratch[4:8], r.checkSum)

	if _, err := w.bufWriter.Write(scratch[:]); err != nil {
		return err
	}
	w.size += int64(len(r.data)) + 8

	if _, err := w.bufWriter.Write(r.data); err != nil {
		return err
	}

	return nil
}

func (w *writer) sync() error {
	w.wg.Add(1)
	defer w.wg.Done()
	if err := w.bufWriter.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *writer) close() error {
	// Make sure sync thread isn't running
	w.wg.Wait()

	// Close the logFile
	return w.f.Close()
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
