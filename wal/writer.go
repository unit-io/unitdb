package wal

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"sync"

	"github.com/saffat-in/tracedb/fs"
)

const (
	// MaxRecordBytes is the largest size a single record can be.
	MaxRecordBytes uint32 = 100 * 1024 * 1024
)

const (
	checksumSize = 16
	blockSize    = 512
	blockHeader  = 8 // nextBlock offset

	// MaxPayloadSize is the number of bytes that can fit into a single
	// block. For best performance, the number of blocks written should be
	// minimized, so clients should try to keep the length of log update's
	// field slightly below a multiple of MaxPayloadSize.
	MaxPayloadSize = blockSize - blockHeader

	// firstblockHeader is the size of the marshalled non-payload data of a
	// log's firstBlock. It includes the logStatus, sequence number,
	// checksum, and nextBlock offset.
	firstblockHeader = 8 + 8 + checksumSize + 8

	// maxFirstPayloadSize is the number of bytes that can fit into the first
	// block of a transaction. The first block holds more metadata than the
	// subsequent blocks, so its maximum payload is smaller.
	maxFirstPayloadSize = blockSize - firstblockHeader
)

const (
	// logStatusInvalid indicates an incorrectly initialized block.
	logStatusInvalid = iota

	// logStatusWritten indicates that the log has been written, but
	// not fully committed, meaning it should be ignored upon recovery.
	txnStatusWritten

	// logStatusCommitted indicates that the log has been committed,
	// but not completed. During recovery, logs with this status
	// should be loaded and their updates should be provided to the user.
	logStatusComitted

	// logStatusApplied indicates that the log has been committed and
	// applied. Logs with this status can be ignored during recovery,
	// and their associated blocks can be reclaimed.
	logStatusApplied
)

const (
	recoveryStateInvalid = iota
	recoveryStateClean
	recoveryStateUnclean
)

// A checksum is a 128-bit blake2b hash.
type checksum [checksumSize]byte

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// block is linked list of on-disk blocks that comprise a set of log updates.
type block struct {
	// nextBlock points to the logical next block in the logFile.
	// The page may not appear in the file in-order. When
	// marshalled, this value is encoded as nextBlock.offset. If nextBlock is
	// nil, it is encoded as math.MaxUint64.
	nextBlock *block

	// offset is the offset in the file that this log has. It is not
	// marshalled to disk.
	offset uint64

	// data contains the marshalled log updates, which may be spread over
	// multiple blocks. If spread over multiple blocks, the data can be
	// assembled via concatenation.
	data []byte
}

func (b block) size() int { return blockHeader + len(b.data) }

func (b block) nextOffset() uint64 {
	if b.nextBlock == nil {
		return math.MaxUint64
	}
	return b.nextBlock.offset
}

// appendTo appends the marshalled bytes of b to buf, returning the new slice.
func (b *block) appendTo(buf []byte) []byte {
	if b.size() > blockSize {
		panic(fmt.Sprintf("sanity check failed: block is %d bytes too large", b.size()-blockSize))
	}

	// if buf has enough capacity to hold b, use it; otherwise allocate
	var ibuf []byte
	if rest := buf[len(buf):]; cap(rest) >= b.size() {
		ibuf = rest[:b.size()]
	} else {
		ibuf = make([]byte, b.size())
	}

	// write page contents
	binary.LittleEndian.PutUint64(ibuf[0:], b.nextOffset())
	copy(ibuf[8:], b.data)

	return append(buf, ibuf...)
}

// Writer writes entries to the write ahead log.
// Thread-safe.
type Writer struct {
	// setupComplete, writeComplete, and releaseComplete signal the progress of
	// the log writer, and should be set to 'true' in order.
	//
	// When setupComplete is set to true, it means that the creater of the
	// transaction is ready for the transaction to be committed.
	//
	// When writeComplete is set to true, it means that the WAL has
	// successfully and fully written the log.
	//
	// releaseComplete is set to true when the caller has fully written the
	// log, meaning the log can be over-written safely in the
	// WAL, and the on-disk pages can be reclaimed for future writes.
	setupComplete   bool
	writeComplete   bool
	releaseComplete bool

	// status indicates the status of the log writer. It is marshalled to
	// disk. See consts.go for an explanation of each status type.
	status uint64

	// firstBlock is the first block of the log. It is marshalled to
	// disk. Note that because additional log metadata (status,
	// sequenceNumber, checksum) is marshalled alongside firstBlock, the
	// capacity of firstBlock.payload is smaller than subsequent blocks.
	//
	// firstBlock is never nil for valid logs.
	firstBlock *block

	// seq is a unique identifier for the log that orders
	// it in relation to other logs. It is marshalled to disk.
	seq uint64
	crc hash.Hash32

	mu sync.Mutex
	// wg is a WaitGroup that allows us to wait for the syncThread to finish to
	// ensure a clean shutdown
	wg sync.WaitGroup

	fs.FileManager
	size    int64
	maxSize int64
}

type Options struct {
	Dirname    string
	TargetSize int64
}

func NewWriter(nextSeq uint64, opts Options) (Writer, error) {
	fileFlag := os.O_CREATE | os.O_RDWR
	fileMode := os.FileMode(0666)
	fs := fs.FileIO
	fn := logName(nextSeq, opts)
	ensureDir(fn)
	fi, err := fs.OpenFile(fn, fileFlag, fileMode)
	w := Writer{seq: nextSeq, crc: crc32.New(crcTable)}
	if err != nil {
		return w, err
	}
	w.FileManager = fi
	stat, err := fi.Stat()
	if err != nil {
		return w, err
	}
	w.maxSize = opts.TargetSize
	if w.size > opts.TargetSize {
		w.size = blockSize // rollover
		return w, err
	}
	w.size = stat.Size()
	return w, err
}

type rawRecord struct {
	seq      uint64
	data     []byte
	checkSum uint32
}

func (w *Writer) Extend(size uint32) (int64, error) {
	off := w.size
	if err := w.Truncate(off + int64(size)); err != nil {
		return 0, err
	}
	w.size += int64(size)
	return off, nil

}

// Append appends a log record to the WAL. The log record is modified with the log sequence number.
// cb is invoked serially, in log sequence number order.
func (w *Writer) Append(seq uint64, data []byte) error {
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
		seq:      seq,
		data:     dataCopy,
		checkSum: c,
	}
	if err := w.writeRawRecord(r); err != nil {
		return err
	}
	return nil
}

func logName(nextSeq uint64, o Options) string {
	return fmt.Sprintf("%s%cwal-%d.log", o.Dirname, os.PathSeparator, nextSeq)
}

func (w *Writer) writeRawRecord(r rawRecord) error {
	w.wg.Add(1)
	defer w.wg.Done()
	if w.size > w.maxSize {
		w.size = blockSize
	}

	var scratch [8]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(r.data)))
	binary.LittleEndian.PutUint32(scratch[4:8], r.checkSum)

	off := w.size
	if _, err := w.WriteAt(scratch[:], off); err != nil {
		return err
	}
	off += 8
	w.size += int64(len(r.data)) + 8

	if _, err := w.WriteAt(r.data, off); err != nil {
		return err
	}

	return nil
}

func (w *Writer) Sync() error {
	w.wg.Add(1)
	defer w.wg.Done()
	return w.FileManager.Sync()
}

func (w *Writer) Close() error {
	// Make sure sync thread isn't running
	w.wg.Wait()

	// Close the logFile
	return w.FileManager.Close()
}

func (w *Writer) Size() int64 {
	return w.size
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
