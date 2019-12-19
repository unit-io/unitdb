package wal

import (
	"encoding/binary"
	"fmt"
	"hash"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"

	"github.com/saffat-in/tracedb/fs"
)

const (
	// MaxRecordBytes is the largest size a single record can be.
	MaxRecordBytes uint32 = 100 * 1024 * 1024
)

// const (
// 	checksumSize = 16
// 	blockSize    = 512
// 	blockHeader  = 8 // nextBlock offset

// 	// MaxPayloadSize is the number of bytes that can fit into a single
// 	// block. For best performance, the number of blocks written should be
// 	// minimized, so clients should try to keep the length of log update's
// 	// field slightly below a multiple of MaxPayloadSize.
// 	MaxPayloadSize = blockSize - blockHeader

// 	// firstblockHeader is the size of the marshalled non-payload data of a
// 	// log's firstBlock. It includes the logStatus, sequence number,
// 	// checksum, and nextBlock offset.
// 	firstblockHeader = 8 + 8 + checksumSize + 8

// 	// maxFirstPayloadSize is the number of bytes that can fit into the first
// 	// block of a transaction. The first block holds more metadata than the
// 	// subsequent blocks, so its maximum payload is smaller.
// 	maxFirstPayloadSize = blockSize - firstblockHeader
// )

// const (
// 	// logStatusInvalid indicates an incorrectly initialized block.
// 	logStatusInvalid = iota

// 	// logStatusWritten indicates that the log has been written, but
// 	// not fully committed, meaning it should be ignored upon recovery.
// 	logStatusWritten

// 	// logStatusCommitted indicates that the log has been committed,
// 	// but not completed. During recovery, logs with this status
// 	// should be loaded and their updates should be provided to the user.
// 	logStatusComitted

// 	// logStatusApplied indicates that the log has been committed and
// 	// applied. Logs with this status can be ignored during recovery,
// 	// and their associated blocks can be reclaimed.
// 	logStatusApplied
// )

// const (
// 	recoveryStateInvalid = iota
// 	recoveryStateClean
// 	recoveryStateUnclean
// )

// // A checksum is a 128-bit blake2b hash.
// type checksum [checksumSize]byte

var (
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

// // block is linked list of on-disk blocks that comprise a set of log updates.
// type block struct {
// 	// nextBlock points to the logical next block in the logFile.
// 	// The page may not appear in the file in-order. When
// 	// marshalled, this value is encoded as nextBlock.offset. If nextBlock is
// 	// nil, it is encoded as math.MaxUint64.
// 	nextBlock *block

// 	// offset is the offset in the file that this log has. It is not
// 	// marshalled to disk.
// 	offset uint64

// 	// data contains the marshalled log updates, which may be spread over
// 	// multiple blocks. If spread over multiple blocks, the data can be
// 	// assembled via concatenation.
// 	data []byte
// }

// func (b block) size() int { return blockHeader + len(b.data) }

// func (b block) nextOffset() uint64 {
// 	if b.nextBlock == nil {
// 		return math.MaxUint64
// 	}
// 	return b.nextBlock.offset
// }

// // appendTo appends the marshalled bytes of b to buf, returning the new slice.
// func (b *block) appendTo(buf []byte) []byte {
// 	if b.size() > blockSize {
// 		panic(fmt.Sprintf("sanity check failed: block is %d bytes too large", b.size()-blockSize))
// 	}

// 	// if buf has enough capacity to hold b, use it; otherwise allocate
// 	var ibuf []byte
// 	if rest := buf[len(buf):]; cap(rest) >= b.size() {
// 		ibuf = rest[:b.size()]
// 	} else {
// 		ibuf = make([]byte, b.size())
// 	}

// 	// write page contents
// 	binary.LittleEndian.PutUint64(ibuf[0:], b.nextOffset())
// 	copy(ibuf[8:], b.data)

// 	return append(buf, ibuf...)
// }

// Writer writes entries to the write ahead log.
// Thread-safe.
type Writer struct {
	// // setupComplete, writeComplete, and releaseComplete signal the progress of
	// // the log writer, and should be set to 'true' in order.
	// //
	// // When setupComplete is set to true, it means that the creater of the
	// // transaction is ready for the transaction to be committed.
	// //
	// // When writeComplete is set to true, it means that the WAL has
	// // successfully and fully written the log.
	// //
	// // releaseComplete is set to true when the caller has fully written the
	// // log, meaning the log can be over-written safely in the
	// // WAL, and the on-disk pages can be reclaimed for future writes.
	// setupComplete   bool
	// writeComplete   bool
	// releaseComplete bool

	// // status indicates the status of the log writer. It is marshalled to
	// // disk. See consts.go for an explanation of each status type.
	// status uint64

	// // firstBlock is the first block of the log. It is marshalled to
	// // disk. Note that because additional log metadata (status,
	// // sequenceNumber, checksum) is marshalled alongside firstBlock, the
	// // capacity of firstBlock.payload is smaller than subsequent blocks.
	// //
	// // firstBlock is never nil for valid logs.
	// firstBlock *block

	// seq is a unique identifier for the log that orders
	// it in relation to other logs. It is marshalled to disk.
	seq uint64
	crc hash.Hash32

	wal *WAL
	mu  sync.Mutex
}

func (wal *WAL) NewWriter() (Writer, error) {
	writer := Writer{
		seq: wal.startSeq,
		wal: wal,
		crc: crc32.New(crcTable),
	}

	if wal.index.FileManager == nil || wal.data.FileManager == nil || wal.data.size > wal.opts.TargetSize {
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

type file struct {
	fs.FileManager
	size int64
}

func openFile(name string) (file, error) {
	fileFlag := os.O_CREATE | os.O_RDWR
	fileMode := os.FileMode(0666)
	fs := fs.FileIO
	fi, err := fs.OpenFile(name, fileFlag, fileMode)
	f := file{}
	if err != nil {
		return f, err
	}
	f.FileManager = fi

	stat, err := fi.Stat()
	if err != nil {
		return f, err
	}
	f.size = stat.Size()
	return f, err
}

func (f *file) extend(size uint32) (int64, error) {
	off := f.size
	if err := f.Truncate(off + int64(size)); err != nil {
		return 0, err
	}
	f.size += int64(size)
	return off, nil
}

func (f *file) append(data []byte) error {
	off := f.size
	if _, err := f.WriteAt(data, off); err != nil {
		return err
	}
	f.size += int64(len(data))
	return nil
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

	done := make(chan error, 1)
	dataLen := align512(uint32(w.seq + uint64(len(data))))
	buf := make([]byte, dataLen)
	seq := make([]byte, 8)
	binary.LittleEndian.PutUint64(seq[0:8], w.seq)
	copy(buf, seq)
	copy(data[8:], data)

	go func() {
		done <- w.wal.data.append(buf)
	}()
	return done
}

func indexName(nextSeq uint64, o Options) string {
	return fmt.Sprintf("%s%cwal-%d.index", o.Dirname, os.PathSeparator, nextSeq)
}

func logName(nextSeq uint64, o Options) string {
	return fmt.Sprintf("%s%cwal-%d.log", o.Dirname, os.PathSeparator, nextSeq)
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
