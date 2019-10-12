package tracedb

import (
	"encoding/binary"
	"time"

	"github.com/saffat-in/tracedb/fs"
)

type entry struct {
	hash      uint32
	keySize   uint16
	valueSize uint32
	expiresAt uint32
	kvOffset  int64
}

func (e entry) timeStamp() uint32 {
	return e.expiresAt
}

func (e entry) isExpired() bool {
	return e.expiresAt != 0 && e.expiresAt <= uint32(time.Now().Unix())
}

func (e entry) kvSize() uint32 {
	return uint32(e.keySize) + e.valueSize
}

type block struct {
	entries [entriesPerBlock]entry
	next    int64
}

type blockHandle struct {
	block
	file   fs.FileManager
	offset int64
}

const (
	blockSize uint32 = 512
)

func align512(n uint32) uint32 {
	return (n + 511) &^ 511
}

func (b block) MarshalBinary() ([]byte, error) {
	buf := make([]byte, blockSize)
	data := buf
	for i := 0; i < entriesPerBlock; i++ {
		e := b.entries[i]
		binary.LittleEndian.PutUint32(buf[:4], e.hash)
		binary.LittleEndian.PutUint16(buf[4:6], e.keySize)
		binary.LittleEndian.PutUint32(buf[6:10], e.valueSize)
		binary.LittleEndian.PutUint32(buf[10:14], e.expiresAt)
		binary.LittleEndian.PutUint64(buf[14:22], uint64(e.kvOffset))
		buf = buf[22:]
	}
	binary.LittleEndian.PutUint64(buf[:8], uint64(b.next))
	return data, nil
}

func (b *block) UnmarshalBinary(data []byte) error {
	for i := 0; i < entriesPerBlock; i++ {
		_ = data[22] // bounds check hint to compiler; see golang.org/issue/14808
		b.entries[i].hash = binary.LittleEndian.Uint32(data[:4])
		b.entries[i].keySize = binary.LittleEndian.Uint16(data[4:6])
		b.entries[i].valueSize = binary.LittleEndian.Uint32(data[6:10])
		b.entries[i].expiresAt = binary.LittleEndian.Uint32(data[10:14])
		b.entries[i].kvOffset = int64(binary.LittleEndian.Uint64(data[14:22]))
		data = data[22:]
	}
	b.next = int64(binary.LittleEndian.Uint64(data[:8]))
	return nil
}

func (b *block) del(entryIdx int) {
	i := entryIdx
	for ; i < entriesPerBlock-1; i++ {
		b.entries[i] = b.entries[i+1]
	}
	b.entries[i] = entry{}
}

func (b *blockHandle) read() error {
	buf, err := b.file.Slice(b.offset, b.offset+int64(blockSize))
	if err != nil {
		return err
	}
	return b.UnmarshalBinary(buf)
}

func (b *blockHandle) write() error {
	buf, err := b.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = b.file.WriteAt(buf, b.offset)
	return err
}

type entryWriter struct {
	block      *blockHandle
	entryIdx   int
	prevblocks []*blockHandle
}

func (ew *entryWriter) insert(sl entry, db *DB) error {
	if ew.entryIdx == entriesPerBlock {
		nextblock, err := db.createOverflowBlock()
		if err != nil {
			return err
		}
		ew.block.next = nextblock.offset
		ew.prevblocks = append(ew.prevblocks, ew.block)
		ew.block = nextblock
		ew.entryIdx = 0
	}
	ew.block.entries[ew.entryIdx] = sl
	ew.entryIdx++
	return nil
}

func (ew *entryWriter) write() error {
	for i := len(ew.prevblocks) - 1; i >= 0; i-- {
		if err := ew.prevblocks[i].write(); err != nil {
			return err
		}
	}
	return ew.block.write()
}
