package tracedb

import (
	"encoding/binary"
	"time"

	"github.com/unit-io/tracedb/fs"
)

type entry struct {
	seq       uint64
	topicSize uint16
	valueSize uint32
	expiresAt uint32
	msgOffset int64

	topicOffset int64
	cacheBlock  []byte
}

func (e entry) time() uint32 {
	return e.expiresAt
}

func (e entry) Seq() uint64 {
	return e.seq
}

func (e entry) isExpired() bool {
	return e.expiresAt != 0 && e.expiresAt <= uint32(time.Now().Unix())
}

func (e entry) mSize() uint32 {
	return idSize + uint32(e.topicSize) + e.valueSize
}

// MarshalBinary serliazed entry into binary data
func (e entry) MarshalBinary() ([]byte, error) {
	buf := make([]byte, entrySize)
	data := buf
	binary.LittleEndian.PutUint64(buf[:8], e.seq)
	binary.LittleEndian.PutUint16(buf[8:10], e.topicSize)
	binary.LittleEndian.PutUint32(buf[10:14], e.valueSize)
	binary.LittleEndian.PutUint32(buf[14:18], e.expiresAt)
	binary.LittleEndian.PutUint64(buf[18:26], uint64(e.topicOffset))
	return data, nil
}

// MarshalBinary deserliazed entry from binary data
func (e *entry) UnmarshalBinary(data []byte) error {
	e.seq = binary.LittleEndian.Uint64(data[:8])
	e.topicSize = binary.LittleEndian.Uint16(data[8:10])
	e.valueSize = binary.LittleEndian.Uint32(data[10:14])
	e.expiresAt = binary.LittleEndian.Uint32(data[14:18])
	e.topicOffset = int64(binary.LittleEndian.Uint64(data[18:26]))
	return nil
}

type block struct {
	entries  [entriesPerIndexBlock]entry
	next     uint32
	entryIdx uint16
}

type blockHandle struct {
	block
	file   fs.FileManager
	offset int64

	leased bool
}

const (
	entrySize        = 26
	blockSize uint32 = 4096
)

func align(n uint32) uint32 {
	return (n + 511) &^ 511
}

// MarshalBinary serliazed entries block into binary data
func (b block) MarshalBinary() []byte {
	buf := make([]byte, blockSize)
	data := buf
	for i := 0; i < entriesPerIndexBlock; i++ {
		e := b.entries[i]
		binary.LittleEndian.PutUint64(buf[:8], e.seq)
		binary.LittleEndian.PutUint16(buf[8:10], e.topicSize)
		binary.LittleEndian.PutUint32(buf[10:14], e.valueSize)
		binary.LittleEndian.PutUint32(buf[14:18], e.expiresAt)
		binary.LittleEndian.PutUint64(buf[18:26], uint64(e.msgOffset))
		buf = buf[entrySize:]
	}
	binary.LittleEndian.PutUint32(buf[:4], b.next)
	binary.LittleEndian.PutUint16(buf[4:6], b.entryIdx)
	return data
}

// UnmarshalBinary deserliazed entries block from binary data
func (b *block) UnmarshalBinary(data []byte) error {
	for i := 0; i < entriesPerIndexBlock; i++ {
		_ = data[entrySize] // bounds check hint to compiler; see golang.org/issue/14808
		b.entries[i].seq = binary.LittleEndian.Uint64(data[:8])
		b.entries[i].topicSize = binary.LittleEndian.Uint16(data[8:10])
		b.entries[i].valueSize = binary.LittleEndian.Uint32(data[10:14])
		b.entries[i].expiresAt = binary.LittleEndian.Uint32(data[14:18])
		b.entries[i].msgOffset = int64(binary.LittleEndian.Uint64(data[18:26]))
		data = data[entrySize:]
	}
	b.next = binary.LittleEndian.Uint32(data[:4])
	b.entryIdx = binary.LittleEndian.Uint16(data[4:6])
	return nil
}

func (b *block) del(entryIdx int) {
	i := entryIdx
	for ; i < entriesPerIndexBlock-1; i++ {
		b.entries[i] = b.entries[i+1]
	}
	b.entries[i] = entry{}
}

func (bh *blockHandle) read() error {
	buf, err := bh.file.Slice(bh.offset, bh.offset+int64(blockSize))
	if err != nil {
		return err
	}
	return bh.UnmarshalBinary(buf)
}

func (bh *blockHandle) write() error {
	if bh.entryIdx == 0 {
		return nil
	}
	buf := bh.MarshalBinary()
	_, err := bh.file.WriteAt(buf, bh.offset)
	return err
}
