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
	mOffset   int64

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
	binary.LittleEndian.PutUint64(buf[18:26], uint64(e.mOffset))
	binary.LittleEndian.PutUint64(buf[26:34], uint64(e.topicOffset))
	return data, nil
}

// MarshalBinary deserliazed entry from binary data
func (e *entry) UnmarshalBinary(data []byte) error {
	e.seq = binary.LittleEndian.Uint64(data[:8])
	e.topicSize = binary.LittleEndian.Uint16(data[8:10])
	e.valueSize = binary.LittleEndian.Uint32(data[10:14])
	e.expiresAt = binary.LittleEndian.Uint32(data[14:18])
	e.mOffset = int64(binary.LittleEndian.Uint64(data[18:26]))
	e.topicOffset = int64(binary.LittleEndian.Uint64(data[26:34]))
	return nil
}

type block struct {
	entries  [entriesPerBlock]entry
	next     uint32
	entryIdx uint16
}

type blockHandle struct {
	block
	file   fs.FileManager
	offset int64
}

const (
	entrySize            = 34
	entrySlotSize        = 26
	blockSize     uint32 = 4096
)

func align512(n uint32) uint32 {
	return (n + 511) &^ 511
}

// MarshalBinary serliazed entries block into binary data
func (b block) MarshalBinary() ([]byte, error) {
	buf := make([]byte, blockSize)
	data := buf
	for i := 0; i < entriesPerBlock; i++ {
		e := b.entries[i]
		binary.LittleEndian.PutUint64(buf[:8], e.seq)
		binary.LittleEndian.PutUint16(buf[8:10], e.topicSize)
		binary.LittleEndian.PutUint32(buf[10:14], e.valueSize)
		binary.LittleEndian.PutUint32(buf[14:18], e.expiresAt)
		binary.LittleEndian.PutUint64(buf[18:26], uint64(e.mOffset))
		buf = buf[entrySlotSize:]
	}
	binary.LittleEndian.PutUint32(buf[:4], b.next)
	binary.LittleEndian.PutUint16(buf[4:6], b.entryIdx)
	return data, nil
}

// UnmarshalBinary dserliazed entries block from binary data
func (b *block) UnmarshalBinary(data []byte) error {
	for i := 0; i < entriesPerBlock; i++ {
		_ = data[entrySlotSize] // bounds check hint to compiler; see golang.org/issue/14808
		b.entries[i].seq = binary.LittleEndian.Uint64(data[:8])
		b.entries[i].topicSize = binary.LittleEndian.Uint16(data[8:10])
		b.entries[i].valueSize = binary.LittleEndian.Uint32(data[10:14])
		b.entries[i].expiresAt = binary.LittleEndian.Uint32(data[14:18])
		b.entries[i].mOffset = int64(binary.LittleEndian.Uint64(data[18:26]))
		data = data[entrySlotSize:]
	}
	b.next = binary.LittleEndian.Uint32(data[:4])
	b.entryIdx = binary.LittleEndian.Uint16(data[4:6])
	return nil
}

func (b *block) del(entryIdx int) {
	i := entryIdx
	for ; i < entriesPerBlock-1; i++ {
		b.entries[i] = b.entries[i+1]
	}
	b.entries[i] = entry{}
}

func (h *blockHandle) read() error {
	buf, err := h.file.Slice(h.offset, h.offset+int64(blockSize))
	if err != nil {
		return err
	}
	return h.UnmarshalBinary(buf)
}

func (h *blockHandle) write() error {
	buf, err := h.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = h.file.WriteAt(buf, h.offset)
	return err
}
