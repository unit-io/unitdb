package tracedb

import (
	"encoding/binary"
	"time"

	"github.com/saffat-in/tracedb/fs"
	"github.com/saffat-in/tracedb/memdb"
)

type entry struct {
	seq       uint64
	hash      uint32
	topicSize uint16
	valueSize uint32
	expiresAt uint32
	mOffset   int64
}

func (e entry) timeStamp() uint32 {
	return e.expiresAt
}

func (e entry) isExpired() bool {
	return e.expiresAt != 0 && e.expiresAt <= uint32(time.Now().Unix())
}

func (e entry) mSize() uint32 {
	return idSize + uint32(e.topicSize) + e.valueSize
}

type block struct {
	entries  [entriesPerBlock]entry
	next     uint32
	entryIdx uint16
}

type blockHandle struct {
	block
	table  fs.FileManager
	offset int64

	updated bool
	cache   *memdb.DB
	cacheID uint64
}

const (
	entrySize        = 22
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
		binary.LittleEndian.PutUint16(buf[4:6], e.topicSize)
		binary.LittleEndian.PutUint32(buf[6:10], e.valueSize)
		binary.LittleEndian.PutUint32(buf[10:14], e.expiresAt)
		binary.LittleEndian.PutUint64(buf[14:22], uint64(e.mOffset))
		buf = buf[entrySize:]
	}
	binary.LittleEndian.PutUint32(buf[:4], b.next)
	binary.LittleEndian.PutUint16(buf[4:6], b.entryIdx)
	return data, nil
}

func (b *block) UnmarshalBinary(data []byte) error {
	for i := 0; i < entriesPerBlock; i++ {
		_ = data[entrySize] // bounds check hint to compiler; see golang.org/issue/14808
		b.entries[i].hash = binary.LittleEndian.Uint32(data[:4])
		b.entries[i].topicSize = binary.LittleEndian.Uint16(data[4:6])
		b.entries[i].valueSize = binary.LittleEndian.Uint32(data[6:10])
		b.entries[i].expiresAt = binary.LittleEndian.Uint32(data[10:14])
		b.entries[i].mOffset = int64(binary.LittleEndian.Uint64(data[14:22]))
		data = data[entrySize:]
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

func (h *blockHandle) readRaw() ([]byte, error) {
	return h.table.Slice(h.offset, h.offset+int64(blockSize))
}

func (h *blockHandle) read(fillCache bool) error {
	// var cacheKey uint64
	// if h.cache != nil {
	// 	cacheKey = h.cacheID ^ uint64(h.offset)
	// 	if data, _ := h.cache.Get(cacheKey, blockSize); data != nil && len(data) == int(blockSize) {
	// 		return h.UnmarshalBinary(data)
	// 	}
	// }

	buf, err := h.table.Slice(h.offset, h.offset+int64(blockSize))
	if err != nil {
		return err
	}

	// if h.cache != nil && fillCache {
	// 	h.cache.Set(cacheKey, h.offset, buf)
	// }

	return h.UnmarshalBinary(buf)
}

func (h *blockHandle) write() error {
	buf, err := h.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = h.table.WriteAt(buf, h.offset)
	// if h.cache != nil {
	// 	cacheKey := h.cacheID ^ uint64(h.offset)
	// 	h.cache.Delete(cacheKey)
	// }
	return err
}

func (h *blockHandle) writeRaw(raw []byte) error {
	_, err := h.table.WriteAt(raw, h.offset)
	return err
}

type entryWriter struct {
	block *blockHandle
	buf   []byte
	entry entry
}

func (ew *entryWriter) MarshalBinary() ([]byte, error) {
	buf := make([]byte, entrySize)
	data := buf
	e := ew.entry
	binary.LittleEndian.PutUint32(buf[:4], e.hash)
	binary.LittleEndian.PutUint16(buf[4:6], e.topicSize)
	binary.LittleEndian.PutUint32(buf[6:10], e.valueSize)
	binary.LittleEndian.PutUint32(buf[10:14], e.expiresAt)
	binary.LittleEndian.PutUint64(buf[14:22], uint64(e.mOffset))

	return data, nil
}

func (ew *entryWriter) write() error {
	buf, err := ew.MarshalBinary()
	if err != nil {
		return err
	}
	ew.buf = append(ew.buf, buf...)
	return nil
}
