package memdb

import (
	"encoding/binary"
)

type entry struct {
	hash      uint32
	topicSize uint16
	valueSize uint32
	expiresAt uint32
	kvOffset  int64
}

func (e entry) kvSize() uint32 {
	return uint32(e.topicSize) + keySize + e.valueSize
}

type block struct {
	entries [entriesPerBlock]entry
	next    int64
}

type blockHandle struct {
	block
	table  tableManager
	offset int64
}

const (
	entrySize        = 22
	blockSize uint32 = 512
)

func align512(n uint32) uint32 {
	return (n + 511) &^ 511
}

func (b *block) UnmarshalBinary(data []byte) error {
	for i := 0; i < entriesPerBlock; i++ {
		_ = data[entrySize] // bounds check hint to compiler; see golang.org/issue/14808
		b.entries[i].hash = binary.LittleEndian.Uint32(data[:4])
		b.entries[i].topicSize = binary.LittleEndian.Uint16(data[4:6])
		b.entries[i].valueSize = binary.LittleEndian.Uint32(data[6:10])
		b.entries[i].expiresAt = binary.LittleEndian.Uint32(data[10:14])
		b.entries[i].kvOffset = int64(binary.LittleEndian.Uint64(data[14:22]))
		data = data[entrySize:]
	}
	b.next = int64(binary.LittleEndian.Uint32(data[:4]))
	return nil
}

func (h *blockHandle) readRaw() ([]byte, error) {
	return h.table.slice(h.offset, h.offset+int64(blockSize))
}

func (h *blockHandle) read() error {
	buf, err := h.table.slice(h.offset, h.offset+int64(blockSize))
	if err != nil {
		return err
	}
	return h.UnmarshalBinary(buf)
}

func (h *blockHandle) writeRaw(raw []byte) error {
	_, err := h.table.writeAt(raw, h.offset)
	return err
}

type entryWriter struct {
	block    *blockHandle
	entryIdx int
	entry    entry
}

func (ew *entryWriter) MarshalBinary() ([]byte, error) {
	buf := make([]byte, entrySize)
	data := buf
	e := ew.entry
	binary.LittleEndian.PutUint32(buf[:4], e.hash)
	binary.LittleEndian.PutUint16(buf[4:6], e.topicSize)
	binary.LittleEndian.PutUint32(buf[6:10], e.valueSize)
	binary.LittleEndian.PutUint32(buf[10:14], e.expiresAt)
	binary.LittleEndian.PutUint64(buf[14:22], uint64(e.kvOffset))
	return data, nil
}

func (ew *entryWriter) write() error {
	buf, err := ew.MarshalBinary()
	if err != nil {
		return err
	}
	off := ew.block.offset + int64(ew.entryIdx*entrySize)
	_, err = ew.block.table.writeAt(buf, off)
	return err
}
