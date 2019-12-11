package memdb

import (
	"encoding/binary"
)

type entryHeader struct {
	blockIndex uint32
	valueSize  uint32
	offset     int64
}

type entry struct {
	hash      uint32
	topicSize uint16
	valueSize uint32
	expiresAt uint32
	tmOffset  int64
}

func (e entry) mSize() uint32 {
	return uint32(e.topicSize) + idSize + e.valueSize
}

type block struct {
	entries  [entriesPerBlock]entry
	next     uint32
	entryIdx uint16
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
		b.entries[i].tmOffset = int64(binary.LittleEndian.Uint64(data[14:22]))
		data = data[entrySize:]
	}
	b.next = binary.LittleEndian.Uint32(data[:4])
	b.entryIdx = binary.LittleEndian.Uint16(data[4:6])
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

func (h *blockHandle) readFooter() error {
	// read block footer
	off := h.offset + int64(blockSize-6)
	buf, err := h.table.slice(off, h.offset+int64(blockSize))
	if err != nil {
		return err
	}
	h.next = binary.LittleEndian.Uint32(buf[:4])
	h.entryIdx = binary.LittleEndian.Uint16(buf[4:6])
	return nil
}

func (h *blockHandle) writeFooter() error {
	// write next entry idx to the block
	off := h.offset + int64(blockSize-6)
	buf := make([]byte, 6)
	binary.LittleEndian.PutUint32(buf[:4], h.next)
	binary.LittleEndian.PutUint16(buf[4:6], h.entryIdx)
	_, err := h.table.writeAt(buf, off)
	return err
}

func (h *blockHandle) writeRaw(raw []byte) error {
	_, err := h.table.writeAt(raw, h.offset)
	return err
}

type entryWriter struct {
	block *blockHandle
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
	binary.LittleEndian.PutUint64(buf[14:22], uint64(e.tmOffset))

	return data, nil
}

func (ew *entryWriter) write() error {
	buf, err := ew.MarshalBinary()
	if err != nil {
		return err
	}
	off := ew.block.offset + int64(ew.block.entryIdx*entrySize)
	_, err = ew.block.table.writeAt(buf, off)
	if err != nil {
		return err
	}
	ew.block.entryIdx++
	return ew.block.writeFooter()
}

func (ew *entryWriter) del() error {
	ew.entry = entry{}
	buf, err := ew.MarshalBinary()
	if err != nil {
		return err
	}
	off := ew.block.offset + int64(ew.block.entryIdx*entrySize)
	if _, err = ew.block.table.writeAt(buf, off); err != nil {
		return err
	}
	return nil
}
