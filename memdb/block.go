package memdb

import (
	"encoding/binary"
)

type entry struct {
	seq       uint64
	hash      uint32
	topicSize uint16
	valueSize uint32
	expiresAt uint32
	mOffset   int64
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
	entrySize        = 30
	blockSize uint32 = 696
)

func align512(n uint32) uint32 {
	return (n + 511) &^ 511
}

func (b *block) UnmarshalBinary(data []byte) error {
	for i := 0; i < entriesPerBlock; i++ {
		_ = data[entrySize] // bounds check hint to compiler; see golang.org/issue/14808
		b.entries[i].seq = binary.LittleEndian.Uint64(data[:8])
		b.entries[i].hash = binary.LittleEndian.Uint32(data[8:12])
		b.entries[i].topicSize = binary.LittleEndian.Uint16(data[12:14])
		b.entries[i].valueSize = binary.LittleEndian.Uint32(data[14:18])
		b.entries[i].expiresAt = binary.LittleEndian.Uint32(data[18:22])
		b.entries[i].mOffset = int64(binary.LittleEndian.Uint64(data[22:30]))
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
	binary.LittleEndian.PutUint64(buf[:8], e.seq)
	binary.LittleEndian.PutUint32(buf[8:12], e.hash)
	binary.LittleEndian.PutUint16(buf[12:14], e.topicSize)
	binary.LittleEndian.PutUint32(buf[14:18], e.valueSize)
	binary.LittleEndian.PutUint32(buf[18:22], e.expiresAt)
	binary.LittleEndian.PutUint64(buf[22:30], uint64(e.mOffset))

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
	ew.block.entryIdx = ew.block.entryIdx + 1
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
