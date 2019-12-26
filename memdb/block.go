package memdb

// import (
// 	"encoding/binary"
// )

// type entryHeader struct {
// 	blockIndex  uint32
// 	messageSize uint32
// 	offset      int64
// }

// type entry struct {
// 	seq       uint64
// 	topicSize uint16
// 	valueSize uint32
// 	expiresAt uint32
// 	mOffset   int64
// }

// func (e entry) mSize() uint32 {
// 	return uint32(e.topicSize) + idSize + e.valueSize
// }

// type block struct {
// 	entries [entriesPerBlock]entry
// 	// next     uint32
// 	entryIdx uint16
// }

// type blockHandle struct {
// 	block
// 	table  tableManager
// 	offset int64
// }

// const (
// 	entrySize        = 26
// 	blockSize uint32 = 512
// )

// func align512(n uint32) uint32 {
// 	return (n + 511) &^ 511
// }

// func (h *blockHandle) readRaw() ([]byte, error) {
// 	return h.table.slice(h.offset, h.offset+int64(blockSize))
// }

// func (h *blockHandle) readFooter() error {
// 	// read block footer
// 	off := h.offset + int64(blockSize-18)
// 	buf, err := h.table.slice(off, h.offset+int64(blockSize))
// 	if err != nil {
// 		return err
// 	}
// 	h.entryIdx = binary.LittleEndian.Uint16(buf[:2])
// 	return nil
// }

// func (h *blockHandle) writeFooter() error {
// 	// write next entry idx to the block
// 	off := h.offset + int64(blockSize-18)
// 	buf := make([]byte, 2)
// 	binary.LittleEndian.PutUint16(buf[:2], h.entryIdx)
// 	_, err := h.table.writeAt(buf, off)
// 	return err
// }

// type entryWriter struct {
// 	block *blockHandle
// 	entry entry
// }

// func (ew *entryWriter) MarshalBinary() ([]byte, error) {
// 	buf := make([]byte, entrySize)
// 	data := buf
// 	e := ew.entry
// 	binary.LittleEndian.PutUint64(buf[:8], e.seq)
// 	binary.LittleEndian.PutUint16(buf[8:10], e.topicSize)
// 	binary.LittleEndian.PutUint32(buf[10:14], e.valueSize)
// 	binary.LittleEndian.PutUint32(buf[14:18], e.expiresAt)
// 	binary.LittleEndian.PutUint64(buf[18:26], uint64(e.mOffset))

// 	return data, nil
// }

// func (ew *entryWriter) write() error {
// 	buf, err := ew.MarshalBinary()
// 	if err != nil {
// 		return err
// 	}
// 	off := ew.block.offset + int64(ew.block.entryIdx*entrySize)
// 	_, err = ew.block.table.writeAt(buf, off)
// 	if err != nil {
// 		return err
// 	}
// 	ew.block.entryIdx++
// 	return ew.block.writeFooter()
// }
