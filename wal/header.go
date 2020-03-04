package wal

import (
	"encoding/binary"
)

var (
	signature     = [8]byte{'t', 'r', 'a', 'c', 'e', 'd', 'b', '\xfd'}
	logHeaderSize = 30
	headerSize    uint32
)

type logInfo struct {
	status     uint16
	entryCount uint32
	seq        uint64 // log sequence
	upperSeq   uint64 // db sequence
	size       int64
	offset     int64

	_ [30]byte
}

// MarshalBinary serialized logInfo into binary data
func (l logInfo) MarshalBinary() ([]byte, error) {
	buf := make([]byte, logHeaderSize)
	binary.LittleEndian.PutUint16(buf[:2], l.status)
	binary.LittleEndian.PutUint32(buf[2:6], l.entryCount)
	binary.LittleEndian.PutUint64(buf[6:14], l.seq)
	binary.LittleEndian.PutUint64(buf[14:22], uint64(l.size))
	binary.LittleEndian.PutUint64(buf[22:30], uint64(l.offset))
	return buf, nil
}

// UnmarshalBinary deserialized logInfo from binary data
func (l *logInfo) UnmarshalBinary(data []byte) error {
	l.status = binary.LittleEndian.Uint16(data[:2])
	l.entryCount = binary.LittleEndian.Uint32(data[2:6])
	l.seq = binary.LittleEndian.Uint64(data[6:14])
	l.size = int64(binary.LittleEndian.Uint64(data[14:22]))
	l.offset = int64(binary.LittleEndian.Uint64(data[22:30]))
	return nil
}

type header struct {
	signature [8]byte
	version   uint32
	seq       uint64
	freeBlock
	_ [256]byte
}

func init() {
	headerSize = uint32(align512(int64(binary.Size(logInfo{}))))
}

// MarshalBinary serialized header into binary data
func (h header) MarshalBinary() ([]byte, error) {
	buf := make([]byte, headerSize)
	copy(buf[:8], h.signature[:])
	binary.LittleEndian.PutUint32(buf[8:12], h.version)
	binary.LittleEndian.PutUint64(buf[12:20], h.seq)
	binary.LittleEndian.PutUint64(buf[20:28], uint64(h.freeBlock.size))
	binary.LittleEndian.PutUint64(buf[28:36], uint64(h.freeBlock.offset))
	binary.LittleEndian.PutUint64(buf[36:44], uint64(h.freeBlock.currSize))
	binary.LittleEndian.PutUint64(buf[44:52], uint64(h.freeBlock.currOffset))
	return buf, nil
}

// UnmarshalBinary deserialized header from binary data
func (h *header) UnmarshalBinary(data []byte) error {
	copy(h.signature[:], data[:8])
	h.version = binary.LittleEndian.Uint32(data[8:12])
	h.seq = binary.LittleEndian.Uint64(data[12:20])
	h.freeBlock.size = int64(binary.LittleEndian.Uint64(data[20:28]))
	h.freeBlock.offset = int64(binary.LittleEndian.Uint64(data[28:36]))
	h.freeBlock.currSize = int64(binary.LittleEndian.Uint64(data[36:44]))
	h.freeBlock.currOffset = int64(binary.LittleEndian.Uint64(data[44:52]))
	return nil
}
