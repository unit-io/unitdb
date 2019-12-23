package wal

import (
	"encoding/binary"
)

var (
	signature  = [8]byte{'t', 'r', 'a', 'c', 'e', 'd', 'b', '\xfd'}
	headerSize uint32
)

type logInfo struct {
	status uint16
	seq    uint64
	size   int64
	offset int64

	_ [26]byte
}

func init() {
	headerSize = uint32(binary.Size(logInfo{}))
}

func (l logInfo) MarshalBinary() ([]byte, error) {
	buf := make([]byte, headerSize)
	binary.LittleEndian.PutUint16(buf[:2], l.status)
	binary.LittleEndian.PutUint64(buf[2:10], l.seq)
	binary.LittleEndian.PutUint64(buf[10:18], uint64(l.size))
	binary.LittleEndian.PutUint64(buf[18:26], uint64(l.offset))
	return buf, nil
}

func (l *logInfo) UnmarshalBinary(data []byte) error {
	l.status = binary.LittleEndian.Uint16(data[:2])
	l.seq = binary.LittleEndian.Uint64(data[2:10])
	l.size = int64(binary.LittleEndian.Uint64(data[10:18]))
	l.offset = int64(binary.LittleEndian.Uint64(data[18:26]))
	return nil
}
