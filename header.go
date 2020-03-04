package tracedb

import (
	"encoding/binary"
)

var (
	signature  = [7]byte{'t', 'r', 'a', 'c', 'e', 'd', 'b'}
	headerSize uint32
)

type header struct {
	signature [7]byte
	version   uint32
	dbInfo
	_ [256]byte
}

func init() {
	headerSize = align512(uint32(binary.Size(header{})))
}

// MarshalBinary serializes header into binary data
func (h header) MarshalBinary() ([]byte, error) {
	buf := make([]byte, headerSize)
	copy(buf[:7], h.signature[:])
	buf[7] = h.encryption
	binary.LittleEndian.PutUint32(buf[8:12], h.version)
	binary.LittleEndian.PutUint64(buf[12:20], h.seq)
	binary.LittleEndian.PutUint64(buf[20:28], uint64(h.count))
	binary.LittleEndian.PutUint32(buf[28:32], uint32(h.windowIndex))
	binary.LittleEndian.PutUint32(buf[32:36], uint32(h.blockIndex))
	binary.LittleEndian.PutUint64(buf[36:44], uint64(h.freeblockOff))
	binary.LittleEndian.PutUint64(buf[44:52], h.cacheID)
	return buf, nil
}

// UnmarshalBinary deserializes header from binary data
func (h *header) UnmarshalBinary(data []byte) error {
	copy(h.signature[:], data[:7])
	h.encryption = data[7]
	h.seq = binary.LittleEndian.Uint64(data[12:20])
	h.count = int64(binary.LittleEndian.Uint64(data[20:28]))
	h.windowIndex = int32(binary.LittleEndian.Uint32(data[28:32]))
	h.blockIndex = int32(binary.LittleEndian.Uint32(data[32:36]))
	h.freeblockOff = int64(binary.LittleEndian.Uint64(data[36:44]))
	h.cacheID = binary.LittleEndian.Uint64(data[44:52])

	return nil
}
