package unitdb

import (
	"encoding/binary"
)

var (
	signature  = [7]byte{'t', 'r', 'a', 'c', 'e', 'd', 'b'}
	headerSize = uint32(64)
)

type header struct {
	signature [7]byte
	version   uint32
	dbInfo
	_ [12]byte
}

// func init() {
// 	// headerSize = align(uint32(binary.Size(header{})))
// 	headerSize = uint32(binary.Size(header{}))
// }

// MarshalBinary serializes header into binary data
func (h header) MarshalBinary() ([]byte, error) {
	buf := make([]byte, headerSize)
	copy(buf[:7], h.signature[:])
	buf[7] = uint8(h.encryption)
	binary.LittleEndian.PutUint32(buf[8:12], h.version)
	binary.LittleEndian.PutUint64(buf[12:20], h.sequence)
	binary.LittleEndian.PutUint64(buf[20:28], uint64(h.count))
	binary.LittleEndian.PutUint32(buf[28:32], uint32(h.windowIdx))
	binary.LittleEndian.PutUint32(buf[32:36], uint32(h.blockIdx))
	binary.LittleEndian.PutUint64(buf[36:44], h.cacheID)
	return buf, nil
}

// UnmarshalBinary de-serializes header from binary data
func (h *header) UnmarshalBinary(data []byte) error {
	copy(h.signature[:], data[:7])
	h.encryption = int8(data[7])
	h.sequence = binary.LittleEndian.Uint64(data[12:20])
	h.count = int64(binary.LittleEndian.Uint64(data[20:28]))
	h.windowIdx = int32(binary.LittleEndian.Uint32(data[28:32]))
	h.blockIdx = int32(binary.LittleEndian.Uint32(data[32:36]))
	h.cacheID = binary.LittleEndian.Uint64(data[36:44])

	return nil
}
