package tracedb

import (
	"encoding/binary"
)

var (
	signature  = [8]byte{'t', 'r', 'a', 'c', 'e', 'd', 'b', '\xfd'}
	headerSize uint32
)

type header struct {
	signature [8]byte
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
	copy(buf[:8], h.signature[:])
	binary.LittleEndian.PutUint32(buf[8:12], h.version)
	buf[12] = h.encryption
	binary.LittleEndian.PutUint64(buf[13:21], uint64(h.seq))
	binary.LittleEndian.PutUint32(buf[21:25], h.count)
	binary.LittleEndian.PutUint32(buf[25:29], h.nBlocks)
	binary.LittleEndian.PutUint32(buf[29:33], h.blockIndex)
	binary.LittleEndian.PutUint64(buf[33:41], uint64(h.freeblockOff))
	binary.LittleEndian.PutUint64(buf[41:49], h.cacheID)
	binary.LittleEndian.PutUint32(buf[49:53], h.hashSeed)
	return buf, nil
}

// UnmarshalBinary deserializes header from binary data
func (h *header) UnmarshalBinary(data []byte) error {
	copy(h.signature[:], data[:8])
	h.version = binary.LittleEndian.Uint32(data[8:12])
	h.encryption = data[12]
	h.seq = binary.LittleEndian.Uint64(data[13:21])
	h.count = binary.LittleEndian.Uint32(data[21:25])
	h.nBlocks = binary.LittleEndian.Uint32(data[25:29])
	h.blockIndex = binary.LittleEndian.Uint32(data[29:33])
	h.freeblockOff = int64(binary.LittleEndian.Uint64(data[33:41]))
	h.cacheID = binary.LittleEndian.Uint64(data[41:49])
	h.hashSeed = binary.LittleEndian.Uint32(data[49:53])
	return nil
}
