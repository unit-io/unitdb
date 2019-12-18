package memdb

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

func (h header) MarshalBinary() ([]byte, error) {
	buf := make([]byte, headerSize)
	copy(buf[:8], h.signature[:])
	binary.LittleEndian.PutUint32(buf[8:12], h.version)
	binary.LittleEndian.PutUint32(buf[12:16], h.nBlocks)
	binary.LittleEndian.PutUint32(buf[16:20], h.blockIndex)
	return buf, nil
}

func (h *header) UnmarshalBinary(data []byte) error {
	copy(h.signature[:], data[:8])
	h.version = binary.LittleEndian.Uint32(data[8:12])
	h.nBlocks = binary.LittleEndian.Uint32(data[12:16])
	h.blockIndex = binary.LittleEndian.Uint32(data[16:20])
	return nil
}
