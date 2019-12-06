package memdb

// import (
// 	"encoding/binary"
// 	"log"
// )

// type internalKey []byte

// func makeInternalKey(dst, ukey []byte, seq uint64, dFlag bool, expiresAt uint32) internalKey {
// 	if seq > keyMaxSeq {
// 		panic("memdb: invavalHash sequence number")
// 	}

// 	var dBit int8
// 	if dFlag {
// 		dBit = 1
// 	}
// 	dst = ensureBuffer(dst, len(ukey)+12)
// 	copy(dst, ukey)
// 	binary.LittleEndian.PutUint64(dst[len(ukey):len(ukey)+8], (seq<<8)|uint64(dBit))
// 	binary.LittleEndian.PutUint32(dst[len(ukey)+8:], expiresAt)
// 	return internalKey(dst)
// }

// func parseInternalKey(ik []byte) (ukey []byte, seq uint64, dFlag bool, expiresAt uint32, err error) {
// 	if len(ik) < keySize {
// 		log.Print("invavalHash internal key length")
// 		return
// 	}
// 	expiresAt = binary.LittleEndian.Uint32(ik[len(ik)-4:])
// 	num := binary.LittleEndian.Uint64(ik[len(ik)-12 : len(ik)-4])
// 	seq, dFlag = uint64(num>>8), num&0xff != 0
// 	ukey = ik[:len(ik)-12]
// 	return
// }

// func ensureBuffer(b []byte, n int) []byte {
// 	if cap(b) < n {
// 		return make([]byte, n)
// 	}
// 	return b[:n]
// }
