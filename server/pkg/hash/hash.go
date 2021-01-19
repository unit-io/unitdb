package hash

import (
	"encoding/binary"
)

const (
	offset32 uint32 = 0xcc9e2d51
	prime32  uint32 = 0x1b873593

	// Init is what 32 bits hash values should be initialized with.
	Init = offset32
)

// Of returns the hash of bytes. it uses salt to shuffle the slice before calculating hash
func WithSalt(text []byte, salt uint32) uint32 {
	b := shuffleInPlace(text, salt)
	return New(b)
}

// New returns the hash of bytes.
func New(b []byte) uint32 {
	h := Init
	i := 0
	n := (len(b) / 8) * 8

	for i != n {
		h = (h ^ uint32(b[i])) * prime32
		h = (h ^ uint32(b[i+1])) * prime32
		h = (h ^ uint32(b[i+2])) * prime32
		h = (h ^ uint32(b[i+3])) * prime32
		h = (h ^ uint32(b[i+4])) * prime32
		h = (h ^ uint32(b[i+5])) * prime32
		h = (h ^ uint32(b[i+6])) * prime32
		h = (h ^ uint32(b[i+7])) * prime32
		i += 8
	}

	for _, c := range b[i:] {
		h = (h ^ uint32(c)) * prime32
	}

	return h
}

// shuffleInPlace shuffle the slice
func shuffleInPlace(text []byte, contract uint32) []byte {
	if contract == 0 {
		return text
	}
	salt := make([]byte, 4)
	binary.BigEndian.PutUint32(salt[0:4], contract)

	result := duplicateSlice(text)
	for i, v, p := len(result)-1, 0, 0; i > 0; i-- {
		p += int(salt[v])
		j := (int(salt[v]) + v + p) % i
		result[i], result[j] = result[j], result[i]
		v = (v + 1) % len(salt)
	}
	return result
}

// duplicateSlice get a copy of slice
func duplicateSlice(data []byte) []byte {
	result := make([]byte, len(data))
	copy(result, data)
	return result
}
