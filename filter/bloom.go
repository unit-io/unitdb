package filter

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"sync"
)

const (
	// MMin is the minimum Filter filter bits count
	MMin = 2
	// KMin is the minimum number of keys
	KMin = 1
	// Uint64Bytes is the number of bytes in type uint64
	Uint64Bytes = 8
)

// Filter is an opaque Filter filter type
type Filter struct {
	lock sync.RWMutex
	bits []uint64
	keys []uint64
	m    uint64 // number of bits the "bits" field should recognize
	n    uint64 // number of inserted elements
}

func newFilter(m, k uint64) *Filter {
	keys := make([]uint64, k)
	binary.Read(rand.Reader, binary.LittleEndian, keys)
	if m < MMin {
		return nil
	}
	return &Filter{
		m:    m,
		n:    0,
		bits: make([]uint64, (m+63)/64),
		keys: keys,
	}
}

func newFilterFromBytes(b []byte, m, k uint64) *Filter {
	if m < MMin {
		return nil
	}
	keys := make([]uint64, k)
	bits := make([]uint64, (m+63)/64)
	buf := bytes.NewBuffer(b)
	binary.Read(buf, binary.LittleEndian, keys)
	binary.Read(buf, binary.LittleEndian, bits)

	return &Filter{
		m:    m,
		n:    0,
		bits: bits,
		keys: keys,
	}
}

// Bytes() returns the bytes backing the Filter filter.
func (b *Filter) Bytes() []byte {
	b.lock.RLock()
	defer b.lock.RUnlock()
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, b.keys)
	binary.Write(buf, binary.LittleEndian, b.bits)
	return buf.Bytes()
}

// Hashable -> hashes
func (b *Filter) hash(h uint64) []uint64 {
	n := len(b.keys)
	hashes := make([]uint64, n)
	for i := 0; i < n; i++ {
		hashes[i] = h ^ b.keys[i]
	}
	return hashes
}

// Add adds `key` to the Filter filter.
func (b *Filter) Add(h uint64) {
	b.lock.Lock()
	defer b.lock.Unlock()

	for _, i := range b.hash(h) {
		i %= b.m
		b.bits[i>>6] |= 1 << uint(i&0x3f)
	}
	b.n++
}

// Test returns whether `key` is found.
func (b *Filter) Test(h uint64) bool {
	b.lock.RLock()
	defer b.lock.RUnlock()

	for _, i := range b.hash(h) {
		i %= b.m
		if (b.bits[i>>6]>>uint(i&0x3f))&1 == 0 {
			return false
		}
	}
	return true
}
