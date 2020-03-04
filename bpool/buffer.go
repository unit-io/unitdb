package bpool

import (
	"errors"
	"sync/atomic"
)

type (
	buffer struct {
		buf  []byte
		size int64
	}
)

func (b *buffer) append(data []byte) (int64, error) {
	off := b.size
	if _, err := b.writeAt(data, off); err != nil {
		return 0, err
	}
	return off, nil
}

func (b *buffer) allocate(size uint32) (int64, error) {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	off := atomic.LoadInt64(&b.size)
	return off, b.truncate(off + int64(size))
}

func (b *buffer) bytes() ([]byte, error) {
	return b.slice(0, b.size)
}

func (b *buffer) reset() (ok bool) {
	atomic.StoreInt64(&b.size, 0)
	// b.buf = b.buf[:0]
	b.buf = nil
	return true
}

func (b *buffer) readAt(p []byte, off int64) (int, error) {
	n := len(p)
	if int64(n) > b.size-off {
		return 0, errors.New("eof")
	}
	copy(p, b.buf[off:off+int64(n)])
	return n, nil
}

func (b *buffer) writeAt(p []byte, off int64) (int, error) {
	n := len(p)
	if off == b.size {
		b.buf = append(b.buf, p...)
		b.size += int64(n)
	} else if off+int64(n) > b.size {
		panic("trying to write past EOF - undefined behavior")
	} else {
		copy(b.buf[off:off+int64(n)], p)
	}
	return n, nil
}

func (b *buffer) truncate(size int64) error {
	if size > b.Size() {
		diff := int(size - b.Size())
		b.buf = append(b.buf, make([]byte, diff)...)
	} else {
		b.buf = b.buf[:b.Size()]
	}
	atomic.StoreInt64(&b.size, size)
	return nil
}

func (b *buffer) slice(start int64, end int64) ([]byte, error) {
	return b.buf[start:end], nil
}

// Size returns buffer size
func (b *buffer) Size() int64 {
	return atomic.LoadInt64(&b.size)
}

// incSize increases buffer size to allocate more buffer
func (b *buffer) incSize(size int64) int64 {
	return atomic.AddInt64(&b.size, size)
}
