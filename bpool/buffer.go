package bpool

import "errors"

type (
	buffer struct {
		buf     []byte
		maxSize int64
		size    int64
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
	off := b.size
	return off, b.truncate(b.size + int64(size))
}

func (b *buffer) bytes() ([]byte, error) {
	return b.slice(0, b.size)
}

func (b *buffer) reset() (ok bool) {
	b.size = 0
	b.buf = b.buf[:0]
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
	if size > b.size {
		diff := int(size - b.size)
		b.buf = append(b.buf, make([]byte, diff)...)
	} else {
		b.buf = b.buf[:b.size]
	}
	b.size = size
	return nil
}

func (b *buffer) truncateFront(off int64) error {
	if off > b.size {
		b.buf = nil
		b.size = 0
		return nil
	}
	b.buf = b.buf[off:b.size]
	b.size = b.size - off
	return nil
}

func (b *buffer) slice(start int64, end int64) ([]byte, error) {
	return b.buf[start:end], nil
}
