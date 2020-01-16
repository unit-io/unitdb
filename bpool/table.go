package bpool

import (
	"errors"
)

func newTable(size int64) (*bufTable, error) {
	tb := &bufTable{}
	tb.maxSize = size
	return tb, nil
}

type bufTable struct {
	buf       []byte
	allocated int64
	maxSize   int64
}

func (m *bufTable) readAt(p []byte, off int64) (int, error) {
	n := len(p)
	if int64(n) > m.allocated-off {
		return 0, errors.New("eof")
	}
	copy(p, m.buf[off:off+int64(n)])
	return n, nil
}

func (m *bufTable) writeAt(p []byte, off int64) (int, error) {
	n := len(p)
	if off == m.allocated {
		m.buf = append(m.buf, p...)
		m.allocated += int64(n)
	} else if off+int64(n) > m.allocated {
		panic("trying to write past EOF - undefined behavior")
	} else {
		copy(m.buf[off:off+int64(n)], p)
	}
	return n, nil
}

func (m *bufTable) extend(size uint32) (int64, error) {
	off := m.allocated
	return off, m.truncate(m.allocated + int64(size))
}

func (m *bufTable) truncate(size int64) error {
	if size > m.allocated {
		diff := int(size - m.allocated)
		m.buf = append(m.buf, make([]byte, diff)...)
	} else {
		m.buf = m.buf[:m.allocated]
	}
	m.allocated = size
	return nil
}

func (m *bufTable) reset() error {
	m.allocated = 0
	m.buf = m.buf[:0]
	return nil
}

func (m *bufTable) size() int64 {
	return m.allocated
}

func (m *bufTable) slice(start int64, end int64) ([]byte, error) {
	return m.buf[start:end], nil
}
