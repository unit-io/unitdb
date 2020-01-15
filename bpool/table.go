package bpool

import (
	"errors"
)

func newTable(size int64) (*buffTable, error) {
	tb := &buffTable{}
	tb.maxSize = size
	return tb, nil
}

type buffTable struct {
	buf       []byte
	allocated int64
	maxSize   int64
}

func (m *buffTable) readAt(p []byte, off int64) (int, error) {
	n := len(p)
	if int64(n) > m.allocated-off {
		return 0, errors.New("eof")
	}
	copy(p, m.buf[off:off+int64(n)])
	return n, nil
}

func (m *buffTable) writeAt(p []byte, off int64) (int, error) {
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

func (m *buffTable) extend(size uint32) (int64, error) {
	off := m.allocated
	return off, m.truncate(m.allocated + int64(size))
}

func (m *buffTable) truncate(size int64) error {
	if size > m.allocated {
		diff := int(size - m.allocated)
		m.buf = append(m.buf, make([]byte, diff)...)
	} else {
		m.buf = m.buf[:m.allocated]
	}
	m.allocated = size
	return nil
}

func (m *buffTable) reset() error {
	m.allocated = 0
	m.buf = m.buf[:0]
	return nil
}

func (m *buffTable) size() int64 {
	return m.allocated
}

func (m *buffTable) slice(start int64, end int64) ([]byte, error) {
	return m.buf[start:end], nil
}
