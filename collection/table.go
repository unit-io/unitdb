package collection

import (
	"errors"
)

// type table struct {
// 	tables map[string]*buffTable
// }

// // bufferManager represents a memory-mapped table.
// type bufferManager interface {
// 	readAt(p []byte, off int64) (int, error)
// 	writeAt(p []byte, off int64) (int, error)

// 	name() string
// 	size() int64
// 	slice(start int64, end int64) ([]byte, error)
// 	extend(size uint32) (int64, error)
// 	truncate(size int64) error
// }

// // buffer represents a virtual memory table.
// type buffer interface {
// 	newTable(name string, size int) (bufferManager, error)
// 	remove(name string) error
// }

// // buff is a store backed by memory table.
// var buff = &table{tables: map[string]*buffTable{}}

func newTable(name string, size int64) (*buffTable, error) {
	tb := &buffTable{}
	tb.tableName = name
	tb.maxSize = size
	return tb, nil
}

type buffTable struct {
	tableName string
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

func (m *buffTable) name() string {
	return m.tableName
}

func (m *buffTable) size() int64 {
	return m.allocated
}

func (m *buffTable) slice(start int64, end int64) ([]byte, error) {
	return m.buf[start:end], nil
}
