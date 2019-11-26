package memdb

import (
	"errors"
)

type table struct {
	tables map[string]*memTable
}

// tableManager represents a memory-mapped table.
type tableManager interface {
	readAt(p []byte, off int64) (int, error)
	writeAt(p []byte, off int64) (int, error)

	name() string
	size() int64
	slice(start int64, end int64) ([]byte, error)
	extend(size uint32) error
	truncate(size int64) error
	close() error
}

// memdb represents a virtual memory table.
type memdb interface {
	newTable(name string, size int) (tableManager, error)
	remove(name string) error
}

// mem is a store backed by memory table.
var mem = &table{tables: map[string]*memTable{}}

func (t *table) newTable(name string, size int64) (tableManager, error) {
	tb := t.tables[name]
	if tb == nil {
		tb = &memTable{}
		tb.tableName = name
		tb.maxSize = size
		t.tables[name] = tb
	} else if !tb.closed {
		return nil, errors.New("table exist")
	} else {
		tb.closed = false
	}
	return tb, nil
}

func (t *table) remove(name string) error {
	if _, ok := t.tables[name]; ok {
		delete(t.tables, name)
		return nil
	}
	return errors.New("table does not exist")
}

type memTable struct {
	tableName string
	buf       []byte
	allocated int64
	maxSize   int64
	offset    int64
	closed    bool
}

func (m *memTable) close() error {
	if m.closed {
		return errors.New("table closed")
	}
	m.closed = true
	return nil
}

func (m *memTable) readAt(p []byte, off int64) (int, error) {
	if m.closed {
		return 0, errors.New("table closed")
	}
	n := len(p)
	if int64(n) > m.allocated-off {
		return 0, errors.New("eof")
	}
	copy(p, m.buf[off:off+int64(n)])
	return n, nil
}

func (m *memTable) writeAt(p []byte, off int64) (int, error) {
	if m.closed {
		return 0, errors.New("table closed")
	}
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

func (m *memTable) extend(size uint32) error {
	return m.truncate(m.allocated + int64(size))
}

func (m *memTable) truncate(size int64) error {
	if m.closed {
		return errors.New("table closed")
	}
	if size > m.allocated {
		diff := int(size - m.allocated)
		m.buf = append(m.buf, make([]byte, diff)...)
	} else {
		m.buf = m.buf[:m.allocated]
	}
	m.allocated = size
	return nil
}

func (m *memTable) name() string {
	return m.tableName
}

func (m *memTable) size() int64 {
	return m.allocated
}

func (m *memTable) slice(start int64, end int64) ([]byte, error) {
	if m.closed {
		return nil, errors.New("table closed")
	}
	return m.buf[start:end], nil
}
