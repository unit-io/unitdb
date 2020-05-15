package memdb

import "errors"

type dataTable struct {
	buf    []byte
	size   int64
	closed bool
}

// func (t *dataTable) append(data []byte) (int64, error) {
// 	off := t.size
// 	if _, err := t.writeAt(data, off); err != nil {
// 		return 0, err
// 	}
// 	return off, nil
// }

func (t *dataTable) allocate(size uint32) (int64, error) {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	off := t.size
	return off, t.truncate(t.size + int64(size))
}

func (t *dataTable) shrink(off int64) error {
	if t.size == 0 {
		panic("unable to shrink table of size zero bytes")
	}
	if err := t.truncateFront(off); err != nil {
		return err
	}
	return nil
}

func (t *dataTable) readRaw(off int64, size uint32) ([]byte, error) {
	return t.slice(off, off+int64(size))
}

func (t *dataTable) close() error {
	if t.closed {
		return errors.New("table closed")
	}
	t.closed = true
	return nil
}

// func (t *dataTable) readAt(p []byte, off int64) (int, error) {
// 	if t.closed {
// 		return 0, errors.New("table closed")
// 	}
// 	n := len(p)
// 	if int64(n) > t.size-off {
// 		return 0, errors.New("eof")
// 	}
// 	copy(p, t.buf[off:off+int64(n)])
// 	return n, nil
// }

func (t *dataTable) writeAt(p []byte, off int64) (int, error) {
	if t.closed {
		return 0, errors.New("table closed")
	}
	n := len(p)
	if off == t.size {
		t.buf = append(t.buf, p...)
		t.size += int64(n)
	} else if off+int64(n) > t.size {
		panic("trying to write past EOF - undefined behavior")
	} else {
		copy(t.buf[off:off+int64(n)], p)
	}
	return n, nil
}

// func (t *dataTable) extend(size uint32) (int64, error) {
// 	off := t.size
// 	return off, t.truncate(t.size + int64(size))
// }

func (t *dataTable) truncate(size int64) error {
	if t.closed {
		return errors.New("table closed")
	}
	if size > t.size {
		diff := int(size - t.size)
		t.buf = append(t.buf, make([]byte, diff)...)
	} else {
		t.buf = t.buf[:t.size]
	}
	t.size = size
	return nil
}

func (t *dataTable) truncateFront(off int64) error {
	if t.closed {
		return errors.New("table closed")
	}
	if off > t.size {
		t.buf = nil
		t.size = 0
		return nil
	}
	t.buf = t.buf[off:t.size]
	t.size = t.size - off
	return nil
}

func (t *dataTable) slice(start int64, end int64) ([]byte, error) {
	if t.closed {
		return nil, errors.New("table closed")
	}
	return t.buf[start:end], nil
}
