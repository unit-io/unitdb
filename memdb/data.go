package memdb

type dataTable struct {
	tableManager
	size int64
}

func (t *dataTable) append(data []byte) (int64, error) {
	off := t.size
	if _, err := t.writeAt(data, off); err != nil {
		return 0, err
	}
	t.size += int64(len(data))
	return off, nil
}

func (t *dataTable) allocate(size uint32) (int64, error) {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	off := t.size
	if err := t.truncate(off + int64(size)); err != nil {
		return 0, err
	}
	t.size += int64(size)
	return off, nil
}

func (t *dataTable) shrink(off int64) error {
	if t.size == 0 {
		panic("unable to shrink table of size zero bytes")
	}
	if err := t.truncateFront(off); err != nil {
		return err
	}
	t.size -= off
	return nil
}

func (t *dataTable) readRaw(off int64, size uint32) ([]byte, error) {
	return t.slice(off, off+int64(size))
}
