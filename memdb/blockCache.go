package memdb

type blockCache struct {
	tableManager
	size int64
}

func (b *blockCache) append(data []byte) (int64, error) {
	off := b.size
	if _, err := b.writeAt(data, off); err != nil {
		return 0, err
	}
	b.size += int64(len(data))
	return off, nil
}

// func (b *blockCache) allocate(size uint32) (int64, error) {
// 	return b.extend(size)
// }

func (b *blockCache) allocate(size uint32) (int64, error) {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	off := b.size
	if err := b.truncate(off + int64(size)); err != nil {
		return 0, err
	}
	b.size += int64(size)
	return off, nil
}

func (b *blockCache) readRaw(off int64, size uint32) ([]byte, error) {
	return b.slice(off, off+int64(size))
}
