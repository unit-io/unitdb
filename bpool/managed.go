package bpool

import "encoding"

type (
	header struct {
		size       int64 // free size at the start
		currOffset int64 // current offset for write
		currSize   int64 // size available for write
	}

	managedBuffer struct {
		*bufTable
		m map[uint64][]int64 // map of key->offset
		header
		size       int64
		targetSize int64
	}
)

func (b *managedBuffer) append(data []byte) (int64, error) {
	off := b.size
	if _, err := b.writeAt(data, off); err != nil {
		return 0, err
	}
	b.size += int64(len(data))
	return off, nil
}

func (b *managedBuffer) allocate(size uint32) (int64, error) {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	// do not allocate freeblocks until target size has reached of the log to avoid fragmentation
	if b.targetSize > (b.size+int64(size)) || (b.targetSize < (b.size+int64(size)) && b.currSize < int64(size)) {
		off := b.size
		if err := b.truncate(off + int64(size)); err != nil {
			return 0, err
		}
		b.size += int64(size)
		return off, nil
	}
	off := b.currOffset
	b.currSize -= int64(size)
	b.currOffset += int64(size)
	return off, nil
}

func (b *managedBuffer) read(off int64, size uint32) ([]byte, error) {
	return b.slice(off, off+int64(size))
}

func (b *managedBuffer) bytes() ([]byte, error) {
	return b.slice(0, b.size)
}

func (b *managedBuffer) reset() (ok bool) {
	b.size = 0
	if err := b.truncate(0); err != nil {
		return false
	}
	return true
}

func (b *managedBuffer) writeMarshalableAt(m encoding.BinaryMarshaler, off int64) error {
	buf, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = b.writeAt(buf, off)
	return err
}

func (b *managedBuffer) readUnmarshalableAt(m encoding.BinaryUnmarshaler, size uint32, off int64) error {
	buf := make([]byte, size)
	if _, err := b.readAt(buf, off); err != nil {
		return err
	}
	return m.UnmarshalBinary(buf)
}
