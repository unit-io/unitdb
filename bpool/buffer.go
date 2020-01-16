package bpool

import "encoding"

type (
	buffer struct {
		*bufTable
		size int64
	}
)

func (b *buffer) append(data []byte) (int64, error) {
	off := b.size
	if _, err := b.writeAt(data, off); err != nil {
		return 0, err
	}
	b.size += int64(len(data))
	return off, nil
}

func (b *buffer) allocate(size uint32) (int64, error) {
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

func (b *buffer) read(off int64, size uint32) ([]byte, error) {
	return b.slice(off, off+int64(size))
}

func (b *buffer) bytes() ([]byte, error) {
	return b.slice(0, b.size)
}

func (b *buffer) reset() (ok bool) {
	b.size = 0
	if err := b.truncate(0); err != nil {
		return false
	}
	return true
}

func (b *buffer) writeMarshalableAt(m encoding.BinaryMarshaler, off int64) error {
	buf, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = b.writeAt(buf, off)
	return err
}

func (b *buffer) readUnmarshalableAt(m encoding.BinaryUnmarshaler, size uint32, off int64) error {
	buf := make([]byte, size)
	if _, err := b.readAt(buf, off); err != nil {
		return err
	}
	return m.UnmarshalBinary(buf)
}
