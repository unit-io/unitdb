package collection

import "encoding"

type (
	header struct {
		size       int64 // free size at the start
		currOffset int64 // current offset for write
		currSize   int64 // size available for write
	}

	data struct {
		*buffTable
		header
		size       int64
		targetSize int64
	}
)

func (d *data) append(data []byte) (int64, error) {
	off := d.size
	if _, err := d.writeAt(data, off); err != nil {
		return 0, err
	}
	d.size += int64(len(data))
	return off, nil
}

func (d *data) allocate(size uint32) (int64, error) {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	// do not allocate freeblocks until target size has reached of the log to avoid fragmentation
	if d.targetSize > (d.size+int64(size)) || (d.targetSize < (d.size+int64(size)) && d.currSize < int64(size)) {
		off := d.size
		if err := d.truncate(off + int64(size)); err != nil {
			return 0, err
		}
		d.size += int64(size)
		return off, nil
	}
	off := d.currOffset
	d.currSize -= int64(size)
	d.currOffset += int64(size)
	return off, nil
}

func (d *data) read(off int64, size uint32) ([]byte, error) {
	return d.slice(off, off+int64(size))
}

func (d *data) bytes() ([]byte, error) {
	return d.slice(0, d.size)
}

func (d *data) writeMarshalableAt(m encoding.BinaryMarshaler, off int64) error {
	buf, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = d.writeAt(buf, off)
	return err
}

func (d *data) readUnmarshalableAt(m encoding.BinaryUnmarshaler, size uint32, off int64) error {
	buf := make([]byte, size)
	if _, err := d.readAt(buf, off); err != nil {
		return err
	}
	return m.UnmarshalBinary(buf)
}
