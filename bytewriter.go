package tracedb

const (
	DEFAULT_BUFFER_CAP = 3000
)

type byteWriter struct {
	buf []byte
	pos int
}

func newByteWriter() *byteWriter {
	return &byteWriter{
		buf: make([]byte, DEFAULT_BUFFER_CAP),
		pos: 0,
	}
}

func (b *byteWriter) writeUint16(n uint16) int {
	currentCap := len(b.buf) - b.pos
	if currentCap < 1 {
		b.grow(2)
	}
	for i := uint(b.pos); i < uint(2); i++ {
		b.buf[i] =
			byte(n >> (i * 8))
	}
	b.pos += 2
	return 2
}

// writeUint is a helper function.
func (b *byteWriter) writeUint(l int, n uint64) (int, bool) {
	currentCap := len(b.buf) - b.pos

	switch l {
	case 8:
		if currentCap < 8 {
			b.grow(8)
		}
		for i := uint(b.pos); i < uint(8); i++ {
			b.buf[i] =
				byte(n >> (i * 8))
		}
		b.pos += 8
	case 4:
		if currentCap < 4 {
			b.grow(4)
		}
		for i := uint(b.pos); i < uint(4); i++ {
			b.buf[i] =
				byte(n >> (i * 8))
		}
		b.pos += 4
	case 2:
		if currentCap < 2 {
			b.grow(2)
		}
		for i := uint(b.pos); i < uint(2); i++ {
			b.buf[i] =
				byte(n >> (i * 8))
		}
		b.pos += 2
	case 1:
		if currentCap < 1 {
			b.grow(1)
		}
		i := uint(b.pos)
		b.buf[i] = byte(n >> (i * 8))
		b.pos += 1
	default:
		return l, false
	}
	return l, true
}

func (b *byteWriter) write(p []byte) int {
	currentCap := len(b.buf) - b.pos
	if currentCap < len(p) {
		b.grow(len(p) - currentCap)
	}
	if len(p) > 0 {
		copy(b.buf[b.pos:], p)
		b.pos += len(p)
	}
	return len(p)
}

func (b byteWriter) grow(n int) {
	nbuffer := make([]byte, len(b.buf), len(b.buf)+n)
	copy(nbuffer, b.buf)
	b.buf = nbuffer
}
