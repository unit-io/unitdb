package collection

const (
	// Maximum message payload size allowed from client in bytes (262144 = 256KB).
	DEFAULT_BUFFER_CAP = 700
)

type ByteWriter struct {
	buf []byte
	pos int
}

func NewByteWriter() *ByteWriter {
	return &ByteWriter{
		buf: make([]byte, DEFAULT_BUFFER_CAP),
		pos: 0,
	}
}

// WriteUint16 is a helper function.
func (b *ByteWriter) WriteUint16(n uint16) int {
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

// WriteUint is a helper function.
func (b *ByteWriter) WriteUint(l int, n uint64) (int, bool) {
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

func (b *ByteWriter) Write(p []byte) int {
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

func (b ByteWriter) Bytes() []byte {
	return b.buf[:b.pos]
}

func (b ByteWriter) grow(n int) {
	nbuffer := make([]byte, len(b.buf), len(b.buf)+n)
	copy(nbuffer, b.buf)
	b.buf = nbuffer
}
