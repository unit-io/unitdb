package message

import (
	"math"
	"sync/atomic"

	"github.com/saffat-in/tracedb/uid"
	"github.com/kelindar/binary"
)

// Various constant parts of the SSID.
const (
	// system   = uint32(0)
	Contract = uint32(3376684800)
	wildcard = uint32(857445537)

	fixed              = 16
	DEFAULT_BUFFER_CAP = 3000
)

// ID represents a message ID encoded at 128bit and lexigraphically sortable
type ID []byte

// NewID creates a new message identifier for the current time.
func NewID(ssid Ssid) ID {
	id := make(ID, len(ssid)*4+fixed)

	binary.BigEndian.PutUint32(id[0:4], ssid[0]^ssid[1])
	binary.BigEndian.PutUint32(id[4:8], uid.NewApoch())
	binary.BigEndian.PutUint32(id[8:12], math.MaxUint32-atomic.AddUint32(&uid.Next, 1)) // Reverse order
	binary.BigEndian.PutUint32(id[12:16], uid.NewUnique())
	for i, v := range ssid {
		binary.BigEndian.PutUint32(id[fixed+i*4:fixed+4+i*4], v)
	}

	return id
}

func (id ID) SetEncryption() {
	u := binary.BigEndian.Uint32(id[12:16])
	u |= (1 << 8)
	binary.BigEndian.PutUint32(id[12:16], u)
}

func (id ID) IsEncrypted() bool {
	u := binary.BigEndian.Uint32(id[12:16])
	return u&0xff != 0
}

// Entry represents a entry which has to be forwarded or stored.
type Entry struct {
	Topic     []byte `json:"chan,omitempty"` // The topic of the message
	Payload   []byte `json:"data,omitempty"` // The payload of the message
	ExpiresAt uint32 // The time expiry of the message
	Contract  uint32 // The contract is used to as salt to hash topic parts and also used as prefix in the message Id
}

// NewEntry creates a new entry structure from the topic and payload.
func NewEntry(topic, payload []byte) *Entry {
	return &Entry{
		Topic:   topic,
		Payload: payload,
	}
}

func (e *Entry) Marshal() ([]byte, error) {
	b := newByteWriter()
	b.WriteUint16(uint16(len(e.Topic)))
	b.Write(e.Topic)
	b.Write(e.Payload)
	return b.buf[:b.pos], nil
}

func (e *Entry) Unmarshal(data []byte) error {
	l := binary.LittleEndian.Uint16(data[:2])
	e.Topic = data[2 : l+2]
	e.Payload = data[l+2:]
	return nil
}

// genPrefix generates a new message identifier only containing the prefix.
func GenPrefix(ssid Ssid, from int64) ID {
	id := make(ID, 8)
	if len(ssid) < 2 {
		return id
	}

	binary.BigEndian.PutUint32(id[0:4], ssid[0]^ssid[1])
	binary.BigEndian.PutUint32(id[4:8], math.MaxUint32-uint32(from-uid.Offset))

	return id
}

// Time gets the time of the key, adjusted.
func (id ID) Time() int64 {
	return int64(math.MaxUint32-binary.BigEndian.Uint32(id[4:8])) + uid.Offset
}

// EvalPrefix matches the prefix with the cutoff time.
func (id ID) EvalPrefix(ssid Ssid, cutoff int64) bool {
	return (binary.BigEndian.Uint32(id[0:4]) == ssid[0]^ssid[1]) && id.Time() >= cutoff
}

func (b byteWriter) grow(n int) {
	nbuffer := make([]byte, len(b.buf), len(b.buf)+n)
	copy(nbuffer, b.buf)
	b.buf = nbuffer
}

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

func (b *byteWriter) WriteUint16(n uint16) int {
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

func (b *byteWriter) Write(p []byte) int {
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
