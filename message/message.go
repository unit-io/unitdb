package message

import (
	"math"
	"sync/atomic"

	"github.com/kelindar/binary"
	"github.com/saffat-in/tracedb/uid"
)

// Various constant parts of the ID.
const (
	Contract = uint32(3376684800)
	Wildcard = uint32(857445537)

	fixed              = 16
	DEFAULT_BUFFER_CAP = 3000
)

// ID represents a message ID encoded at 128bit and lexigraphically sortable
type ID []byte

// NewID creates a new message identifier for the current time.
func NewID(parts []Part) ID {
	id := make(ID, fixed)
	if len(parts) > 1 {
		binary.BigEndian.PutUint32(id[0:4], parts[0].Query^parts[1].Query)
	} else {
		binary.BigEndian.PutUint32(id[0:4], parts[0].Query^Wildcard)
	}
	binary.BigEndian.PutUint32(id[4:8], uid.NewApoch())
	binary.BigEndian.PutUint32(id[8:12], math.MaxUint32-atomic.AddUint32(&uid.Next, 1)) // Reverse order
	binary.BigEndian.PutUint32(id[12:16], uid.NewUnique()|(0<<2))                       // clear encryption bit
	return id
}

// NewID creates a new message identifier for the current time.
func (id *ID) SetContract(parts []Part) {
	newid := make(ID, fixed)
	if len(parts) == 1 {
		binary.BigEndian.PutUint32(newid[0:4], parts[0].Query^Wildcard)
	} else {
		binary.BigEndian.PutUint32(newid[0:4], parts[0].Query^parts[1].Query)
	}
	copy(newid[4:], *id)
	*id = newid
}

func (id ID) SetEncryption() {
	binary.BigEndian.PutUint32(id[12:16], binary.BigEndian.Uint32(id[12:16])|(1<<2)) //set encryption bit
}

func (id ID) IsEncrypted() bool {
	return binary.BigEndian.Uint32(id[12:16])&(1<<2) != 0
}

// Entry represents a entry which has to be forwarded or stored.
type Entry struct {
	ID        ID     `json:"id,omitempty"`   // The ID of the message
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
	b.writeUint16(uint16(len(e.Topic)))
	b.write(e.Topic)
	b.write(e.Payload)
	return b.buf[:b.pos], nil
}

func (e *Entry) Unmarshal(data []byte) error {
	l := binary.LittleEndian.Uint16(data[:2])
	e.Topic = data[2 : l+2]
	e.Payload = data[l+2:]
	return nil
}

// genPrefix generates a new message identifier only containing the prefix.
func GenPrefix(parts []Part, from int64) ID {
	id := make(ID, 8)
	if len(parts) < 2 {
		return id
	}

	binary.BigEndian.PutUint32(id[0:4], parts[0].Query^parts[1].Query)
	binary.BigEndian.PutUint32(id[4:8], math.MaxUint32-uint32(from-uid.Offset))

	return id
}

// genPrefix generates a new message identifier only containing the prefix.
func GenID(e *Entry) ID {
	id := make(ID, 12)
	u := (uid.NewUnique() << 4) | 0 // set first bit zero as it is used set encryption flag on id
	binary.BigEndian.PutUint32(id[0:4], uid.NewApoch())
	binary.BigEndian.PutUint32(id[4:8], math.MaxUint32-atomic.AddUint32(&uid.Next, 1)) // Reverse order
	binary.BigEndian.PutUint32(id[8:12], u)
	return id
}

// Time gets the time of the key, adjusted.
func (id ID) Time() int64 {
	return int64(math.MaxUint32-binary.BigEndian.Uint32(id[4:8])) + uid.Offset
}

// EvalPrefix matches the prefix with the cutoff time.
func (id ID) EvalPrefix(parts []Part, cutoff int64) bool {
	if cutoff > 0 {
		return (binary.BigEndian.Uint32(id[0:4]) == parts[0].Query^parts[1].Query || binary.BigEndian.Uint32(id[0:4]) == parts[0].Query^Wildcard) && id.Time() >= cutoff
	} else {
		return binary.BigEndian.Uint32(id[0:4]) == parts[0].Query^parts[1].Query || binary.BigEndian.Uint32(id[0:4]) == parts[0].Query^Wildcard
	}
}

func (b byteWriter) grow(n int) {
	nbuffer := make([]byte, len(b.buf), len(b.buf)+n)
	copy(nbuffer, b.buf)
	b.buf = nbuffer
}
