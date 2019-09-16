package tracedb

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/frontnet/tracedb/uid"
	"github.com/kelindar/binary"
)

// Various constant parts of the SSID.
const (
	// system   = uint32(0)
	Contract = uint32(3376684800)
	wildcard = uint32(857445537)

	fixed = 16
)

// Ssid represents a subscription ID which contains a contract and a list of hashes
// for various parts of the topic.
type Ssid []uint32

// NewSsid creates a new SSID.
func (m *Message) setSsid(parts []Part) {
	ssid := make([]uint32, 0, len(parts))
	for _, part := range parts {
		ssid = append(ssid, part.Query)
	}
	m.id = newID(ssid)
}

// AddContract adds contract to the parts.
func (m *Message) setContract(c *Topic) {
	part := Part{
		Wildchars: 0,
		Query:     m.contract,
	}
	if c.Parts[0].Query == wildcard {
		c.Parts[0].Query = m.contract
	} else {
		parts := []Part{part}
		c.Parts = append(parts, c.Parts...)
	}
}

// ID represents a message ID encoded at 128bit and lexigraphically sortable
type ID []byte

// NewID creates a new message identifier for the current time.
func newID(ssid Ssid) ID {
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

// Message represents a message which has to be forwarded or stored.
type Message struct {
	id        ID     // The ID of the message
	Topic     []byte // The topic of the message
	Payload   []byte // The payload of the message
	expiresAt uint64 // The time-to-live of the message
	contract  uint32 // The contract is used to as salt to hash topic parts and also used as prefix in the message Id
}

// New creates a new message structure from the topic and payload.
func NewMessage(topic, payload []byte) *Message {
	return &Message{
		Topic:   topic,
		Payload: payload,
	}
}

// Size returns the byte size of the message.
func (m *Message) Size() int64 {
	return int64(len(m.Payload))
}

// GenPrefix generates a new message identifier only containing the prefix.
func GenPrefix(ssid Ssid, from int64) ID {
	id := make(ID, 8)
	if len(ssid) < 2 {
		return id
	}

	binary.BigEndian.PutUint32(id[0:4], ssid[0]^ssid[1])
	binary.BigEndian.PutUint32(id[4:8], math.MaxUint32-uint32(from-uid.Offset))

	return id
}

// Prefix adds prefix to the Id and also used as contract salt to hash the topic parts
func (m *Message) Prefix(pref uint32) *Message {
	m.contract = pref
	return m
}

// WithTTL adds time to live duration to Message e. Message stored with a TTL would automatically expire
// after the time has elapsed, and will be deleted from db.
func (m *Message) WithTTL(dur time.Duration) *Message {
	m.expiresAt = uint64(time.Now().Add(dur).Unix())
	return m
}

// Time gets the time of the key, adjusted.
func (id ID) Time() int64 {
	return int64(math.MaxUint32-binary.BigEndian.Uint32(id[4:8])) + uid.Offset
}

// EvalPrefix matches the prefix with the cutoff time.
func (id ID) EvalPrefix(ssid Ssid, cutoff int64) bool {
	return (binary.BigEndian.Uint32(id[0:4]) == ssid[0]^ssid[1]) && id.Time() >= cutoff
}
