package tracedb

import (
	"encoding/binary"

	"github.com/unit-io/tracedb/collection"
)

// Entry represents a entry which has to be forwarded or stored.
type Entry struct {
	ID        []byte `json:"id,omitempty"`   // The ID of the message
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

// Marshal serializes message topic and payload into binary data
func (e *Entry) Marshal() ([]byte, error) {
	b := collection.NewByteWriter()
	b.WriteUint16(uint16(len(e.Topic)))
	b.Write(e.Topic)
	buf := bufPool.Get()
	defer bufPool.Put(buf)
	buf.Write(b.Bytes())
	buf.Write(e.Payload)
	return buf.Bytes(), nil
}

// Unmarshal dserializes message topic and payload from binary data
func (e *Entry) Unmarshal(data []byte) error {
	l := binary.LittleEndian.Uint16(data[:2])
	e.Topic = data[2 : l+2]
	e.Payload = data[l+2:]
	return nil
}
