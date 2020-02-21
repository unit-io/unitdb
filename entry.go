package tracedb

// Entry represents a entry which has to be forwarded or stored.
type Entry struct {
	contract    uint64
	seq         uint64
	id          []byte
	topicHash   uint64
	topicOffset int64
	topic       []byte
	val         []byte
	encryption  bool
	ID          []byte `json:"id,omitempty"`   // The ID of the message
	Topic       []byte `json:"chan,omitempty"` // The topic of the message
	Payload     []byte `json:"data,omitempty"` // The payload of the message
	ExpiresAt   uint32 // The time expiry of the message
	Contract    uint32 // The contract is used to as salt to hash topic parts and also used as prefix in the message Id
}

// NewEntry creates a new entry structure from the topic and payload.
func NewEntry(topic, payload []byte) *Entry {
	return &Entry{
		Topic:   topic,
		Payload: payload,
	}
}
