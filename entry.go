package unitdb

import (
	"strconv"
	"time"
	"unsafe"
)

type topic struct {
	data   []byte
	hash   uint64
	offset int64
	size   uint16
	parsed bool
}

// Entry represents an entry which is stored into DB.
type Entry struct {
	contract uint64
	topic
	seq        uint64
	id         []byte
	val        []byte
	encryption bool
	ID         []byte // The ID of the message
	Topic      []byte // The topic of the message
	Payload    []byte // The payload of the message
	ExpiresAt  uint32 // The time expiry of the message
	Contract   uint32 // The contract is used to as salt to hash topic parts and also used as prefix in the message Id
}

// NewEntry creates a new entry structure from the topic and payload.
func NewEntry(topic, payload []byte) *Entry {
	return &Entry{
		Topic:   topic,
		Payload: payload,
	}
}

// SetID sets entry ID.
func (e *Entry) SetID(id []byte) *Entry {
	e.ID = id
	return e
}

// SetPayload sets payload to put entry into DB.
func (e *Entry) SetPayload(payload []byte) *Entry {
	e.Payload = payload
	return e
}

// SetContract sets contract on entry.
func (e *Entry) SetContract(contract uint32) *Entry {
	e.Contract = contract
	return e
}

// SetTTL sets TTL for message expiry for the entry.
func (e *Entry) SetTTL(ttl []byte) *Entry {
	val, err := strconv.ParseInt(unsafeToString(ttl), 10, 64)
	if err == nil {
		e.ExpiresAt = uint32(time.Now().Add(time.Duration(int(val)) * time.Second).Unix())
		return e
	}
	var duration time.Duration
	duration, _ = time.ParseDuration(unsafeToString(ttl))
	e.ExpiresAt = uint32(time.Now().Add(duration).Unix())
	return e
}

func (e *Entry) reset() {
	e.seq = 0
	e.id = nil
	e.val = nil
	e.ID = nil
	e.Payload = nil
}

type topics []topic

// addUnique adds topic to the set.
func (top *topics) addUnique(value topic) (added bool) {
	for i, v := range *top {
		if v.hash == value.hash {
			(*top)[i].offset = value.offset
			return false
		}
	}
	*top = append(*top, value)
	added = true
	return
}

// unsafeToString is used to convert a slice
// of bytes to a string without incurring overhead.
func unsafeToString(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}
