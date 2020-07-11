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

type (
	internalEntry struct {
		contract uint64
		topic
		seq        uint64
		id         []byte
		val        []byte
		encryption bool
	}
	Entry struct {
		internalEntry
		ID        []byte // The ID of the message
		Topic     []byte // The topic of the message
		Payload   []byte // The payload of the message
		ExpiresAt uint32 // The time expiry of the message
		Contract  uint32 // The contract is used to as salt to hash topic parts and also used as prefix in the message Id
	}
)

// NewEntry creates a new entry structure from the topic.
func NewEntry(topic []byte) *Entry {
	return &Entry{
		Topic: topic,
	}
}

// WithID sets entry ID
func (e *Entry) WithID(id []byte) *Entry {
	e.ID = id
	return e
}

// WithPayload sets payload to put entry into DB.
func (e *Entry) WithPayload(payload []byte) *Entry {
	e.Payload = payload
	return e
}

// WithContract sets contract on entry.
func (e *Entry) WithContract(contract uint32) *Entry {
	e.Contract = contract
	return e
}

// WithTTL sets TTL for message expiry for the entry.
func (e *Entry) WithTTL(ttl []byte) *Entry {
	val, err := strconv.ParseInt(unsafeToString(ttl), 10, 64)
	if err == nil {
		e.ExpiresAt = uint32(time.Now().Add(time.Duration(int(val)) * time.Second).Unix())
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

// unsafeToString is used to convert a slice
// of bytes to a string without incurring overhead.
func unsafeToString(bs []byte) string {
	return *(*string)(unsafe.Pointer(&bs))
}
