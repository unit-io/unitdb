package message

import (
	"math"
	"sync/atomic"

	"github.com/kelindar/binary"
	"github.com/unit-io/tracedb/uid"
)

// Various constant parts of the ID.
const (
	// Contract contract is default contract used for topics if client program does not specify Contract in the request
	Contract = uint32(3376684800)
	// Wildcard wildcard is hash for wildcard topic such as '*'
	Wildcard = uint32(857445537)

	fixed = 16
)

// ID represents a message ID encoded at 128bit and lexigraphically sortable
type ID []byte

// AddContract adds a Contract to ID, it is used to validate prefix.
func (id *ID) AddContract(parts []Part) {
	newid := make(ID, fixed+4)
	if len(parts) == 1 {
		binary.BigEndian.PutUint32(newid[0:4], parts[0].Query^Wildcard)
	} else {
		binary.BigEndian.PutUint32(newid[0:4], parts[0].Query^parts[1].Query)
	}
	copy(newid[4:], *id)
	*id = newid
}

// IsEncrypted return if the encyption is set on ID
func (id ID) IsEncrypted() bool {
	num := binary.BigEndian.Uint64(id[12:20])
	return num&0xff != 0
}

// Prefix generates prefix from parts and concatenate contract and first part of the topic
func Prefix(parts []Part) uint64 {
	if len(parts) == 1 {
		return uint64(parts[0].Query)<<32 + uint64(Wildcard)
	}
	return uint64(parts[0].Query)<<32 + uint64(parts[1].Query)
}

// GenPrefix generates a new message identifier only containing the prefix.
func GenPrefix(parts []Part, from int64) ID {
	id := make(ID, 8)
	if len(parts) < 2 {
		return id
	}

	binary.BigEndian.PutUint32(id[0:4], parts[0].Query^parts[1].Query)
	binary.BigEndian.PutUint32(id[4:8], math.MaxUint32-uint32(from-uid.Offset))

	return id
}

// NewID generates a new message identifier without containing a prefix. Prefix is set later when arrives.
func NewID(seq uint64, encrypted bool) ID {
	var eBit int8
	if encrypted {
		eBit = 1
	}
	id := make(ID, fixed)
	binary.BigEndian.PutUint32(id[0:4], uid.NewApoch())
	binary.BigEndian.PutUint32(id[4:8], math.MaxUint32-atomic.AddUint32(&uid.Next, 1)) // Reverse order
	binary.BigEndian.PutUint64(id[8:16], (seq<<8)|uint64(eBit))                        //set encryption flag on id
	return id
}

// Time gets the time of the key, adjusted.
func (id ID) Time() int64 {
	return int64(math.MaxUint32-binary.BigEndian.Uint32(id[4:8])) + uid.Offset
}

// Seq gets the seq for the key.
func (id ID) Seq() uint64 {
	num := binary.BigEndian.Uint64(id[12:20])
	return uint64(num >> 8)
}

// EvalPrefix matches the prefix with the cutoff time.
func (id ID) EvalPrefix(parts []Part, cutoff int64) bool {
	if cutoff > 0 {
		return (binary.BigEndian.Uint32(id[0:4]) == parts[0].Query^parts[1].Query || binary.BigEndian.Uint32(id[0:4]) == parts[0].Query^Wildcard) && id.Time() >= cutoff
	}
	return binary.BigEndian.Uint32(id[0:4]) == parts[0].Query^parts[1].Query || binary.BigEndian.Uint32(id[0:4]) == parts[0].Query^Wildcard
}
