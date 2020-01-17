package message

import (
	"math"
	"sync/atomic"

	"github.com/kelindar/binary"
	"github.com/unit-io/tracedb/uid"
)

// Various constant parts of the ID.
const (
	// MasterContract contract is default contract used for topics if client program does not specify Contract in the request
	MasterContract = uint32(3376684800)
	// Wildcard wildcard is hash for wildcard topic such as '*'
	Wildcard = uint32(857445537)

	fixed = 16
)

// ID represents a message ID encoded at 128bit and lexigraphically sortable
type ID []byte

// AddContract adds a Contract to ID, it is used to validate prefix.
func (id *ID) AddContract(contract uint64) {
	newid := make(ID, fixed+8)
	binary.BigEndian.PutUint64(newid[0:8], contract)
	copy(newid[8:], *id)
	*id = newid
}

// IsEncrypted return if an encyption is set on ID
func (id ID) IsEncrypted() bool {
	num := binary.BigEndian.Uint64(id[16:24])
	return num&0xff != 0
}

// Contract generates contract from parts and concatenate contract and first part of the topic
func Contract(parts []Part) uint64 {
	if len(parts) == 1 {
		return uint64(parts[0].Query)<<32 + uint64(Wildcard)
	}
	return uint64(parts[0].Query)<<32 + uint64(parts[1].Query)
}

// GenPrefix generates a new message identifier only containing the prefix.
func GenPrefix(contract uint64, from int64) ID {
	id := make(ID, 12)

	binary.BigEndian.PutUint64(id[0:8], contract)
	binary.BigEndian.PutUint32(id[8:12], math.MaxUint32-uint32(from-uid.Offset))

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

// Time gets the time of the id, adjusted.
func (id ID) Time() int64 {
	return int64(math.MaxUint32-binary.BigEndian.Uint32(id[8:12])) + uid.Offset
}

// Seq gets the seq for the id.
func (id ID) Seq() uint64 {
	num := binary.BigEndian.Uint64(id[16:24])
	return uint64(num >> 8)
}

// Contract gets the contract for the id.
func (id ID) Contract() uint64 {
	if len(id) < fixed+8 {
		return 0
	}
	return binary.BigEndian.Uint64(id[:8])
}

// EvalPrefix matches the prefix with the cutoff time.
func (id ID) EvalPrefix(contract uint64, cutoff int64) bool {
	if cutoff > 0 {
		return binary.BigEndian.Uint64(id[0:8]) == contract && id.Time() >= cutoff
	}
	return binary.BigEndian.Uint64(id[0:8]) == contract
}
