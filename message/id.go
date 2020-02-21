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

	None      = uint32(0)      // ID has no flags.
	Encrypted = uint32(1 << 0) // ID has encryption set.

	fixed = 16
)

// Contract generates contract from parts and concatenate contract and first part of the topic
func Contract(parts []Part) uint64 {
	if len(parts) == 1 {
		return uint64(parts[0].Query)<<32 + uint64(Wildcard)
	}
	return uint64(parts[0].Query)<<32 + uint64(parts[1].Query)
}

// ID represents a message ID encoded at 128bit and lexigraphically sortable
type ID []byte

// AddContract adds a Contract to ID, it is used to validate prefix.
func (id *ID) AddContract(contract uint64) {
	newid := make(ID, fixed+8)
	copy(newid[:fixed], *id)
	binary.LittleEndian.PutUint64(newid[fixed:fixed+8], contract)
	*id = newid
}

// Contract gets the contract for the id.
func (id ID) Contract() uint64 {
	if len(id) < fixed+8 {
		return 0
	}
	return binary.LittleEndian.Uint64(id[fixed:])
}

// NewID generates a new message identifier without containing a prefix. Prefix is set later when arrives.
func NewID(seq uint64, encrypted bool) ID {
	var eBit int8
	if encrypted {
		eBit = 1
	}
	id := make(ID, fixed)
	binary.LittleEndian.PutUint32(id[0:4], uid.NewApoch())
	binary.LittleEndian.PutUint32(id[4:8], math.MaxUint32-atomic.AddUint32(&uid.Next, 1)) // Reverse order
	binary.LittleEndian.PutUint64(id[8:16], (seq<<8)|uint64(eBit))                        //set encryption flag on id
	return id
}

// SetEncryption sets an encyption on ID
func (id ID) SetEncryption() {
	eBit := 1
	id[16] = byte(eBit)
}

// IsEncrypted return if an encyption is set on ID
func (id ID) IsEncrypted() bool {
	num := binary.LittleEndian.Uint64(id[8:16])
	return num&0xff != 0
}

// Seq gets the seq for the id.
func (id ID) Seq() uint64 {
	num := binary.LittleEndian.Uint64(id[8:16])
	return uint64(num >> 8)
}

// Time gets the time of the id, adjusted.
func (id ID) Time() int64 {
	return int64(math.MaxUint32-binary.LittleEndian.Uint32(id[0:4])) + uid.Offset
}

// EvalPrefix matches the prefix with the cutoff time.
func (id ID) EvalPrefix(contract uint64, cutoff int64) bool {
	if cutoff > 0 {
		return binary.LittleEndian.Uint64(id[fixed:]) == contract && id.Time() >= cutoff
	}
	return binary.LittleEndian.Uint64(id[fixed:]) == contract
}
