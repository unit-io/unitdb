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

	fixed = 16
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

// SetContract set contract on ID, setting contract helps to validate prefix.
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

// SetEncryption sets message encryption so while decoding a message it is also decrypted
func (id ID) SetEncryption() {
	binary.BigEndian.PutUint32(id[12:16], binary.BigEndian.Uint32(id[12:16])|(1<<2)) //set encryption bit
}

// IsEncrypted return if the encyrption is set on ID
func (id ID) IsEncrypted() bool {
	return binary.BigEndian.Uint32(id[12:16])&(1<<2) != 0
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

// GenID generates a new message identifier without containing a prefix. Prefix is set later when arrives.
func GenID() ID {
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
