package uid

import (
	"math"
	"math/rand"
	"sync/atomic"
	"time"
)

const (
	// Offset is used to create new apoch from current time
	Offset = 1555770000
)

var (
	// Next is the next identifier. It is time in seconds
	// to avoid collisions of ids between process restarts.
	Next = uint32(
		time.Date(2070, 1, 1, 0, 0, 0, 0, time.UTC).Sub(time.Now()).Seconds(),
	)
)

// LID represents a process-wide unique ID.
type LID uint64

// NewApoch creats an appoch to generate uniue id
func NewApoch() uint32 {
	now := uint32(time.Now().Unix() - Offset)
	return math.MaxUint32 - now
}

// NewUnique return unique value to use generating unique id
func NewUnique() uint32 {
	b := make([]byte, 4)
	random := rand.New(rand.NewSource(int64(NewApoch())))
	random.Read(b)
	u := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(0)
	return u
}

// NewID generates a new, process-wide unique ID.
func NewLID() LID {
	return LID(atomic.AddUint32(&Next, 1))
}
