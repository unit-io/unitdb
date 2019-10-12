package uid

import (
	"math"
	"math/rand"
	"time"
)

const (
	Offset = 1555770000
)

var (
	// next is the next identifier. It is time in seconds
	// to avoid collisions of ids between process restarts.
	Next = uint32(
		time.Date(2070, 1, 1, 0, 0, 0, 0, time.UTC).Sub(time.Now()).Seconds(),
	)
)

func NewApoch() uint32 {
	now := uint32(time.Now().Unix() - Offset)
	return math.MaxUint32 - now
}

func NewUnique() uint32 {
	b := make([]byte, 4)
	random := rand.New(rand.NewSource(int64(NewApoch())))
	random.Read(b)
	u := uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
	u |= (0 << 8)
	return u
}
