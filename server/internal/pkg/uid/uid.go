package uid

import (
	"encoding/binary"
	"math"
	"math/rand"
	"time"
)

const (
	Offset = 1555770000
)

var (
	// next is the next identifier. It is time in millsecond
	// to avoid collisions of ids between process restarts.
	Next = uint32(
		time.Date(2070, 1, 1, 0, 0, 0, 0, time.UTC).Sub(TimeNow()),
	)
)

func NewApoch() uint32 {
	now := uint32(TimeNow().Unix() - Offset)
	return math.MaxUint32 - now
}

func NewUnique() uint32 {
	b := make([]byte, 4)
	random := rand.New(rand.NewSource(int64(NewApoch())))
	random.Read(b)
	return binary.BigEndian.Uint32(b)
}

// TimeNow returns current wall time in UTC rounded to milliseconds.
func TimeNow() time.Time {
	return time.Now().UTC().Round(time.Millisecond)
}
