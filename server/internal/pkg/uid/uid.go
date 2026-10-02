/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uid

import (
	"crypto/rand"
	"encoding/binary"
	"math"
	"time"
)

const (
	Offset = 1555770000
)

var (
	// Next is the last local identifier handed out (see NewLID). It starts
	// at a random value from crypto/rand, rather than at the milliseconds
	// until 2070, so that ids do not follow from the time a process started:
	// nodes of a cluster started close together handed out the same
	// connection ids.
	Next = randomUint32()
)

// randomUint32 returns a random number from crypto/rand.
func randomUint32() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("uid: crypto/rand: " + err.Error())
	}
	return binary.BigEndian.Uint32(b[:])
}

func NewApoch() uint32 {
	now := uint32(TimeNow().Unix() - Offset)
	return math.MaxUint32 - now
}

// NewUnique returns a random number from crypto/rand. It used to come from
// math/rand seeded with the time in seconds, which made it predictable.
func NewUnique() uint32 {
	return randomUint32()
}

// TimeNow returns current wall time in UTC rounded to milliseconds.
func TimeNow() time.Time {
	return time.Now().UTC().Round(time.Millisecond)
}
