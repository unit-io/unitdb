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
