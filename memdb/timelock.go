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

package memdb

import (
	"sync"

	"github.com/unit-io/unitdb/hash"
)

// _TimeLock mutex to lock/unlock.
type _TimeLock struct {
	internal   []*sync.RWMutex
	consistent *hash.Consistent
}

// newTimeLock creates mutex to lock/unlock.
func newTimeLock() _TimeLock {
	mu := _TimeLock{
		internal:   make([]*sync.RWMutex, nBlocks),
		consistent: hash.InitConsistent(int(nBlocks), int(nBlocks)),
	}

	for i := 0; i < nBlocks; i++ {
		mu.internal[i] = new(sync.RWMutex)
	}

	return mu
}

// getTimeLock returns mutex for the provided timeID
func (mu *_TimeLock) getTimeLock(timeID _TimeID) *sync.RWMutex {
	return mu.internal[mu.consistent.FindBlock(uint64(timeID))]
}
