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

package unitdb

import (
	"sync"

	"github.com/unit-io/unitdb/hash"
)

// mutex mutex to lock/unlock.
type mutex struct {
	internal   []*sync.RWMutex
	consistent *hash.Consistent
}

// newMutex creates mutex to lock/unlock.
func newMutex() mutex {
	mu := mutex{
		internal:   make([]*sync.RWMutex, nBlocks),
		consistent: hash.InitConsistent(int(nBlocks), int(nBlocks)),
	}

	for i := 0; i < nBlocks; i++ {
		mu.internal[i] = new(sync.RWMutex)
	}

	return mu
}

// getMutex returns mutex under given blockID
func (mu *mutex) getMutex(blockID uint64) *sync.RWMutex {
	return mu.internal[mu.consistent.FindBlock(blockID)]
}
