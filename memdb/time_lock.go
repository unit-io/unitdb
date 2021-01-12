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

// _TimeLock mutex to perform time based lock/unlock.
type (
	_Internal struct {
		*sync.RWMutex
	}
	_TimeLock struct {
		locks      []_Internal
		consistent *hash.Consistent
	}
)

// newTimeLock creates mutex to perform time based lock/unlock.
func newTimeLock() _TimeLock {
	timeLock := _TimeLock{
		locks:      make([]_Internal, nLocks),
		consistent: hash.InitConsistent(int(nLocks), int(nLocks)),
	}

	for i := 0; i < nLocks; i++ {
		timeLock.locks[i] = _Internal{new(sync.RWMutex)}
	}

	return timeLock
}

// getTimeLock returns mutex for the provided time ID
func (tl *_TimeLock) getTimeLock(timeID _TimeID) _Internal {
	return tl.locks[tl.consistent.FindBlock(uint64(timeID))]
}
