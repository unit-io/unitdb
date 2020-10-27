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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/unitdb/hash"
)

type (
	_ExpiryWindowEntries []timeWindowEntry

	timeWindowEntry interface {
		expiryTime() uint32
	}

	_ExpiryWindow struct {
		windows map[int64]_ExpiryWindowEntries // map[expiryHash]windowEntries.

		mu sync.RWMutex // Read Write mutex, guards access to internal collection.
	}

	_ExpiryWindows struct {
		sync.RWMutex
		expiry     []*_ExpiryWindow
		consistent *hash.Consistent
	}

	_ExpiryWindowBucket struct {
		sync.RWMutex
		expiryWindows *_ExpiryWindows

		expDurationType     time.Duration
		maxExpDurations     int
		backgroundKeyExpiry bool
		earliestExpiryHash  int64
	}
)

// newExpiryWindows creates a new concurrent expiryWindows.
func newExpiryWindows() *_ExpiryWindows {
	w := &_ExpiryWindows{
		expiry:     make([]*_ExpiryWindow, nBlocks),
		consistent: hash.InitConsistent(nBlocks, nBlocks),
	}

	for i := 0; i < nBlocks; i++ {
		w.expiry[i] = &_ExpiryWindow{windows: make(map[int64]_ExpiryWindowEntries)}
	}

	return w
}

// getWindows returns shard under given key.
func (w *_ExpiryWindows) getWindows(key uint64) *_ExpiryWindow {
	w.RLock()
	defer w.RUnlock()
	return w.expiry[w.consistent.FindBlock(key)]
}

func newExpiryWindowBucket(bgKeyExp bool, expDurType time.Duration, maxExpDur int) *_ExpiryWindowBucket {
	ex := &_ExpiryWindowBucket{backgroundKeyExpiry: bgKeyExp, expDurationType: expDurType, maxExpDurations: maxExpDur}
	ex.expiryWindows = newExpiryWindows()
	return ex
}

func (wb *_ExpiryWindowBucket) getExpiredEntries(maxResults int) []timeWindowEntry {
	if !wb.backgroundKeyExpiry {
		return nil
	}
	var expiredEntries []timeWindowEntry
	startTime := uint32(time.Now().Unix())

	if atomic.LoadInt64(&wb.earliestExpiryHash) > int64(startTime) {
		return expiredEntries
	}

	for i := 0; i < wb.maxExpDurations; i++ {
		// get windows shard.
		ws := wb.expiryWindows.expiry[i]
		ws.mu.Lock()
		defer ws.mu.Unlock()
		windowTimes := make([]int64, 0, len(ws.windows))
		for windowTime := range ws.windows {
			windowTimes = append(windowTimes, windowTime)
		}
		sort.Slice(windowTimes[:], func(i, j int) bool { return windowTimes[i] < windowTimes[j] })
		for i := 0; i < len(windowTimes); i++ {
			if windowTimes[i] > int64(startTime) || len(expiredEntries) > maxResults {
				break
			}
			windowEntries := ws.windows[windowTimes[i]]
			expiredEntriesCount := 0
			for i := range windowEntries {
				entry := windowEntries[i]
				if entry.expiryTime() < startTime {
					expiredEntries = append(expiredEntries, entry)
					expiredEntriesCount++
				}
			}
			if expiredEntriesCount == len(windowEntries) {
				delete(ws.windows, windowTimes[i])
			}
		}
	}
	atomic.StoreInt64(&wb.earliestExpiryHash, 0)
	return expiredEntries
}

// addExpiry adds expiry for entries expiring. Entries expires in future are not added to expiry window.
func (wb *_ExpiryWindowBucket) addExpiry(e timeWindowEntry) error {
	if !wb.backgroundKeyExpiry {
		return nil
	}
	timeExpiry := int64(time.Unix(int64(e.expiryTime()), 0).Truncate(wb.expDurationType).Add(1 * wb.expDurationType).Unix())
	atomic.CompareAndSwapInt64(&wb.earliestExpiryHash, 0, timeExpiry)

	// get windows shard.
	ws := wb.expiryWindows.getWindows(uint64(e.expiryTime()))
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if expiryWindow, ok := ws.windows[timeExpiry]; ok {
		expiryWindow = append(expiryWindow, e)
		ws.windows[timeExpiry] = expiryWindow
	} else {
		ws.windows[timeExpiry] = _ExpiryWindowEntries{e}
	}

	return nil
}
