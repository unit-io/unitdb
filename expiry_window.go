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

type expiryWindow struct {
	windows map[int64]expiryWindowEntries // map[expiryHash]windowEntries.

	mu sync.RWMutex // Read Write mutex, guards access to internal collection.
}

type expiryWindows struct {
	sync.RWMutex
	expiry     []*expiryWindow
	consistent *hash.Consistent
}

// newExpiryWindows creates a new concurrent expiryWindows.
func newExpiryWindows() *expiryWindows {
	w := &expiryWindows{
		expiry:     make([]*expiryWindow, nShards),
		consistent: hash.InitConsistent(nShards, nShards),
	}

	for i := 0; i < nShards; i++ {
		w.expiry[i] = &expiryWindow{windows: make(map[int64]expiryWindowEntries)}
	}

	return w
}

type expiryWindowEntries []timeWindowEntry

type timeWindowEntry interface {
	expiryTime() uint32
}

// getWindows returns shard under given key.
func (w *expiryWindows) getWindows(key uint64) *expiryWindow {
	w.RLock()
	defer w.RUnlock()
	return w.expiry[w.consistent.FindBlock(key)]
}

type expiryWindowBucket struct {
	sync.RWMutex
	*expiryWindows

	expDurationType     time.Duration
	maxExpDurations     int
	backgroundKeyExpiry bool
	earliestExpiryHash  int64
}

func newExpiryWindowBucket(bgKeyExp bool, expDurType time.Duration, maxExpDur int) *expiryWindowBucket {
	ex := &expiryWindowBucket{backgroundKeyExpiry: bgKeyExp, expDurationType: expDurType, maxExpDurations: maxExpDur}
	ex.expiryWindows = newExpiryWindows()
	return ex
}

func (wb *expiryWindowBucket) getExpiredEntries(maxResults int) []timeWindowEntry {
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
func (wb *expiryWindowBucket) addExpiry(e timeWindowEntry) error {
	if !wb.backgroundKeyExpiry {
		return nil
	}
	timeExpiry := int64(time.Unix(int64(e.expiryTime()), 0).Truncate(wb.expDurationType).Add(1 * wb.expDurationType).Unix())
	atomic.CompareAndSwapInt64(&wb.earliestExpiryHash, 0, timeExpiry)

	// get windows shard.
	ws := wb.getWindows(uint64(e.expiryTime()))
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if expiryWindow, ok := ws.windows[timeExpiry]; ok {
		expiryWindow = append(expiryWindow, e)
		ws.windows[timeExpiry] = expiryWindow
	} else {
		ws.windows[timeExpiry] = expiryWindowEntries{e}
	}

	return nil
}
