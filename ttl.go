package tracedb

import (
	"sort"
	"sync"
	"time"

	"github.com/unit-io/tracedb/hash"
)

// A "thread" safe timeWindows.
// To avoid lock bottlenecks timeWindows are dived to several (nShards).
type timeWindows struct {
	sync.RWMutex
	timeWindows []*windows
	consistent  *hash.Consistent
}

type windows struct {
	windows      map[timeHash]timeWindow
	sync.RWMutex // Read Write mutex, guards access to internal collection.
}

// newTimeWindows creates a new concurrent timeWindows.
func newTimeWindows() timeWindows {
	tw := timeWindows{
		timeWindows: make([]*windows, nShards+1),
		consistent:  hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
		tw.timeWindows[i] = &windows{windows: make(map[timeHash]timeWindow)}
	}

	return tw
}

// getWindows returns shard under given key
func (tw *timeWindows) getWindows(key uint64) *windows {
	tw.RLock()
	defer tw.RUnlock()
	return tw.timeWindows[tw.consistent.FindBlock(key)]
}

type timeWindowEntry interface {
	timeStamp() uint32
}

type timeWindow struct {
	entries []timeWindowEntry
}

type timeHash int64

type timeWindowBucket struct {
	timeWindows
	durationType       time.Duration
	maxDurations       int
	earliestExpiryHash timeHash
}

func newTimeWindowBucket(durType time.Duration, maxDur int) timeWindowBucket {
	l := timeWindowBucket{}
	l.timeWindows = newTimeWindows()
	l.durationType = durType
	l.maxDurations = maxDur

	return l
}

func (wb *timeWindowBucket) expireOldEntries() []timeWindowEntry {
	var expiredEntries []timeWindowEntry
	startTime := uint32(time.Now().Unix())

	if timeHash(startTime) < wb.earliestExpiryHash {
		return expiredEntries
	}

	for i := 0; i < nShards; i++ {
		// get windows shard
		ws := wb.timeWindows.timeWindows[i]
		ws.Lock()
		defer ws.Unlock()
		windowTimes := make([]timeHash, 0, len(ws.windows))
		for windowTime := range ws.windows {
			windowTimes = append(windowTimes, windowTime)
		}
		sort.Slice(windowTimes[:], func(i, j int) bool { return windowTimes[i] < windowTimes[j] })
		for i := 0; i < len(windowTimes); i++ {
			if windowTimes[i] > timeHash(startTime) {
				break
			}
			window := ws.windows[windowTimes[i]]
			expiredEntriesCount := 0
			for i := range window.entries {
				entry := window.entries[i]
				if entry.timeStamp() < startTime {
					// logger.Printf("deleteing expired key: %v", time.Unix(int64(entry.timeStamp()), 0))
					expiredEntries = append(expiredEntries, entry)
					expiredEntriesCount++
				}
			}
			if expiredEntriesCount == len(window.entries) {
				delete(ws.windows, windowTimes[i])
			}
		}
	}
	return expiredEntries
}

func (wb *timeWindowBucket) add(e timeWindowEntry) {
	// logger.Printf("entry add time %v", time.Unix(int64(entry.timeStamp()), 0).Truncate(wb.durationType))
	entryTime := timeHash(time.Unix(int64(e.timeStamp()), 0).Truncate(wb.durationType).Add(1 * wb.durationType).Unix())
	if wb.earliestExpiryHash == 0 {
		wb.earliestExpiryHash = entryTime
	}
	// get windows shard
	ws := wb.getWindows(uint64(entryTime))
	ws.Lock()
	defer ws.Unlock()
	if window, ok := ws.windows[entryTime]; ok {
		ws.windows[entryTime] = timeWindow{entries: append(window.entries, e)}
	} else {
		ws.windows[entryTime] = timeWindow{entries: []timeWindowEntry{e}}
		if wb.earliestExpiryHash > entryTime {
			wb.earliestExpiryHash = entryTime
		}
	}

	// wb.expireOldEntries()
}

// // addExpired adds expired entries to timewindow
// func (wb *timeWindowBucket) addExpired(e timeWindowEntry) {
// 	// logger.Printf("entry add time %v", time.Unix(int64(entry.timeStamp()), 0).Truncate(wb.durationType))
// 	entryTime := timeHash(time.Now().Truncate(wb.durationType).Add(1 * wb.durationType).Unix())
// 	// get windows shard
// 	ws := wb.getWindows(uint64(entryTime))
// 	ws.Lock()
// 	defer ws.Unlock()
// 	if window, ok := ws.windows[entryTime]; ok {
// 		ws.windows[entryTime] = timeWindow{entries: append(window.entries, e)}
// 	} else {
// 		ws.windows[entryTime] = timeWindow{entries: []timeWindowEntry{e}}
// 	}
// }

func (wb *timeWindowBucket) all() []timeWindowEntry {
	wb.expireOldEntries()
	startTime := time.Now().Add(-time.Duration(wb.maxDurations) * wb.durationType).Unix()

	var all []timeWindowEntry
	for i := 0; i < nShards; i++ {
		// get windows shard
		ws := wb.timeWindows.timeWindows[i]
		ws.Lock()
		defer ws.Unlock()
		for windowTime := range ws.windows {
			window := ws.windows[windowTime]

			for i := range window.entries {
				entry := window.entries[i]

				if entry.timeStamp() > uint32(startTime) {
					all = append(all, entry)
				}
			}
		}
	}

	return all
}
