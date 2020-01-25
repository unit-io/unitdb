package tracedb

import (
	"sort"
	"sync"
	"sync/atomic"
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
	windows      map[int64]timeWindow
	sync.RWMutex // Read Write mutex, guards access to internal collection.
}

// newTimeWindows creates a new concurrent timeWindows.
func newTimeWindows() timeWindows {
	tw := timeWindows{
		timeWindows: make([]*windows, nShards+1),
		consistent:  hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
		tw.timeWindows[i] = &windows{windows: make(map[int64]timeWindow)}
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

// type timeHash int64

type timeWindowBucket struct {
	timeWindows
	durationType       time.Duration
	maxDurations       int
	earliestExpiryHash int64
}

func newTimeWindowBucket(durType time.Duration, maxDur int) timeWindowBucket {
	l := timeWindowBucket{}
	l.timeWindows = newTimeWindows()
	l.durationType = durType
	l.maxDurations = maxDur

	return l
}

func (wb *timeWindowBucket) expireOldEntries(maxResults int) []timeWindowEntry {
	var expiredEntries []timeWindowEntry
	startTime := uint32(time.Now().Unix())

	if atomic.LoadInt64(&wb.earliestExpiryHash) > int64(startTime) {
		return expiredEntries
	}

	for i := 0; i < nShards; i++ {
		// get windows shard
		ws := wb.timeWindows.timeWindows[i]
		ws.Lock()
		defer ws.Unlock()
		windowTimes := make([]int64, 0, len(ws.windows))
		for windowTime := range ws.windows {
			windowTimes = append(windowTimes, windowTime)
		}
		sort.Slice(windowTimes[:], func(i, j int) bool { return windowTimes[i] < windowTimes[j] })
		for i := 0; i < len(windowTimes); i++ {
			if windowTimes[i] > int64(startTime) || len(expiredEntries) > maxResults {
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
	entryTime := int64(time.Unix(int64(e.timeStamp()), 0).Truncate(wb.durationType).Add(1 * wb.durationType).Unix())
	atomic.CompareAndSwapInt64(&wb.earliestExpiryHash, 0, entryTime)

	// get windows shard
	ws := wb.getWindows(uint64(entryTime))
	ws.Lock()
	defer ws.Unlock()
	if window, ok := ws.windows[entryTime]; ok {
		ws.windows[entryTime] = timeWindow{entries: append(window.entries, e)}
	} else {
		ws.windows[entryTime] = timeWindow{entries: []timeWindowEntry{e}}
		if atomic.LoadInt64(&wb.earliestExpiryHash) > entryTime {
			wb.earliestExpiryHash = entryTime
		}
	}
}

func (wb *timeWindowBucket) all(maxResults int) []timeWindowEntry {
	wb.expireOldEntries(maxResults)
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
