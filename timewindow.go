package tracedb

import (
	"math"
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
	windows    []*windows
	consistent *hash.Consistent
}

type windows struct {
	expiry       map[int64]timeWindow
	blocks       map[int64]timeWindow
	sync.RWMutex // Read Write mutex, guards access to internal collection.
}

// newTimeWindows creates a new concurrent timeWindows.
func newTimeWindows() timeWindows {
	w := timeWindows{
		windows:    make([]*windows, nShards+1),
		consistent: hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
		w.windows[i] = &windows{expiry: make(map[int64]timeWindow), blocks: make(map[int64]timeWindow)}
	}

	return w
}

// getWindows returns shard under given key
func (w *timeWindows) getWindows(key uint64) *windows {
	w.RLock()
	defer w.RUnlock()
	return w.windows[w.consistent.FindBlock(key)]
}

type timeWindowEntry interface {
	expiryTime() uint32
	timeStamp() uint32
}

type timeWindow struct {
	entries []timeWindowEntry
}

// type timeHash int64

type timeWindowBucket struct {
	file
	timeWindows
	durationType       time.Duration
	maxDurations       int
	blockDurations     int
	earliestExpiryHash int64
}

func newTimeWindowBucket(f file, durType time.Duration, maxDur, blockDur int) timeWindowBucket {
	l := timeWindowBucket{file: f}
	l.timeWindows = newTimeWindows()
	l.durationType = durType
	l.blockDurations = blockDur
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
		ws := wb.timeWindows.windows[i]
		ws.Lock()
		defer ws.Unlock()
		windowTimes := make([]int64, 0, len(ws.expiry))
		for windowTime := range ws.expiry {
			windowTimes = append(windowTimes, windowTime)
		}
		sort.Slice(windowTimes[:], func(i, j int) bool { return windowTimes[i] < windowTimes[j] })
		for i := 0; i < len(windowTimes); i++ {
			if windowTimes[i] > int64(startTime) || len(expiredEntries) > maxResults {
				break
			}
			window := ws.expiry[windowTimes[i]]
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
				delete(ws.expiry, windowTimes[i])
			}
		}
	}
	return expiredEntries
}

func (wb *timeWindowBucket) add(e timeWindowEntry) {
	// logger.Printf("entry add time %v", time.Unix(int64(entry.timeStamp()), 0).Truncate(wb.durationType))
	expiryTime := int64(time.Unix(int64(e.expiryTime()), 0).Truncate(wb.durationType).Add(1 * wb.durationType).Unix())
	entryTime := int64(time.Unix(int64(e.timeStamp()), 0).Truncate(wb.durationType).Add(1 * wb.durationType).Unix())
	atomic.CompareAndSwapInt64(&wb.earliestExpiryHash, 0, expiryTime)

	// get windows shard
	ws := wb.getWindows(uint64(expiryTime))
	ws.Lock()
	defer ws.Unlock()
	if expiry, ok := ws.expiry[expiryTime]; ok {
		ws.expiry[expiryTime] = timeWindow{entries: append(expiry.entries, e)}
	} else {
		ws.expiry[expiryTime] = timeWindow{entries: []timeWindowEntry{e}}
		if atomic.LoadInt64(&wb.earliestExpiryHash) > expiryTime {
			wb.earliestExpiryHash = expiryTime
		}
	}

	if block, ok := ws.blocks[entryTime]; ok {
		ws.blocks[entryTime] = timeWindow{entries: append(block.entries, e)}
	} else {
		ws.blocks[entryTime] = timeWindow{entries: []timeWindowEntry{e}}
	}
}

func (wb *timeWindowBucket) readAll(maxResults int) (startTime time.Time, entries []timeWindowEntry) {
	wb.expireOldEntries(math.MaxInt32)
	startTime = time.Now().Add(-time.Duration(wb.blockDurations) * wb.durationType)

	start := uint32(startTime.Unix())
	for i := 0; i < nShards; i++ {
		// get windows shard
		ws := wb.timeWindows.windows[i]
		ws.Lock()
		defer ws.Unlock()
		for windowTime := range ws.expiry {
			window := ws.expiry[windowTime]

			for i := range window.entries {
				entry := window.entries[i]

				if entry.timeStamp() > start {
					entries = append(entries, entry)
				}
			}
		}
	}

	return startTime, entries
}
