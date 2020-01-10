package tracedb

import (
	"sort"
	"time"
)

type timeWindowEntry interface {
	timeStamp() uint32
}

type timeWindow struct {
	entries []timeWindowEntry
}

type timeHash int64

type timeWindowBucket struct {
	windows            map[timeHash]timeWindow
	durationType       time.Duration
	maxDurations       int
	earliestExpiryHash timeHash
}

func newTimeWindowBucket(durType time.Duration, maxDur int) timeWindowBucket {
	l := timeWindowBucket{}
	l.windows = make(map[timeHash]timeWindow)
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

	windowTimes := make([]timeHash, 0, len(wb.windows))
	for windowTime := range wb.windows {
		windowTimes = append(windowTimes, windowTime)
	}
	sort.Slice(windowTimes[:], func(i, j int) bool { return windowTimes[i] < windowTimes[j] })
	for i := 0; i < len(windowTimes); i++ {
		if windowTimes[i] > timeHash(startTime) {
			break
		}
		window := wb.windows[windowTimes[i]]
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
			delete(wb.windows, windowTimes[i])
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
	if window, ok := wb.windows[entryTime]; ok {
		wb.windows[entryTime] = timeWindow{entries: append(window.entries, e)}
	} else {
		wb.windows[entryTime] = timeWindow{entries: []timeWindowEntry{e}}
		if wb.earliestExpiryHash > entryTime {
			wb.earliestExpiryHash = entryTime
		}
	}

	// wb.expireOldEntries()
}

func (wb *timeWindowBucket) all() []timeWindowEntry {
	wb.expireOldEntries()
	startTime := time.Now().Add(-time.Duration(wb.maxDurations) * wb.durationType).Unix()

	var all []timeWindowEntry
	for windowTime := range wb.windows {
		window := wb.windows[windowTime]

		for i := range window.entries {
			entry := window.entries[i]

			if entry.timeStamp() > uint32(startTime) {
				all = append(all, entry)
			}
		}
	}

	return all
}
