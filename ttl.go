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
	windows      map[timeHash]timeWindow
	durationType time.Duration
	maxDurations int
}

func newTimeWindowBucket(durType time.Duration, maxDur int) timeWindowBucket {
	l := timeWindowBucket{}
	l.windows = make(map[timeHash]timeWindow)
	l.durationType = durType
	l.maxDurations = maxDur

	return l
}

func (wl *timeWindowBucket) expireOldEntries() []timeWindowEntry {
	var expiredEntries []timeWindowEntry
	startTime := time.Now().Add(-time.Duration(wl.maxDurations) * wl.durationType).Unix()

	windowTimes := make([]timeHash, 0, len(wl.windows))
	for windowTime := range wl.windows {
		windowTimes = append(windowTimes, windowTime)
	}
	sort.Slice(windowTimes[:], func(i, j int) bool { return windowTimes[i] < windowTimes[j] })
	for i := 0; i < len(windowTimes); i++ {
		if windowTimes[i] >= timeHash(startTime) {
			break
		}
		window := wl.windows[windowTimes[i]]
		for i := range window.entries {
			entry := window.entries[i]

			expiredEntries = append(expiredEntries, entry)
		}
		delete(wl.windows, windowTimes[i])
	}
	return expiredEntries
}

func (wl *timeWindowBucket) add(entry timeWindowEntry) {
	logger.Printf("entry add time %v", time.Unix(int64(entry.timeStamp()), 0).Truncate(wl.durationType).Add(wl.durationType))
	entryTime := timeHash(time.Unix(int64(entry.timeStamp()), 0).Truncate(wl.durationType).Unix())

	if window, ok := wl.windows[entryTime]; ok {
		wl.windows[entryTime] = timeWindow{entries: append(window.entries, entry)}
	} else {
		wl.windows[entryTime] = timeWindow{entries: []timeWindowEntry{entry}}
	}

	// wl.expireOldEntries()
}

func (wl *timeWindowBucket) all() []timeWindowEntry {
	wl.expireOldEntries()
	startTime := time.Now().Add(-time.Duration(wl.maxDurations) * wl.durationType).Unix()

	var all []timeWindowEntry
	for windowTime := range wl.windows {
		window := wl.windows[windowTime]

		for i := range window.entries {
			entry := window.entries[i]

			if entry.timeStamp() > uint32(startTime) {
				all = append(all, entry)
			}
		}
	}

	return all
}
