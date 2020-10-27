package unitdb

import (
	"sync"
	"time"
)

type (
	_TimeRecord struct {
		refs      int
		lastUnref int64
	}

	_TimeMark struct {
		sync.RWMutex
		timeRecord      _TimeRecord
		records         map[int64]_TimeRecord // map of timeID pending commit.
		releasedRecords map[int64]_TimeRecord // map of timeID commit applied.
	}
)

func newTimeMark() _TimeMark {
	return _TimeMark{timeRecord: _TimeRecord{lastUnref: time.Now().UTC().UnixNano()}, records: make(map[int64]_TimeRecord), releasedRecords: make(map[int64]_TimeRecord)}
}

func (r _TimeRecord) isExpired(expDur time.Duration) bool {
	if r.lastUnref > 0 && r.lastUnref+expDur.Nanoseconds() <= int64(time.Now().UTC().Nanosecond()) {
		return true
	}
	return false
}

func (r _TimeRecord) isReleased(lastUnref int64) bool {
	if r.lastUnref > 0 && r.lastUnref < lastUnref {
		return true
	}
	return false
}

func (tm *_TimeMark) newID() int64 {
	tm.Lock()
	defer tm.Unlock()
	timeID := time.Now().UTC().UnixNano()
	if r, ok := tm.records[timeID]; ok {
		r.refs++
		return timeID
	}
	tm.records[timeID] = _TimeRecord{refs: 1}
	return timeID
}

func (tm *_TimeMark) release(timeID int64) {
	tm.Lock()
	defer tm.Unlock()

	r, ok := tm.records[timeID]
	if !ok {
		return
	}
	r.refs--
	if r.refs > 0 {
		tm.records[timeID] = r
	} else {
		delete(tm.records, timeID)
		r.lastUnref = tm.timeRecord.lastUnref
		tm.releasedRecords[timeID] = r
	}
}

func (tm *_TimeMark) isReleased(timeID int64) bool {
	if r, ok := tm.releasedRecords[timeID]; ok {
		if r.refs == -1 {
			// timeID is aborted
			return false
		}
		if r.isReleased(tm.timeRecord.lastUnref) {
			return true
		}
	}
	return false
}

func (tm *_TimeMark) isAborted(timeID int64) bool {
	tm.RLock()
	defer tm.RUnlock()
	if r, ok := tm.releasedRecords[timeID]; ok {
		if r.refs == -1 {
			// timeID is aborted
			return true
		}
	}
	return false
}

func (tm *_TimeMark) abort(timeID int64) {
	tm.Lock()
	defer tm.Unlock()

	if _, ok := tm.records[timeID]; ok {
		delete(tm.records, timeID)
	}
	r := _TimeRecord{refs: -1, lastUnref: tm.timeRecord.lastUnref}
	tm.releasedRecords[timeID] = r
}

func (tm *_TimeMark) startReleaser(dur time.Duration) {
	tm.Lock()
	defer tm.Unlock()

	for timeID, r := range tm.releasedRecords {
		if r.isExpired(dur) {
			delete(tm.releasedRecords, timeID)
		}
	}
}
