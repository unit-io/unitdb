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
	"time"
)

type (
	_TimeRecord struct {
		refs      int
		lastUnref _TimeID
	}

	TimeMark struct {
		sync.RWMutex
		durations       time.Duration
		timeRecord      _TimeRecord
		records         map[_TimeID]_TimeRecord
		releasedRecords map[_TimeID]_TimeRecord
	}
)

func newTimeMark(maxDur time.Duration) *TimeMark {
	return &TimeMark{durations: maxDur, timeRecord: _TimeRecord{lastUnref: _TimeID(time.Now().UTC().UnixNano())}, records: make(map[_TimeID]_TimeRecord), releasedRecords: make(map[_TimeID]_TimeRecord)}
}

func (r _TimeRecord) isExpired(expDur time.Duration) bool {
	if r.lastUnref > 0 && int64(r.lastUnref)+expDur.Nanoseconds() <= int64(time.Now().UTC().Nanosecond()) {
		return true
	}
	return false
}

func (r _TimeRecord) isReleased(lastUnref _TimeID) bool {
	if r.lastUnref > 0 && r.lastUnref < lastUnref {
		return true
	}
	return false
}

func (tm *TimeMark) newTimeID() _TimeID {
	timeID := _TimeID(time.Now().UTC().Truncate(tm.durations).UnixNano())
	tm.add(timeID)

	return timeID
}

func (tm *TimeMark) add(timeID _TimeID) {
	tm.Lock()
	defer tm.Unlock()
	if r, ok := tm.records[timeID]; ok {
		r.refs++
	}
	tm.records[timeID] = _TimeRecord{refs: 1}
}

func (tm *TimeMark) release(timeID _TimeID) {
	tm.Lock()
	defer tm.Unlock()

	timeMark, ok := tm.records[timeID]
	if !ok {
		return
	}
	timeMark.refs--
	if timeMark.refs > 0 {
		tm.records[timeID] = timeMark
	} else {
		delete(tm.records, timeID)
		timeMark.lastUnref = _TimeID(time.Now().UTC().UnixNano())
		// timeMark.lastUnref = tm.timeRecord.lastUnref
		tm.releasedRecords[timeID] = timeMark
	}
}

func (tm *TimeMark) IsReleased(timeID int64) bool {
	tm.RLock()
	defer tm.RUnlock()
	if r, ok := tm.releasedRecords[_TimeID(timeID)]; ok {
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

func (tm *TimeMark) IsAborted(timeID int64) bool {
	tm.RLock()
	defer tm.RUnlock()
	if r, ok := tm.releasedRecords[_TimeID(timeID)]; ok {
		if r.refs == -1 {
			// timeID is aborted
			return true
		}
	}
	return false
}

func (tm *TimeMark) abort(timeID _TimeID) {
	tm.Lock()
	defer tm.Unlock()

	if _, ok := tm.records[timeID]; ok {
		delete(tm.records, timeID)
	}
	r := _TimeRecord{refs: -1, lastUnref: tm.timeRecord.lastUnref}
	tm.releasedRecords[timeID] = r
}

func (tm *TimeMark) StartReleaser(dur time.Duration) {
	tm.Lock()
	defer tm.Unlock()

	for timeID, r := range tm.releasedRecords {
		if r.isExpired(dur) {
			delete(tm.releasedRecords, timeID)
		}
	}
}
