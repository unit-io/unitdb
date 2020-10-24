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
	timeRecord struct {
		refs      int
		lastUnref int64
	}

	timeMark struct {
		sync.RWMutex
		timeRecord
		records         map[int64]timeRecord // map of timeID pending commit.
		releasedRecords map[int64]timeRecord // map of timeID commit applied.
	}
)

func newTimeMark() *timeMark {
	return &timeMark{timeRecord: timeRecord{lastUnref: time.Now().UTC().UnixNano()}, records: make(map[int64]timeRecord), releasedRecords: make(map[int64]timeRecord)}
}

func (r timeRecord) isExpired(expDur time.Duration) bool {
	if r.lastUnref > 0 && r.lastUnref+expDur.Nanoseconds() <= int64(time.Now().UTC().Nanosecond()) {
		return true
	}
	return false
}

func (r timeRecord) isReleased(lastUnref int64) bool {
	if r.lastUnref > 0 && r.lastUnref < lastUnref {
		return true
	}
	return false
}

func (tm *timeMark) newID() int64 {
	tm.Lock()
	defer tm.Unlock()
	timeID := time.Now().UTC().UnixNano()
	if tm, ok := tm.records[timeID]; ok {
		tm.refs++
		return timeID
	}
	tm.records[timeID] = timeRecord{refs: 1}
	return timeID
}

func (tm *timeMark) release(timeID int64) {
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
		timeMark.lastUnref = tm.timeRecord.lastUnref
		tm.releasedRecords[timeID] = timeMark
	}
}

func (tm *timeMark) isReleased(timeID int64) bool {
	if tm, ok := tm.releasedRecords[timeID]; ok {
		if tm.refs == -1 {
			// timeID is aborted
			return false
		}
		if tm.isReleased(tm.lastUnref) {
			return true
		}
	}
	return false
}

func (tm *timeMark) isAborted(timeID int64) bool {
	tm.RLock()
	defer tm.RUnlock()
	if tm, ok := tm.releasedRecords[timeID]; ok {
		if tm.refs == -1 {
			// timeID is aborted
			return true
		}
	}
	return false
}

func (tm *timeMark) abort(timeID int64) {
	tm.Lock()
	defer tm.Unlock()

	if _, ok := tm.records[timeID]; ok {
		delete(tm.records, timeID)
	}
	timeRecord := timeRecord{refs: -1, lastUnref: tm.timeRecord.lastUnref}
	tm.releasedRecords[timeID] = timeRecord
}

func (tm *timeMark) startReleaser(dur time.Duration) {
	tm.Lock()
	defer tm.Unlock()

	for timeID, r := range tm.releasedRecords {
		if r.isExpired(dur) {
			delete(tm.releasedRecords, timeID)
		}
	}
}
