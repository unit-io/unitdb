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
		lastUnref timeID
	}

	timeMark struct {
		sync.RWMutex
		timeRecord
		records         map[timeID]timeRecord
		releasedRecords map[timeID]timeRecord
	}
)

func newTimeMark() *timeMark {
	return &timeMark{timeRecord: timeRecord{lastUnref: timeID(time.Now().UTC().UnixNano())}, records: make(map[timeID]timeRecord), releasedRecords: make(map[timeID]timeRecord)}
}

func (r timeRecord) isExpired(expDur time.Duration) bool {
	if r.lastUnref > 0 && int64(r.lastUnref)+expDur.Nanoseconds() <= int64(time.Now().UTC().Nanosecond()) {
		return true
	}
	return false
}

func (r timeRecord) isReleased(lastUnref timeID) bool {
	if r.lastUnref > 0 && r.lastUnref < lastUnref {
		return true
	}
	return false
}

func (tm *timeMark) newTimeID() timeID {
	tm.Lock()
	defer tm.Unlock()
	tmID := timeID(time.Now().UTC().UnixNano())
	if tm, ok := tm.records[tmID]; ok {
		tm.refs++
		return tmID
	}
	tm.records[tmID] = timeRecord{refs: 1}
	return tmID
}

func (tm *timeMark) release(tmID timeID) {
	tm.Lock()
	defer tm.Unlock()

	timeMark, ok := tm.records[tmID]
	if !ok {
		return
	}
	timeMark.refs--
	if timeMark.refs > 0 {
		tm.records[tmID] = timeMark
	} else {
		delete(tm.records, tmID)
		timeMark.lastUnref = tm.timeRecord.lastUnref
		tm.releasedRecords[tmID] = timeMark
	}
}

func (tm *timeMark) isReleased(tmID timeID) bool {
	if tm, ok := tm.releasedRecords[tmID]; ok {
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

func (tm *timeMark) isAborted(tmID timeID) bool {
	tm.RLock()
	defer tm.RUnlock()
	if tm, ok := tm.releasedRecords[tmID]; ok {
		if tm.refs == -1 {
			// timeID is aborted
			return true
		}
	}
	return false
}

func (tm *timeMark) abort(tmID timeID) {
	tm.Lock()
	defer tm.Unlock()

	if _, ok := tm.records[tmID]; ok {
		delete(tm.records, tmID)
	}
	timeRecord := timeRecord{refs: -1, lastUnref: tm.timeRecord.lastUnref}
	tm.releasedRecords[tmID] = timeRecord
}

func (tm *timeMark) startReleaser(dur time.Duration) {
	tm.Lock()
	defer tm.Unlock()

	for tmID, r := range tm.releasedRecords {
		if r.isExpired(dur) {
			delete(tm.releasedRecords, tmID)
		}
	}
}
