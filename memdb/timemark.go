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
	"sort"
	"sync"
	"time"
)

type (
	_TimeRecord struct {
		refs      int
		lastUnref _TimeID
	}

	_TimeMark struct {
		sync.RWMutex
		expiryDurations time.Duration
		timeRef         _TimeID
		records         map[_TimeID]_TimeRecord
		releasedRecords map[_TimeID]_TimeRecord
	}
)

func newTimeMark(expiryDuration time.Duration) *_TimeMark {
	return &_TimeMark{expiryDurations: expiryDuration, timeRef: _TimeID(time.Now().UTC().UnixNano()), records: make(map[_TimeID]_TimeRecord), releasedRecords: make(map[_TimeID]_TimeRecord)}
}

func (r _TimeRecord) isExpired(expDur time.Duration) bool {
	if r.lastUnref > 0 && int64(time.Unix(int64(r.lastUnref), 0).UTC().Add(expDur).Nanosecond()) <= int64(time.Now().UTC().Nanosecond()) {
		return true
	}
	return false
}

func (r _TimeRecord) isReleased(timeRef _TimeID) bool {
	if r.lastUnref > 0 && r.lastUnref < timeRef {
		return true
	}
	return false
}

func (tm *_TimeMark) newTimeID() _TimeID {
	timeID := _TimeID(time.Now().UTC().UnixNano())
	tm.add(timeID)

	return timeID
}

func (tm *_TimeMark) add(timeID _TimeID) {
	tm.Lock()
	defer tm.Unlock()
	if r, ok := tm.records[timeID]; ok {
		r.refs++
	}
	tm.records[timeID] = _TimeRecord{refs: 1}
}

func (tm *_TimeMark) release(timeID _TimeID) {
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
		timeMark.lastUnref = tm.timeRef
		tm.releasedRecords[timeID] = timeMark
	}
}

func (tm *_TimeMark) getRecords() (timeIDs []_TimeID) {
	tm.Lock()
	tm.timeRef = _TimeID(time.Now().UTC().UnixNano())
	tm.Unlock()
	tm.RLock()
	defer tm.RUnlock()
	for timeID, r := range tm.releasedRecords {
		if r.isReleased(tm.timeRef) {
			timeIDs = append(timeIDs, timeID)
		}
	}
	sort.Slice(timeIDs[:], func(i, j int) bool {
		return timeIDs[i] < timeIDs[j]
	})

	return timeIDs
}

func (tm *_TimeMark) startExpirer() {
	tm.Lock()
	defer tm.Unlock()

	for timeID, r := range tm.releasedRecords {
		if r.isExpired(tm.expiryDurations) {
			delete(tm.releasedRecords, timeID)
		}
	}
}
