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
		maxDuration     time.Duration
		records         map[_TimeID]_TimeRecord
		releasedRecords map[_TimeID]_TimeRecord
	}
)

func newTimeMark(maxDuration time.Duration) *_TimeMark {
	return &_TimeMark{maxDuration: maxDuration, records: make(map[_TimeID]_TimeRecord), releasedRecords: make(map[_TimeID]_TimeRecord)}
}

func (r _TimeRecord) isReleased(timeUnref _TimeID) bool {
	if r.lastUnref > 0 && r.lastUnref < timeUnref {
		return true
	}
	return false
}

func (tm *_TimeMark) timeNow() _TimeID {
	ID := _TimeID(time.Now().UTC().UnixNano())
	tm.add(ID)

	return ID
}

func (tm *_TimeMark) timeID(ID _TimeID) _TimeID {
	return _TimeID(time.Unix(int64(ID), 0).UTC().Truncate(tm.maxDuration).Unix())
}

func (tm *_TimeMark) add(ID _TimeID) {
	tm.Lock()
	defer tm.Unlock()
	timeID := tm.timeID(ID)
	if r, ok := tm.records[timeID]; ok {
		r.refs++
	}
	tm.records[timeID] = _TimeRecord{refs: 1}
}

func (tm *_TimeMark) release(ID _TimeID) {
	tm.Lock()
	defer tm.Unlock()

	timeID := tm.timeID(ID)
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
		tm.releasedRecords[timeID] = timeMark
	}
}

func (tm *_TimeMark) timeRefs() (timeUnref _TimeID, timeIDs []_TimeID) {
	timeUnref = _TimeID(time.Now().UTC().UnixNano())
	tm.RLock()
	defer tm.RUnlock()
	for timeID, r := range tm.releasedRecords {
		if r.isReleased(timeUnref) {
			timeIDs = append(timeIDs, timeID)
		}
	}
	sort.Slice(timeIDs[:], func(i, j int) bool {
		return timeIDs[i] < timeIDs[j]
	})

	return timeUnref, timeIDs
}

func (tm *_TimeMark) timeUnref(timeUnref _TimeID) {
	tm.Lock()
	defer tm.Unlock()

	for timeID, r := range tm.releasedRecords {
		if r.lastUnref < timeUnref {
			delete(tm.releasedRecords, timeID)
		}
	}
}
