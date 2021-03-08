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

package message

import (
	"sync"
)

// MID is 32-bit local message identifier
type MID uint16

type MessageIds struct {
	sync.RWMutex
	id     uint16
	resume map[uint16]struct{}
	index  map[uint16]uint8 // map[MID]PacketType
}

func NewMessageIds() MessageIds {
	return MessageIds{
		resume: make(map[uint16]struct{}),
		index:  make(map[uint16]uint8),
	}
}

func (mids *MessageIds) Reset() {
	mids.Lock()
	defer mids.Unlock()
	mids.id = 0
}

func (mids *MessageIds) ResumeID(id uint16) {
	mids.Lock()
	defer mids.Unlock()
	mids.resume[id] = struct{}{}
}

func (mids *MessageIds) FreeID(id uint16) {
	mids.Lock()
	defer mids.Unlock()
	delete(mids.index, id)
}

func (mids *MessageIds) NextID(pktType uint8) uint16 {
	mids.Lock()
	defer mids.Unlock()
	mids.id++
	if _, ok := mids.resume[mids.id]; ok {
		mids.NextID(pktType)
	}
	mids.index[mids.id] = pktType
	return mids.id
}

func (mids *MessageIds) GetType(id uint16) uint8 {
	mids.RLock()
	defer mids.RUnlock()
	return mids.index[id]
}
