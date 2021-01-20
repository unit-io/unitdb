package message

import (
	"sync"
)

// MID is 32-bit local message identifier
type MID uint32

type MessageIds struct {
	sync.RWMutex
	id    MID
	index map[MID]uint8 // map[MID]PacketType
}

func NewMessageIds() MessageIds {
	return MessageIds{
		index: make(map[MID]uint8),
	}
}

func (mids *MessageIds) Reset(id MID) {
	mids.Lock()
	defer mids.Unlock()
	mids.id = id
}

func (mids *MessageIds) FreeID(id MID) {
	mids.Lock()
	defer mids.Unlock()
	delete(mids.index, id)
}

func (mids *MessageIds) NextID(pktType uint8) MID {
	mids.Lock()
	defer mids.Unlock()
	mids.id--
	mids.index[mids.id] = pktType
	return mids.id
}

func (mids *MessageIds) GetType(id MID) uint8 {
	mids.RLock()
	defer mids.RUnlock()
	return mids.index[id]
}
