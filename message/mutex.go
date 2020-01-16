package message

import (
	"sync"

	"github.com/unit-io/tracedb/hash"
)

const (
	nMutex = 16
)

// Mutex mutex to lock/unlock on topic based on contract
type Mutex struct {
	internal   []sync.RWMutex
	consistent *hash.Consistent
}

// NewMutex creates mutex to lock/unlock contract and specific topic prefix.
func NewMutex() Mutex {
	mu := Mutex{
		internal:   make([]sync.RWMutex, nMutex),
		consistent: hash.InitConsistent(int(nMutex), int(nMutex)),
	}

	for i := 0; i < nMutex; i++ {
		mu.internal[i] = sync.RWMutex{}
	}

	return mu
}

// GetMutex returns mutex under given contract for a specific topic
func (mu *Mutex) GetMutex(contract uint64) sync.RWMutex {
	return mu.internal[mu.consistent.FindBlock(contract)]
}
