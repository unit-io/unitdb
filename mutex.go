package tracedb

import (
	"sync"

	"github.com/unit-io/tracedb/hash"
)

const (
	nMutex = 16
)

// mutex mutex to lock/unlock on topic based on contract
type mutex struct {
	internal   []sync.RWMutex
	consistent *hash.Consistent
}

// newMutex creates mutex to lock/unlock contract and specific topic prefix.
func newMutex() mutex {
	mu := mutex{
		internal:   make([]sync.RWMutex, nMutex),
		consistent: hash.InitConsistent(int(nMutex), int(nMutex)),
	}

	for i := 0; i < nMutex; i++ {
		mu.internal[i] = sync.RWMutex{}
	}

	return mu
}

// GetMutex returns mutex under given contract for a specific topic
func (mu *mutex) getMutex(contract uint64) sync.RWMutex {
	return mu.internal[mu.consistent.FindBlock(contract)]
}
