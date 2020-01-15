package tracedb

import (
	"sync"

	"github.com/unit-io/tracedb/hash"
)

const (
	nMutex = 16
)

type concurrentMutex struct {
	sync.RWMutex // Read Write mutex, guards access to internal request.
}

// mutex mutex to lock/unlock on topic based on contract and not lock/unlock db
type mutex struct {
	sync.RWMutex
	m          []*concurrentMutex
	consistent *hash.Consistent
}

// newMutex creates a new concurrent mutex.
func newMutex() mutex {
	mu := mutex{
		m:          make([]*concurrentMutex, nMutex),
		consistent: hash.InitConsistent(int(nMutex), int(nMutex)),
	}

	for i := 0; i < nMutex; i++ {
		mu.m[i] = &concurrentMutex{}
	}

	return mu
}

// getMutex returns mutex under given contract key
func (mu *mutex) getMutex(contract uint64) *concurrentMutex {
	mu.RLock()
	defer mu.RUnlock()
	return mu.m[mu.consistent.FindBlock(contract)]
}
