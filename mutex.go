package tracedb

import (
	"sync"

	"github.com/unit-io/tracedb/hash"
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
		m:          make([]*concurrentMutex, nShards),
		consistent: hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
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
