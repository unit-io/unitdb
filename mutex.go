package tracedb

import (
	"math"
	"sync"

	"github.com/unit-io/tracedb/hash"
)

const (
	//MaxMutex support for sharding
	maxMutex = math.MaxUint32 / 4096
	nMutex   = 16
)

type concurrentMutex struct {
	sync.RWMutex // Read Write mutex, guards access to internal request.
}

// mutex mutex to lock/unlock on topic based on prefix and not lock/unlock db
type mutex struct {
	m          []*concurrentMutex
	consistent *hash.Consistent
}

// newMutex creates a new concurrent mutex.
func newMutex() mutex {
	mu := mutex{
		m:          make([]*concurrentMutex, nMutex),
		consistent: hash.InitConsistent(int(maxMutex), int(nMutex)),
	}

	for i := 0; i < nMutex; i++ {
		mu.m[i] = &concurrentMutex{}
	}

	return mu
}

// getMutex returns mutex under given prefix key
func (mu *mutex) getMutex(prefix uint64) *concurrentMutex {
	return mu.m[mu.consistent.FindBlock(prefix)]
}
