//go:build lockcheck

package lockcheck

import (
	"bytes"
	"fmt"
	"runtime"
	"strconv"
	"sync"
)

// Enabled reports whether lock order is checked.
const Enabled = true

type held struct {
	rank int
	name string
}

var (
	mu    sync.Mutex
	holds = make(map[uint64][]held)
)

// Acquire records that the goroutine takes the lock name of rank, before
// it waits for it, and panics if the goroutine holds a lock of the same or
// a higher rank.
func Acquire(rank int, name string) {
	g := goid()
	mu.Lock()
	defer mu.Unlock()
	for _, h := range holds[g] {
		if h.rank >= rank {
			panic(fmt.Sprintf("lockcheck: %s (rank %d) taken holding %s (rank %d)", name, rank, h.name, h.rank))
		}
	}
	holds[g] = append(holds[g], held{rank, name})
}

// Release records that the goroutine let go of the lock of rank it holds.
// A lock released by another goroutine than took it is not tracked.
func Release(rank int) {
	g := goid()
	mu.Lock()
	defer mu.Unlock()
	hs := holds[g]
	for i := len(hs) - 1; i >= 0; i-- {
		if hs[i].rank == rank {
			hs = append(hs[:i], hs[i+1:]...)
			break
		}
	}
	if len(hs) == 0 {
		delete(holds, g)
		return
	}
	holds[g] = hs
}

// goid returns the ID of the calling goroutine, from its stack trace.
func goid() uint64 {
	var buf [64]byte
	b := buf[:runtime.Stack(buf[:], false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	if i := bytes.IndexByte(b, ' '); i > 0 {
		b = b[:i]
	}
	id, _ := strconv.ParseUint(string(b), 10, 64)
	return id
}
