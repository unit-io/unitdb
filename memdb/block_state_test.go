package memdb

import (
	"testing"

	"github.com/unit-io/bpool"
)

func panics(f func()) (did bool) {
	defer func() { did = recover() != nil }()
	f()
	return false
}

// TestBlockLifecycle checks a block goes live, released, gone, only: a
// block released twice returned its buffer to the pool twice.
func TestBlockLifecycle(t *testing.T) {
	pool := bpool.NewBufferPool(1<<20, nil)
	defer pool.Done()
	b := &_Block{data: pool.Get(), records: make(map[_Key]int64)}
	if panics(func() { b.setState(blockGone) }) == false {
		t.Error("a live block made gone")
	}
	b.setState(blockReleased)
	b.free(pool)
	if !panics(func() { b.setState(blockReleased) }) {
		t.Error("a released block released again")
	}
	if !panics(func() { b.free(pool) }) {
		t.Error("a block freed twice")
	}
	b.setState(blockGone)
	if !panics(func() { b.setState(blockLive) }) {
		t.Error("a gone block made live")
	}
}
