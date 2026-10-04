package memdb

import "testing"

// TestTimeMarkCountsLogs counts two logs of a block, as a Flush within a
// block duration makes: the block is done once both are written. add reset
// the count to one, and the block was done after the first.
func TestTimeMarkCountsLogs(t *testing.T) {
	tm := newTimeMark()
	done := func() bool { return len(tm.timeRefs(100)) == 1 }

	tm.add(10)
	tm.add(10)
	tm.release(10)
	if done() {
		t.Fatal("the block is done with one of its two logs written")
	}
	tm.release(10)
	if !done() {
		t.Fatal("the block is not done with both its logs written")
	}

	// Written again after it was done: not done until that log is.
	tm.add(10)
	if done() {
		t.Fatal("the block is done with a log of it not written")
	}
	tm.release(10)
	if !done() {
		t.Fatal("the block is not done with its last log written")
	}
}
