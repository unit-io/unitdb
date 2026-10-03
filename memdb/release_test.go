package memdb

import (
	"sync"
	"testing"
	"time"
)

// TestReleaseLogOnce releases each time block from two goroutines at once,
// as a delete that empties a block and a sync that wrote it can. Both used
// to look the block up before either removed it, and both freed it: the
// second free handed the pool a nil buffer and the process crashed.
func TestReleaseLogOnce(t *testing.T) {
	db, _ := openTestDB(t, WithLogInterval(5*time.Millisecond), WithTimeBlockInterval(20*time.Millisecond))
	for round := uint64(0); round < 50; round++ {
		putN(t, db, round*10, round*10+10)
		time.Sleep(60 * time.Millisecond) // the block is committed and a new one started

		var timeIDs []int64
		if err := db.BlockIterator(func(timeID int64, keys []uint64) (bool, error) {
			timeIDs = append(timeIDs, timeID)
			return false, nil
		}); err != nil {
			t.Fatal(err)
		}
		for _, timeID := range timeIDs {
			var wg sync.WaitGroup
			start := make(chan struct{})
			for i := 0; i < 2; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					db.Free(timeID)
				}()
			}
			close(start)
			wg.Wait()
		}
	}
}
