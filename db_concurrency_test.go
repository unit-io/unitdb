package unitdb

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentGet reads synced topics from several goroutines while others
// write and sync; run it with -race. Reads of synced entries go through the
// block reader the DB shares between readers.
func TestConcurrentGet(t *testing.T) {
	path := t.TempDir() + "/db"
	db, err := Open(path, nil, WithMutable())
	if err != nil {
		t.Fatal(err)
	}

	const topics, perTopic = 8, 50
	topic := func(i int) []byte { return []byte(fmt.Sprintf("conc.t%d", i)) }
	for i := 0; i < topics; i++ {
		for j := 0; j < perTopic; j++ {
			if err := db.Put(topic(i), []byte(fmt.Sprintf("%d-%d", i, j))); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	// Reopen so that the reads come from the files, not the memdb.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if db, err = Open(path, nil, WithMutable()); err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var wg sync.WaitGroup
	errs := make(chan error, topics*2)
	for i := 0; i < topics; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				got, err := db.Get(NewQuery(topic(i)).WithLimit(perTopic))
				if err != nil {
					errs <- err
					return
				}
				if len(got) < perTopic {
					errs <- fmt.Errorf("topic %d: %d of %d messages", i, len(got), perTopic)
					return
				}
			}
		}(i)
		go func(i int) {
			defer wg.Done()
			for n := 0; n < 20; n++ {
				if err := db.Put([]byte(fmt.Sprintf("conc.w%d", i)), []byte("w")); err != nil {
					errs <- err
					return
				}
				if n%5 == 0 {
					if err := db.Sync(); err != nil {
						errs <- err
						return
					}
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}
