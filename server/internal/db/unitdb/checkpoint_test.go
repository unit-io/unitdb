package adapter

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

const testConfig = `{"mem_size": 16777216}`

func openAdapter(t *testing.T, dir string) *adapter {
	t.Helper()
	a := &adapter{}
	if err := a.Open(dir, testConfig, false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { a.Close() })
	return a
}

// TestCheckpoint checks that a checkpoint taken while writes go on opens as
// a store with every message and record written before it, and that it
// refuses a directory that isn't empty.
func TestCheckpoint(t *testing.T) {
	a := openAdapter(t, t.TempDir())
	const contract, topic = 7, "groups.checkpoint"
	for i := 0; i < 50; i++ {
		if err := a.Put(contract, topic, []byte(fmt.Sprintf("before-%d", i)), ""); err != nil {
			t.Fatal(err)
		}
		if err := a.PutMessage(uint64(1000+i), []byte(fmt.Sprintf("record-%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	// A record written twice keeps its last version.
	if err := a.PutMessage(1000, []byte("record-0-again")); err != nil {
		t.Fatal(err)
	}

	// Writes go on during the checkpoint.
	stop := make(chan struct{})
	var during atomic.Int64
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			a.Put(contract, topic, []byte(fmt.Sprintf("during-%d", i)), "")
			a.PutMessage(uint64(5000+i%100), []byte("during"))
			during.Add(1)
		}
	}()
	dst := filepath.Join(t.TempDir(), "checkpoint")
	err := a.Checkpoint(dst)
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if during.Load() == 0 {
		t.Log("no write ran during the checkpoint")
	}

	b := openAdapter(t, dst)
	msgs, err := b.Get(contract, topic, "")
	if err != nil {
		t.Fatal(err)
	}
	before := 0
	for _, m := range msgs {
		if bytes.HasPrefix(m, []byte("before-")) {
			before++
		}
	}
	if before != 50 {
		t.Errorf("the copy has %d of the 50 messages written before the checkpoint (%d in all)", before, len(msgs))
	}
	for i := 0; i < 50; i++ {
		want := fmt.Sprintf("record-%d", i)
		if i == 0 {
			want = "record-0-again"
		}
		got, err := b.GetMessage(uint64(1000 + i))
		if err != nil || string(got) != want {
			t.Errorf("record %d in the copy: %q (%v), want %q", i, got, err, want)
		}
	}

	if s := b.Stats(); s.Messages == 0 || s.DiskBytes == 0 || s.MemEntries == 0 || s.MemSize != 16777216 {
		t.Errorf("stats of the copy: %+v", s)
	}

	// A directory that isn't empty is refused.
	if err := a.Checkpoint(dst); err == nil || !strings.HasPrefix(err.Error(), "store checkpoint: ") {
		t.Errorf("a checkpoint into a directory that isn't empty: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dst, defaultDatabase, "unitdb.lock")); err != nil {
		// The copy's own DB made one when it opened; the checkpoint didn't
		// copy the source's.
		t.Logf("no lock file in the copy: %v", err)
	}
}
