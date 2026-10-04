package internal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/backup"
)

type fakeBucket struct {
	mu      sync.Mutex
	objects map[string]string
	down    bool
}

func (f *fakeBucket) Put(_ context.Context, key string, b []byte, _ backup.Retention) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return errors.New("bucket unreachable")
	}
	f.objects[key] = string(b)
	return nil
}

func (f *fakeBucket) JournalKey(node string, t time.Time) string {
	return "c/journal/" + node + "-" + t.Format(time.RFC3339Nano)
}

// TestSecurityJournalSurvivesRestart checks that a security change waits on
// the node until it is uploaded: the bucket is down, the node restarts, and
// the next upload after takes it; the lag says how old it is meanwhile.
func TestSecurityJournalSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	bucket := &fakeBucket{objects: map[string]string{}, down: true}
	j, err := newSecurityJournal(dir, "n1", bucket)
	if err != nil {
		t.Fatal(err)
	}
	j.record(journalEntry{Security: map[uint32]*ContractState{7: {Revoked: map[uint64]int64{42: 0}}}})
	j.flush() // the bucket is down: kept
	if len(bucket.objects) != 0 || j.oldest.Load() == 0 || j.failures.Load() != 1 {
		t.Fatalf("with the bucket down: %d objects, oldest %d, failures %d", len(bucket.objects), j.oldest.Load(), j.failures.Load())
	}
	j.record(journalEntry{Security: map[uint32]*ContractState{7: {NotBefore: 1700000000}}})

	// The node restarts.
	bucket.down = false
	j2, err := newSecurityJournal(dir, "n1", bucket)
	if err != nil {
		t.Fatal(err)
	}
	if j2.oldest.Load() == 0 {
		t.Error("after a restart, the journal doesn't know it has lines to upload")
	}
	j2.flush()
	var all string
	for _, v := range bucket.objects {
		all += v
	}
	if !strings.Contains(all, `"42":0`) || !strings.Contains(all, `"not_before":1700000000`) {
		t.Errorf("uploaded %q", all)
	}
	if j2.oldest.Load() != 0 {
		t.Error("lines left after every batch was uploaded")
	}
	left, _ := filepath.Glob(filepath.Join(dir, "*.jsonl"))
	if len(left) != 0 {
		t.Errorf("files left: %v", left)
	}
}

func TestReplayJournalRejectsGarbage(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "a.jsonl"), []byte("not json\n"), 0600)
	if err := ReplayJournal(dir); err == nil {
		t.Error("a journal that isn't JSON replayed")
	}
}

// TestParseRun checks that only a run id makes a run, and that the run used
// in paths is formatted anew from its time.
func TestParseRun(t *testing.T) {
	if run, at, ok := parseRun("20261004T213000Z"); !ok || run != "20261004T213000Z" || at.Hour() != 21 {
		t.Errorf("a run id: %q %v %v", run, at, ok)
	}
	for _, bad := range []string{"", "../../etc", "20261004T213000Z/..", "20261304T213000Z", "2026-10-04"} {
		if _, _, ok := parseRun(bad); ok {
			t.Errorf("%q taken for a run id", bad)
		}
	}
}
