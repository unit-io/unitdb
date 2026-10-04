package internal

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
)

// Checkpoints: a copy of the store that opens as it was at one moment
// (store.Checkpoint), taken on request at POST /_checkpoint on the monitor
// port, into CHECKPOINT_DIR, and kept there for a volume snapshot or a copy
// elsewhere.
// Restoring is starting a server with -db_path at the checkpoint, and the
// same keyring.
//
// The monitor port has no other authentication, and a checkpoint holds
// writes back for up to about 1.5 s, so:
//   - it's off unless CHECKPOINT_DIR and CHECKPOINT_TOKEN are both set;
//   - a request needs "Authorization: Bearer <CHECKPOINT_TOKEN>";
//   - the server names the checkpoint's directory, never the request;
//   - one runs at a time, and one per CHECKPOINT_MIN_MINUTES (10);
//   - the newest CHECKPOINT_KEEP (3) are kept, older ones removed.

const checkpointPrefix = "ckpt-"

type checkpointer struct {
	dir      string
	token    string
	keep     int
	minEvery time.Duration

	mu   sync.Mutex // held while one runs
	last time.Time  // when the last one began, under mu

	lastOK   atomic.Int64 // unix seconds of the last success; 0 before any
	failures atomic.Int64
	seconds  atomic.Int64 // milliseconds the last one took
}

// newCheckpointerFromEnv returns the checkpointer the environment asks for,
// or nil when checkpoints are off.
func newCheckpointerFromEnv() *checkpointer {
	dir, token := os.Getenv("CHECKPOINT_DIR"), os.Getenv("CHECKPOINT_TOKEN")
	if dir == "" || token == "" {
		return nil
	}
	keep, _ := strconv.Atoi(os.Getenv("CHECKPOINT_KEEP"))
	if keep < 1 {
		keep = 3
	}
	mins, _ := strconv.Atoi(os.Getenv("CHECKPOINT_MIN_MINUTES"))
	if mins < 0 || os.Getenv("CHECKPOINT_MIN_MINUTES") == "" {
		mins = 10
	}
	return &checkpointer{dir: dir, token: token, keep: keep, minEvery: time.Duration(mins) * time.Minute}
}

func (c *checkpointer) authorized(r *http.Request) bool {
	got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	return subtle.ConstantTimeCompare([]byte(got), []byte(c.token)) == 1
}

func (c *checkpointer) handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !c.authorized(r) {
		http.Error(w, "a checkpoint needs its token", http.StatusUnauthorized)
		return
	}
	if !c.mu.TryLock() {
		http.Error(w, "a checkpoint is running", http.StatusConflict)
		return
	}
	defer c.mu.Unlock()
	now := time.Now().UTC()
	if !c.last.IsZero() && now.Sub(c.last) < c.minEvery {
		http.Error(w, fmt.Sprintf("one checkpoint per %s", c.minEvery), http.StatusTooManyRequests)
		return
	}
	c.last = now
	dst := filepath.Join(c.dir, checkpointPrefix+now.Format("20060102T150405Z"))
	start := time.Now()
	err := store.Checkpoint(dst)
	took := time.Since(start)
	c.seconds.Store(took.Milliseconds())
	if err != nil {
		c.failures.Add(1)
		os.RemoveAll(dst)
		log.Error("checkpoint", err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c.lastOK.Store(now.Unix())
	log.Info("checkpoint", fmt.Sprintf("checkpoint %s in %s", dst, took.Round(time.Millisecond)))
	c.prune()
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"dir":     dst,
		"seconds": took.Seconds(),
	})
}

// prune removes all but the newest keep checkpoints.
func (c *checkpointer) prune() {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), checkpointPrefix) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names) // the names sort by time
	for len(names) > c.keep {
		if err := os.RemoveAll(filepath.Join(c.dir, names[0])); err != nil {
			log.Error("checkpoint", "removing an old checkpoint: "+err.Error())
		}
		names = names[1:]
	}
}

func (c *checkpointer) writeMetrics(m *metricsWriter) {
	m.one("unitdb_checkpoint_last_success_timestamp_seconds", "gauge",
		"When the last checkpoint succeeded, in unix seconds; 0 before any.", float64(c.lastOK.Load()))
	m.one("unitdb_checkpoint_failures_total", "counter", "Checkpoints that failed.", float64(c.failures.Load()))
	m.one("unitdb_checkpoint_duration_seconds", "gauge",
		"How long the last checkpoint took, writes held back meanwhile.", float64(c.seconds.Load())/1000)
}
