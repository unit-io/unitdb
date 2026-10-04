package internal

import (
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
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

// A backup run's id: when it began, in UTC. The job sends it with each of
// the run's checkpoints (POST /_checkpoint?run=<id>), which are named
// ckpt-<run>-<node>, so that a restore takes every node from one run.
var runID = regexp.MustCompile(`^[0-9]{8}T[0-9]{6}Z$`)

// runLayout is the format of a run id.
const runLayout = "20060102T150405Z"

// parseRun returns the run id raw names, as a new string formatted from
// the time it parses to, and when the run began: only that string goes into
// a path, never what a request sent. ok is false if raw isn't a run id.
func parseRun(raw string) (run string, at time.Time, ok bool) {
	if !runID.MatchString(raw) {
		return "", time.Time{}, false
	}
	at, err := time.Parse(runLayout, raw)
	if err != nil {
		return "", time.Time{}, false
	}
	return at.UTC().Format(runLayout), at, true
}

// maxManifest is the largest manifest a node keeps.
const maxManifest = 1 << 20

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
	var run string
	if raw := r.URL.Query().Get("run"); raw != "" {
		var ok bool
		if run, _, ok = parseRun(raw); !ok {
			http.Error(w, "a run id is a UTC time, as 20060102T150405Z", http.StatusBadRequest)
			return
		}
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
	name := checkpointPrefix + now.Format("20060102T150405Z")
	if run != "" {
		name = runCheckpointName(run)
	}
	dst := filepath.Join(c.dir, name)
	described := describeCheckpoint(now)
	described.Run = run
	start := time.Now()
	var info store.CheckpointInfo
	var err error
	// A run's checkpoint holds the run's canary, for the restore test
	// to read back.
	if run != "" {
		if err = store.WriteCanary(run); err != nil {
			err = fmt.Errorf("store checkpoint: the canary: %v", err)
		}
	}
	if err == nil {
		info, err = store.Checkpoint(dst, described)
	}
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
	json.NewEncoder(w).Encode(checkpointResp{Dir: dst, Seconds: took.Seconds(), CheckpointInfo: info})
}

// checkpointResp is the answer to POST /_checkpoint: where the checkpoint
// is, how long it took, and its checkpoint.json.
type checkpointResp struct {
	Dir     string  `json:"dir"`
	Seconds float64 `json:"seconds"`
	store.CheckpointInfo
}

// describeCheckpoint returns what this node knows of a checkpoint it takes
// at t: the store fills in the rest.
func describeCheckpoint(t time.Time) store.CheckpointInfo {
	info := store.CheckpointInfo{Time: t}
	if c := Globals.Cluster; c != nil {
		info.Node = c.thisNodeName
		info.RingVersion = c.getRingVersion()
	}
	if s := Globals.Service; s != nil && s.keys != nil {
		for id := range s.keys.StoreKeys() {
			info.KeyIDs = append(info.KeyIDs, int(id))
		}
		sort.Ints(info.KeyIDs)
	}
	return info
}

// handleCanary takes GET /_checkpoint/canary: the run ids of the store's
// canary message and record, for the restore test, which opens a
// checkpoint on a scratch server and reads them back.
func (c *checkpointer) handleCanary(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if !c.authorized(r) {
		http.Error(w, "the canary needs the checkpoint token", http.StatusUnauthorized)
		return
	}
	msg, rec, err := store.ReadCanary()
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"message": msg, "record": rec})
}

// runCheckpointName is the name of this node's checkpoint of run.
func runCheckpointName(run string) string {
	node := "server"
	if c := Globals.Cluster; c != nil && c.thisNodeName != "" {
		node = c.thisNodeName
	}
	return checkpointPrefix + run + "-" + node
}

// handleManifest takes PUT /_checkpoint/manifest?run=<id>: the backup job's
// manifest of the run, kept in this node's checkpoint of it, so that each
// node's copy of its checkpoint (a volume snapshot, an upload) names the
// whole run.
func (c *checkpointer) handleManifest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "PUT only", http.StatusMethodNotAllowed)
		return
	}
	if !c.authorized(r) {
		http.Error(w, "a manifest needs the checkpoint token", http.StatusUnauthorized)
		return
	}
	run, _, ok := parseRun(r.URL.Query().Get("run"))
	if !ok {
		http.Error(w, "a run id is a UTC time, as 20060102T150405Z", http.StatusBadRequest)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxManifest+1))
	if err != nil || len(body) > maxManifest || !json.Valid(body) {
		http.Error(w, "a manifest is JSON, up to 1 MiB", http.StatusBadRequest)
		return
	}
	dir := filepath.Join(c.dir, runCheckpointName(run))
	if fi, err := os.Stat(filepath.Join(dir, store.CheckpointInfoFile)); err != nil || fi.IsDir() {
		http.Error(w, "this node has no complete checkpoint of run "+run, http.StatusNotFound)
		return
	}
	if err := store.WriteManifest(dir, body); err != nil {
		log.Error("checkpoint", "manifest: "+err.Error())
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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
