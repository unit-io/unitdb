package internal

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"filippo.io/age"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/backup"
	"github.com/unit-io/unitdb/server/internal/store"
)

// Copies off the cluster (docs/backup-restore.md). A node
// uploads what only it has, with an identity that may only put objects
// (internal/backup):
//
//   - its checkpoint of a backup run, when the backup job asks
//     (POST /_checkpoint/upload?run=<id>): archived, compressed and
//     encrypted as it streams, with the run's manifest;
//   - the security journal: each change of the security state this node
//     makes (a revoked client id or topic key, a contract's not-before
//     time), as a JSON line, kept on its volume before the change takes
//     effect and uploaded in a batch every 10 s. A restore from a run replays the journal since
//     (-restored -journal <dir>), so that nothing revoked after the run comes
//     back.
//
// Nodes never read the bucket: the journal to replay is downloaded by an
// operator, with a reader's identity (the backup command's fetch-journal).

// journalEvery is how often the journal is uploaded: 10 s, or
// SECURITY_JOURNAL_EVERY (a duration), for tests.
var journalEvery = func() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("SECURITY_JOURNAL_EVERY")); err == nil && d > 0 {
		return d
	}
	return 10 * time.Second
}()

// offsite is this node's link to the bucket; nil fields when it has none.
var offsite struct {
	store     *backup.Store
	recipient age.Recipient
	node      string
	uploading sync.Mutex // one checkpoint upload at a time

	lastUpload     atomic.Int64 // unix seconds of the last checkpoint upload
	uploadFailures atomic.Int64
	uploadMillis   atomic.Int64
}

// journal is this node's security journal, or nil.
var journal atomic.Pointer[securityJournal]

// InitOffsite reads where backups go (BACKUP_S3_*), and starts the security
// journal when SECURITY_JOURNAL_DIR is set. Run it after ClusterInit, before
// the service starts: a change made before it isn't journalled.
func InitOffsite() error {
	node := "server"
	if c := Globals.Cluster; c != nil && c.thisNodeName != "" {
		node = c.thisNodeName
	}
	offsite.node = node
	cfg, err := backup.ConfigFromEnv()
	if err != nil {
		return err
	}
	if cfg != nil {
		if offsite.store, err = backup.NewStore(context.Background(), cfg); err != nil {
			return fmt.Errorf("backup: %v", err)
		}
		if cfg.Recipient == "" {
			return fmt.Errorf("backup: BACKUP_AGE_RECIPIENT, the backup key's public half, is needed with BACKUP_S3_BUCKET")
		}
		if offsite.recipient, err = backup.ParseRecipient(cfg.Recipient); err != nil {
			return err
		}
	}
	if dir := os.Getenv("SECURITY_JOURNAL_DIR"); dir != "" {
		var up journalUploader
		if offsite.store != nil {
			up = offsite.store
		}
		j, err := newSecurityJournal(dir, node, up)
		if err != nil {
			return err
		}
		if offsite.store == nil {
			log.ErrLogger.Warn().Str("context", "InitOffsite").Msg("SECURITY_JOURNAL_DIR is set, but no bucket: the journal is kept on this node only")
		}
		journal.Store(j)
		go j.run()
	}
	return nil
}

// handleUpload takes POST /_checkpoint/upload?run=<id>: this node's
// checkpoint of the run goes to the bucket, with the run's manifest.
func (c *checkpointer) handleUpload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !c.authorized(r) {
		http.Error(w, "an upload needs the checkpoint token", http.StatusUnauthorized)
		return
	}
	run, runAt, ok := parseRun(r.URL.Query().Get("run"))
	if !ok {
		http.Error(w, "a run id is a UTC time, as 20060102T150405Z", http.StatusBadRequest)
		return
	}
	if offsite.store == nil {
		http.Error(w, "this node has no bucket to upload to (BACKUP_S3_BUCKET)", http.StatusConflict)
		return
	}
	dir := filepath.Join(c.dir, runCheckpointName(run))
	if _, err := os.Stat(filepath.Join(dir, store.CheckpointInfoFile)); err != nil {
		http.Error(w, "this node has no complete checkpoint of run "+run, http.StatusNotFound)
		return
	}
	if !offsite.uploading.TryLock() {
		http.Error(w, "an upload is running", http.StatusConflict)
		return
	}
	defer offsite.uploading.Unlock()

	start := time.Now()
	cfg := offsite.store.Config
	key := cfg.CheckpointKey(run, offsite.node)
	ret := backup.RunRetention(runAt)
	err := uploadCheckpoint(r.Context(), dir, key, ret)
	if err == nil {
		if b, rerr := os.ReadFile(filepath.Join(dir, store.ManifestFile)); rerr == nil {
			err = offsite.store.Put(r.Context(), cfg.ManifestKey(run), b, ret)
		}
	}
	took := time.Since(start)
	offsite.uploadMillis.Store(took.Milliseconds())
	if err != nil {
		offsite.uploadFailures.Add(1)
		log.ErrLogger.Error().Err(err).Str("context", "checkpoint.upload").Str("run", run).Msg("upload failed")
		http.Error(w, "upload: "+err.Error(), http.StatusBadGateway)
		return
	}
	offsite.lastUpload.Store(time.Now().Unix())
	log.ErrLogger.Info().Str("context", "checkpoint.upload").Str("key", key).Dur("took", took).Msg("uploaded")
	w.Header().Set("content-type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"key": key, "tier": ret.Tier, "retain_until": ret.Until, "seconds": took.Seconds()})
}

// uploadCheckpoint streams dir to key: archived, compressed and encrypted
// as the upload takes it, so nothing but the checkpoint is written here.
func uploadCheckpoint(ctx context.Context, dir, key string, ret backup.Retention) error {
	pr, pw := io.Pipe()
	go func() {
		pw.CloseWithError(backup.WriteArchive(pw, dir, offsite.recipient))
	}()
	err := offsite.store.Upload(ctx, key, pr, ret)
	pr.CloseWithError(err) // stops the archive if the upload failed
	return err
}

// journalEntry is one line of the security journal. Replayed, each merges
// into the state as the change did: in any order, and as often.
type journalEntry struct {
	Time time.Time `json:"time"`
	Node string    `json:"node"`
	// Security is a change of the security state (revocations.apply).
	Security map[uint32]*ContractState `json:"security,omitempty"`
}

const (
	journalPending = "pending.jsonl"
	journalBatch   = "batch-"
)

// securityJournal keeps the journal's lines on this node until they are
// uploaded: pending.jsonl takes them as they come, and becomes a batch
// file to upload every journalEvery. A batch is removed once in the bucket,
// so a restart uploads what was left.
type securityJournal struct {
	dir   string
	node  string
	store journalUploader // nil: kept on this node only

	mu sync.Mutex // appends to pending.jsonl, and its rotation

	oldest       atomic.Int64 // unix nanos of the oldest line not uploaded; 0 if none
	lastUpload   atomic.Int64 // unix seconds
	failures     atomic.Int64
	recordErrors atomic.Int64
}

// journalUploader is where journal batches go: the bucket.
type journalUploader interface {
	Put(ctx context.Context, key string, b []byte, ret backup.Retention) error
	JournalKey(node string, t time.Time) string
}

func newSecurityJournal(dir, node string, st journalUploader) (*securityJournal, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, fmt.Errorf("security journal: %v", err)
	}
	j := &securityJournal{dir: dir, node: node, store: st}
	j.oldest.Store(j.oldestLeft())
	return j, nil
}

// record appends e to the journal, and fsyncs it, before the change it
// records takes effect for clients.
func (j *securityJournal) record(e journalEntry) {
	e.Time, e.Node = time.Now().UTC(), j.node
	b, err := json.Marshal(e)
	if err == nil {
		j.mu.Lock()
		err = appendSynced(filepath.Join(j.dir, journalPending), append(b, '\n'))
		if err == nil {
			j.oldest.CompareAndSwap(0, e.Time.UnixNano())
		}
		j.mu.Unlock()
	}
	if err != nil {
		j.recordErrors.Add(1)
		log.ErrLogger.Error().Err(err).Str("context", "securityJournal.record").Msg("a security change isn't in the journal")
	}
}

func appendSynced(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

func (j *securityJournal) run() {
	for {
		time.Sleep(journalEvery)
		j.flush()
	}
}

// flush turns the pending lines into a batch, and uploads the batches left,
// oldest first, until one fails.
func (j *securityJournal) flush() {
	j.mu.Lock()
	pending := filepath.Join(j.dir, journalPending)
	if fi, err := os.Stat(pending); err == nil && fi.Size() > 0 {
		batch := filepath.Join(j.dir, fmt.Sprintf("%s%020d.jsonl", journalBatch, time.Now().UnixNano()))
		if err := os.Rename(pending, batch); err != nil {
			log.ErrLogger.Error().Err(err).Str("context", "securityJournal.flush").Msg("unable to start a batch")
		}
		syncDir(j.dir)
	}
	j.mu.Unlock()
	if j.store == nil {
		return
	}
	for _, name := range j.batches() {
		path := filepath.Join(j.dir, name)
		b, err := os.ReadFile(path)
		if err != nil {
			j.failures.Add(1)
			break
		}
		now := time.Now()
		if err := j.store.Put(context.Background(), j.store.JournalKey(j.node, now), b, backup.JournalRetention(now)); err != nil {
			j.failures.Add(1)
			log.ErrLogger.Error().Err(err).Str("context", "securityJournal.flush").Msg("journal upload failed: kept here, tried again")
			break
		}
		os.Remove(path)
		j.lastUpload.Store(now.Unix())
	}
	j.mu.Lock()
	j.oldest.Store(j.oldestLeft())
	j.mu.Unlock()
}

// batches returns the batch files left, oldest first.
func (j *securityJournal) batches() []string {
	entries, _ := os.ReadDir(j.dir)
	var names []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), journalBatch) && strings.HasSuffix(e.Name(), ".jsonl") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

// oldestLeft returns when the oldest line not uploaded was written, in unix
// nanos, or 0.
func (j *securityJournal) oldestLeft() int64 {
	files := j.batches()
	files = append(files, journalPending)
	for _, name := range files {
		f, err := os.Open(filepath.Join(j.dir, name))
		if err != nil {
			continue
		}
		line, _ := bufio.NewReader(f).ReadBytes('\n')
		f.Close()
		var e journalEntry
		if json.Unmarshal(line, &e) == nil && !e.Time.IsZero() {
			return e.Time.UnixNano()
		}
	}
	return 0
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		d.Sync()
		d.Close()
	}
}

// journalSecurity records a change of the security state this node makes.
func journalSecurity(changes map[uint32]*ContractState) {
	if j := journal.Load(); j != nil {
		j.record(journalEntry{Security: changes})
	}
}

// ReplayJournal merges the journal under dir (the backup command's
// fetch-journal) into this node's security state: every change since the
// run a restore is from. Lines merge as the changes did, so lines
// older than the checkpoint, or replayed twice, change nothing. Run it
// after the service is made, before the cluster starts.
func ReplayJournal(dir string) error {
	var entries []journalEntry
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || !strings.HasSuffix(path, ".jsonl") {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 64*1024), 16<<20)
		for sc.Scan() {
			var e journalEntry
			if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
				return fmt.Errorf("%s: %v", path, err)
			}
			entries = append(entries, e)
		}
		return sc.Err()
	})
	if err != nil {
		return fmt.Errorf("journal replay: %v", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Time.Before(entries[j].Time) })
	security := 0
	for _, e := range entries {
		if e.Security == nil {
			continue
		}
		changed, err := Globals.Service.revocations.replay(e.Security)
		if err != nil {
			return fmt.Errorf("journal replay: %v", err)
		}
		if len(changed) > 0 {
			security++
		}
	}
	log.ErrLogger.Info().Str("context", "ReplayJournal").Int("lines", len(entries)).Int("security_changes", security).Msg("replayed the security journal")
	return nil
}

// writeOffsiteMetrics writes the uploads' and the journal's metrics.
func writeOffsiteMetrics(m *metricsWriter) {
	if offsite.store != nil {
		m.one("unitdb_backup_upload_last_success_timestamp_seconds", "gauge", "When this node last uploaded its checkpoint of a run, in unix seconds; 0 before any.", float64(offsite.lastUpload.Load()))
		m.one("unitdb_backup_upload_failures_total", "counter", "Checkpoint uploads that failed.", float64(offsite.uploadFailures.Load()))
		m.one("unitdb_backup_upload_duration_seconds", "gauge", "How long the last checkpoint upload took.", float64(offsite.uploadMillis.Load())/1000)
	}
	if j := journal.Load(); j != nil {
		lag := 0.0
		if o := j.oldest.Load(); o != 0 {
			lag = time.Since(time.Unix(0, o)).Seconds()
		}
		m.one("unitdb_security_journal_lag_seconds", "gauge", "Age of the oldest security change not in the bucket yet; 0 if none.", lag)
		m.one("unitdb_security_journal_upload_failures_total", "counter", "Journal uploads that failed (kept, and tried again).", float64(j.failures.Load()))
		m.one("unitdb_security_journal_record_errors_total", "counter", "Security changes that couldn't be written to the journal.", float64(j.recordErrors.Load()))
	}
}
