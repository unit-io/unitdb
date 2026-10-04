package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/unit-io/unitdb/server/internal/backup"
)

// verify is the weekly restore test (docs/backup-restore.md),
// run in its own namespace with a reader's identity, the only one besides
// named admins that may read the backup key and the escrowed keyring:
//
//	backup verify -run latest -server /unitdb -pushgateway http://...
//
// It downloads the newest complete run, decrypts it, and opens each node's
// checkpoint on a scratch server of its own (-db_path at it, no cluster,
// no clients), with the escrowed keyring. Each must:
//
//   - open, and answer /_readyz, its checks passing (the store probe
//     writes and reads back);
//   - hold what the manifest says it held, within -stats_tolerance (what
//     expired since);
//   - hold the run's canary, as a message and as a record, both opened
//     with the escrowed keyring.
//
// And the escrow must hold every key the run's manifest names. Only if all
// pass, it pushes unitdb_backup_restore_verified_timestamp_seconds to the
// Pushgateway; the alert on its age (deploy/kubernetes/backup-alerts.yaml)
// catches a test that fails, or doesn't run.
func verify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	runFlag := fs.String("run", "latest", "The run to test, or latest: the newest complete one.")
	server := fs.String("server", "/unitdb", "The unitdb server binary.")
	keyringFile := fs.String("keyring", "", "The keyring, as UNITDB_KEYRING holds it; the escrowed one if unset.")
	idFile := fs.String("identity", "", "The backup key's private half; BACKUP_AGE_IDENTITY, or Secrets Manager, if unset.")
	work := fs.String("work", "", "A scratch directory; a temporary one if unset.")
	pushgateway := fs.String("pushgateway", "", "The Pushgateway to push the result to, as http://pushgateway:9091.")
	tolerance := fs.Float64("stats_tolerance", 0.1, "How far a restored store's counts may be from the manifest's, as a fraction (what expired since).")
	startTimeout := fs.Duration("start_timeout", 5*time.Minute, "Longest a scratch server may take to be ready.")
	fs.Parse(args)

	ctx := context.Background()
	st, err := storeFromEnv(ctx)
	if err != nil {
		return err
	}
	run := *runFlag
	if run == "latest" {
		if run, err = latestCompleteRun(ctx, st); err != nil {
			return err
		}
	}
	id, err := identity(ctx, *idFile, st.Config)
	if err != nil {
		return err
	}
	keyring, err := escrowedKeyring(ctx, *keyringFile, st.Config)
	if err != nil {
		return err
	}
	dir := *work
	if dir == "" {
		if dir, err = os.MkdirTemp("", "unitdb-verify-"); err != nil {
			return err
		}
		defer os.RemoveAll(dir)
	}

	m, dirs, err := fetchRun(ctx, st, id, run, filepath.Join(dir, "run"))
	if err != nil {
		return err
	}
	var failures []string
	if missing := missingKeys(m, keyring); len(missing) > 0 {
		failures = append(failures, fmt.Sprintf("the escrow lacks key ids %v that the run names", missing))
	}
	for i, n := range m.Nodes {
		if err := checkNode(*server, dirs[i], keyring, run, n.Checkpoint.Stats.Messages, n.Checkpoint.Stats.MemEntries, *tolerance, *startTimeout); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", n.Checkpoint.Node, err))
		} else {
			fmt.Printf("run %s, %s: verified\n", run, n.Checkpoint.Node)
		}
	}
	if len(failures) > 0 {
		return fmt.Errorf("run %s:\n  %s", run, strings.Join(failures, "\n  "))
	}
	if *pushgateway != "" {
		if err := pushVerified(*pushgateway, st.Config.Cluster, run); err != nil {
			return fmt.Errorf("run %s verified, but the Pushgateway: %v", run, err)
		}
	}
	fmt.Printf("run %s verified: %d nodes\n", run, len(m.Nodes))
	return nil
}

var runDir = regexp.MustCompile(`^[0-9]{8}T[0-9]{6}Z$`)

// latestCompleteRun returns the newest run whose manifest says it is
// complete.
func latestCompleteRun(ctx context.Context, st *backup.Store) (string, error) {
	keys, err := st.List(ctx, st.Config.Cluster+"/")
	if err != nil {
		return "", err
	}
	seen := map[string]bool{}
	var runs []string
	for _, k := range keys {
		parts := strings.Split(strings.TrimPrefix(k, st.Config.Cluster+"/"), "/")
		if len(parts) == 2 && parts[1] == "manifest.json" && runDir.MatchString(parts[0]) && !seen[parts[0]] {
			seen[parts[0]] = true
			runs = append(runs, parts[0])
		}
	}
	sort.Sort(sort.Reverse(sort.StringSlice(runs)))
	for _, run := range runs {
		if m, err := readManifest(ctx, st, run); err == nil && m.Complete {
			return run, nil
		}
	}
	return "", errors.New("no complete run in the bucket")
}

// escrowedKeyring returns the keyring to open the checkpoints with: the
// file's, or the escrowed one.
func escrowedKeyring(ctx context.Context, file string, cfg *backup.Config) (string, error) {
	if file != "" {
		b, err := os.ReadFile(file)
		return string(b), err
	}
	ss, err := backup.NewAWSSecrets(ctx, cfg.Region)
	if err != nil {
		return "", err
	}
	v, found, err := ss.Get(ctx, backup.KeyringSecret(cfg.Cluster))
	if err != nil {
		return "", err
	}
	if !found {
		return "", fmt.Errorf("no %s in Secrets Manager", backup.KeyringSecret(cfg.Cluster))
	}
	return v, nil
}

// missingKeys returns the key ids the run's checkpoints name that the
// keyring lacks.
func missingKeys(m *runManifest, keyring string) []int {
	have, err := backup.EscrowedKeyIDs(keyring)
	if err != nil {
		return []int{-1}
	}
	var missing []int
	seen := map[int]bool{}
	for _, n := range m.Nodes {
		for _, id := range n.Checkpoint.KeyIDs {
			if !have[id] && !seen[id] {
				seen[id] = true
				missing = append(missing, id)
			}
		}
	}
	return missing
}

// checkNode opens one node's checkpoint on a scratch server, and checks it.
func checkNode(server, dbPath, keyring, run string, messages, memEntries int64, tolerance float64, startTimeout time.Duration) error {
	ports := make([]int, 3)
	for i := range ports {
		p, err := freePort()
		if err != nil {
			return err
		}
		ports[i] = p
	}
	scratch := filepath.Dir(dbPath)
	node := filepath.Base(dbPath)
	conf := filepath.Join(scratch, node+".conf")
	if err := os.WriteFile(conf, []byte(fmt.Sprintf(`{
  "listen": "127.0.0.1:%d",
  "grpc_listen": "127.0.0.1:%d",
  "monitor_listen": "127.0.0.1:%d",
  "logging_level": "Error",
  "encryption_config": {"identifier": "local", "sealed": false, "timestamp": 1522325758},
  "cluster_config": {"self": ""},
  "store_config": {"reset": false, "adapters": {"unitdb": {"database": "unitdb", "mem_size": 500000000}}}
}`, ports[0], ports[1], ports[2])), 0600); err != nil {
		return err
	}
	tokenBytes := make([]byte, 16)
	rand.Read(tokenBytes)
	token := hex.EncodeToString(tokenBytes)
	// The server reads -config from beside its binary: the path relative to it.
	confArg, err := filepath.Rel(filepath.Dir(server), conf)
	if err != nil {
		return err
	}
	cmd := exec.Command(server, "-config", confArg, "-db_path", dbPath)
	// Only what the scratch server needs: no bucket, no cluster.
	cmd.Env = []string{
		"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"),
		"UNITDB_KEYRING=" + keyring,
		"CHECKPOINT_DIR=" + filepath.Join(scratch, node+"-checkpoints"),
		"CHECKPOINT_TOKEN=" + token,
	}
	logs := &strings.Builder{}
	cmd.Stdout, cmd.Stderr = logs, logs
	if err := cmd.Start(); err != nil {
		return err
	}
	exited := make(chan struct{})
	go func() { cmd.Wait(); close(exited) }()
	defer func() {
		cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-exited:
		case <-time.After(30 * time.Second):
			cmd.Process.Kill()
			<-exited
		}
	}()
	monitor := fmt.Sprintf("http://127.0.0.1:%d", ports[2])
	fail := func(format string, a ...interface{}) error {
		return fmt.Errorf(format+"\n    server logs: %.2000s", append(a, logs.String())...)
	}

	// It opens, and is ready: its checks, the store probe among them, pass.
	deadline := time.Now().Add(startTimeout)
	for {
		select {
		case <-exited:
			return fail("the scratch server exited")
		default:
		}
		if code, _, _ := httpGet(monitor+"/_readyz", ""); code == http.StatusOK {
			break
		}
		if time.Now().After(deadline) {
			_, status, _ := httpGet(monitor+"/_status", "")
			return fail("not ready after %s: %s", startTimeout, status)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if code, status, err := httpGet(monitor+"/_status", ""); err != nil || code != http.StatusOK {
		return fail("/_status %d: %s %v", code, status, err)
	}

	// It holds what it held.
	_, metrics, err := httpGet(monitor+"/_metrics", "")
	if err != nil {
		return fail("/_metrics: %v", err)
	}
	for _, c := range []struct {
		name string
		want int64
	}{{"unitdb_store_messages", messages}, {"unitdb_store_mem_entries", memEntries}} {
		got, ok := metricValue(metrics, c.name)
		if !ok {
			return fail("no %s", c.name)
		}
		if slack := math.Max(tolerance*float64(c.want), 5); math.Abs(got-float64(c.want)) > slack {
			return fail("%s is %v, the manifest says %d", c.name, got, c.want)
		}
	}

	// Its canary is the run's.
	code, body, err := httpGet(monitor+"/_checkpoint/canary", token)
	if err != nil || code != http.StatusOK {
		return fail("the canary: %d %s %v", code, body, err)
	}
	var canary struct{ Message, Record string }
	if err := json.Unmarshal([]byte(body), &canary); err != nil || canary.Message != run || canary.Record != run {
		return fail("the canary is %s, not run %s's", body, run)
	}
	return nil
}

func httpGet(url, token string) (int, string, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return 0, "", err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, string(b), nil
}

// metricValue returns an unlabelled metric's value from a /_metrics page.
func metricValue(page, name string) (float64, bool) {
	for _, line := range strings.Split(page, "\n") {
		if strings.HasPrefix(line, name+" ") {
			v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, name+" ")), 64)
			return v, err == nil
		}
	}
	return 0, false
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// pushVerified pushes the test's success to the Pushgateway, under the
// cluster's name.
func pushVerified(gateway, cluster, run string) error {
	runAt, _ := time.Parse("20060102T150405Z", run)
	body := fmt.Sprintf(`# TYPE unitdb_backup_restore_verified_timestamp_seconds gauge
# HELP unitdb_backup_restore_verified_timestamp_seconds When the weekly restore test last verified a run, in unix seconds.
unitdb_backup_restore_verified_timestamp_seconds %d
# TYPE unitdb_backup_restore_verified_run_timestamp_seconds gauge
# HELP unitdb_backup_restore_verified_run_timestamp_seconds When the run it verified began, in unix seconds.
unitdb_backup_restore_verified_run_timestamp_seconds %d
`, time.Now().Unix(), runAt.Unix())
	url := strings.TrimRight(gateway, "/") + "/metrics/job/unitdb_backup_restore_test/cluster/" + cluster
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "text/plain; version=0.0.4")
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
		return fmt.Errorf("%s: %s", resp.Status, b)
	}
	return nil
}
