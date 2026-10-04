package e2e

// The backup run (docs/backup-restore.md): the backup command
// checkpoints every node under one run id, and keeps the run's manifest in
// each node's checkpoint; a node that is down makes the run incomplete.

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// buildBackup builds the backup command, once per test.
func buildBackup(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "backup")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/backup")
	cmd.Dir = serverSourceDir(t)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build backup: %v\n%s", err, out)
	}
	return bin
}

type backupManifest struct {
	Run      string `json:"run"`
	Complete bool   `json:"complete"`
	Nodes    []struct {
		URL        string `json:"url"`
		Error      string `json:"error"`
		Checkpoint struct {
			Dir  string `json:"dir"`
			Node string `json:"node"`
			Run  string `json:"run"`
		} `json:"checkpoint"`
	} `json:"nodes"`
}

// runBackup runs the backup command against the nodes, and returns its
// manifest and whether it exited 0.
func runBackup(t *testing.T, bin string, nodes []*clusterNode) (backupManifest, bool) {
	t.Helper()
	var urls []string
	for _, n := range nodes {
		urls = append(urls, "http://"+n.monitorAddr)
	}
	cmd := exec.Command(bin, "-nodes", strings.Join(urls, ","), "-tries", "2", "-retry_wait", "200ms", "-timeout", "30s")
	cmd.Env = append(os.Environ(), "CHECKPOINT_TOKEN="+checkpointToken)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	var m backupManifest
	if jerr := json.Unmarshal(stdout.Bytes(), &m); jerr != nil {
		t.Fatalf("backup printed %q (%v), stderr:\n%s", stdout.String(), err, stderr.String())
	}
	t.Logf("backup run %s: complete %v, exit %v\n%s", m.Run, m.Complete, err, stderr.String())
	return m, err == nil
}

func TestBackupRunCheckpointsEveryNode(t *testing.T) {
	bin := buildBackup(t)
	c := startClusterWith(t, clusterOpts{env: checkpointEnv(t)}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	cid := newClientID(0x0e2e8c8c)
	publishAcked(t, c.nodes[0], cid, 10, "groups.run.x")

	m, ok := runBackup(t, bin, c.nodes)
	if !ok || !m.Complete || len(m.Nodes) != len(c.nodes) {
		t.Fatalf("a run of %d nodes: %+v", len(c.nodes), m)
	}
	for i, n := range m.Nodes {
		ck := n.Checkpoint
		if want := "ckpt-" + m.Run + "-" + c.nodes[i].name; filepath.Base(ck.Dir) != want || ck.Run != m.Run || ck.Node != c.nodes[i].name {
			t.Errorf("node %s: checkpoint %+v, want %s of run %s", c.nodes[i].name, ck, want, m.Run)
			continue
		}
		// Each node's checkpoint holds the whole run's manifest.
		b, err := os.ReadFile(filepath.Join(ck.Dir, "manifest.json"))
		var kept backupManifest
		if err != nil || json.Unmarshal(b, &kept) != nil || kept.Run != m.Run || len(kept.Nodes) != len(c.nodes) {
			t.Errorf("manifest in %s: %v %s", ck.Dir, err, b)
		}
		info, err := os.ReadFile(filepath.Join(ck.Dir, "checkpoint.json"))
		if err != nil || !strings.Contains(string(info), `"run": "`+m.Run+`"`) {
			t.Errorf("checkpoint.json in %s: %v %s", ck.Dir, err, info)
		}
	}

	// A node down: the others are checkpointed, the run is incomplete, and
	// the command fails, naming it.
	time.Sleep(1100 * time.Millisecond) // a new run id
	down := c.nodes[1]
	down.stop()
	m2, ok := runBackup(t, bin, c.nodes)
	if ok || m2.Complete {
		t.Fatalf("a run with %s down: exit ok %v, %+v", down.name, ok, m2)
	}
	for i, n := range m2.Nodes {
		if (i == 1) != (n.Error != "") {
			t.Errorf("node %s in the incomplete run: error %q", c.nodes[i].name, n.Error)
		}
	}
}

// TestRestoreWholeClusterFromRunID restores every node at its checkpoint of
// one backup run, with run ids: a node at another run's checkpoint is
// refused, however close in time.
func TestRestoreWholeClusterFromRunID(t *testing.T) {
	bin := buildBackup(t)
	c := startClusterWith(t, clusterOpts{env: checkpointEnv(t)}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	contract := uint32(0x0e2e9d9d)
	cid := newClientID(contract)
	var topics []string
	for _, n := range c.nodes {
		topics = append(topics, topicOwnedBy(n.name, contract, "groups.runid.owned", names...))
	}
	publishAcked(t, c.nodes[0], cid, 20, topics...)
	time.Sleep(500 * time.Millisecond) // replication is asynchronous

	first, ok := runBackup(t, bin, c.nodes)
	if !ok {
		t.Fatal("the first run is incomplete")
	}
	time.Sleep(1100 * time.Millisecond) // a new run id
	second, ok := runBackup(t, bin, c.nodes)
	if !ok {
		t.Fatal("the second run is incomplete")
	}
	for _, n := range c.nodes {
		n.stop()
	}
	last := len(c.nodes) - 1
	for i, n := range c.nodes[:last] {
		if err := n.restartAt(second.Nodes[i].Checkpoint.Dir); err != nil {
			t.Fatalf("start %s at run %s: %v\nlogs:\n%s", n.name, second.Run, err, n.logs.String())
		}
	}
	if logs := c.nodes[last].refusedAt(t, first.Nodes[last].Checkpoint.Dir); !strings.Contains(logs, "not the same backup run") {
		t.Errorf("a checkpoint of run %s among run %s's, it says:\n%s", first.Run, second.Run, logs)
	}
	if err := c.nodes[last].restartAt(second.Nodes[last].Checkpoint.Dir); err != nil {
		t.Fatalf("start %s at run %s: %v", c.nodes[last].name, second.Run, err)
	}
	for _, n := range c.nodes {
		waitReadyz(t, n, 30*time.Second)
	}
	for _, n := range c.nodes {
		checkStored(t, n, cid, 20, topics...)
	}
}
