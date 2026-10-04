package e2e

// Runbook B, rehearsed (docs/backup-restore.md): the whole cluster lost,
// restored from the bucket as the runbook says, step by step, and timed.
// The drill on staging (docs/backup-restore.md) runs the same
// steps on Kubernetes.

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/unit-io/unitdb/server/internal/types"
)

func TestRunbookWholeClusterRestore(t *testing.T) {
	endpoint := startMinIO(t)
	bin := buildBackup(t)
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	c := startClusterWith(t, clusterOpts{env: offsiteEnv(t, endpoint, id.Recipient().String())}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	contract := uint32(0x0e2ef1f1)
	cid := newClientID(contract)
	var topics []string
	for _, n := range c.nodes {
		topics = append(topics, topicOwnedBy(n.name, contract, "groups.runbook.owned", names...))
	}
	publishAcked(t, c.nodes[0], cid, 50, topics...)
	time.Sleep(500 * time.Millisecond)
	m := runBackupUpload(t, bin, c.nodes)
	// After the run: a revocation, which the journal keeps.
	revokedID := newClientID(contract)
	uid, _, _ := openClientID(revokedID)
	pub, err := connectTo(t, c.nodes[0].tcpAddr, connectOpts{clientID: primaryClientID(contract)})
	if err != nil {
		t.Fatal(err)
	}
	revoke(t, pub, types.RevokeRequest{Uuid: strconv.FormatUint(uid.Uuid(), 10)})
	for deadline := time.Now().Add(15 * time.Second); !journalLagZero(t, c.nodes[0]); time.Sleep(250 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the journal didn't go up")
		}
	}

	// Step 1: the cluster is lost.
	for _, n := range c.nodes {
		n.stop()
	}
	tool := func(args ...string) string {
		t.Helper()
		cmd := exec.Command(bin, args...)
		cmd.Env = append(os.Environ(),
			"BACKUP_S3_BUCKET="+offsiteBucket, "BACKUP_S3_ENDPOINT="+endpoint, "BACKUP_S3_REGION=us-east-1", "BACKUP_CLUSTER=e2e",
			"AWS_ACCESS_KEY_ID="+minioUser, "AWS_SECRET_ACCESS_KEY="+minioPassword, "BACKUP_AGE_IDENTITY="+id.String())
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("backup %v: %v\n%s", args, err, out)
		}
		return string(out)
	}
	start := time.Now()

	// Step 4: the newest complete run.
	runs := tool("runs")
	if first := strings.SplitN(runs, "\n", 2)[0]; first != m.Run+"\tcomplete" {
		t.Fatalf("backup runs: %q, want %s first, complete", runs, m.Run)
	}

	// Step 5: each node's new claim gets its checkpoint and the journal.
	claims := map[string]string{}
	for _, n := range c.nodes {
		claim := t.TempDir()
		claims[n.name] = claim
		tool("fetch", "-run="+m.Run, "-node="+n.name, "-out="+filepath.Join(claim, "unitdb"))
		tool("fetch-journal", "-since="+m.Run, "-out="+filepath.Join(claim, "restore-journal"))
	}
	fetched := time.Since(start)

	// Step 6: every node with -restored and -journal; ready once
	// reconciled.
	for _, n := range c.nodes {
		claim := claims[n.name]
		if err := n.restartAt(filepath.Join(claim, "unitdb"), "-restored", "-journal", filepath.Join(claim, "restore-journal")); err != nil {
			t.Fatalf("step 6, %s: %v\nlogs:\n%s", n.name, err, n.logs.String())
		}
	}
	for _, n := range c.nodes {
		waitReadyz(t, n, 60*time.Second)
	}
	ready := time.Since(start)
	for _, n := range c.nodes {
		if _, err := os.Stat(filepath.Join(claims[n.name], "unitdb", "restored-from.json")); err != nil {
			t.Errorf("step 6, %s: not marked restored: %v", n.name, err)
		}
	}

	// Step 7: the flags come out; the nodes restart as usual.
	for _, n := range c.nodes {
		if err := n.restartAt(filepath.Join(claims[n.name], "unitdb")); err != nil {
			t.Fatalf("step 7, %s: %v\nlogs:\n%s", n.name, err, n.logs.String())
		}
	}
	for _, n := range c.nodes {
		waitReadyz(t, n, 60*time.Second)
	}

	// Step 8: checked.
	for _, n := range c.nodes {
		checkStored(t, n, cid, 50, topics...)
		if _, err := connectTo(t, n.tcpAddr, connectOpts{clientID: revokedID}); err == nil {
			t.Errorf("step 8: the client id revoked after the run connects to %s", n.name)
		}
	}
	t.Logf("runbook B: fetched in %s, ready in %s (%d nodes, run %s)", fetched.Round(time.Millisecond), ready.Round(time.Millisecond), len(c.nodes), m.Run)
}
