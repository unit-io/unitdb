package e2e

// Copies off the cluster (docs/backup-restore.md), against
// MinIO: a run's checkpoints go to a bucket with Object Lock, encrypted;
// the security journal follows; a whole-cluster restore from the bucket
// replays the journal, so a client id revoked after the run stays revoked.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/unit-io/unitdb/server/internal/backup"
	"github.com/unit-io/unitdb/server/internal/types"
)

const (
	minioUser     = "e2e-minio"
	minioPassword = "e2e-minio-password"
	offsiteBucket = "unitdb-backups-e2e"
)

// startMinIO starts a MinIO server for the test, with a bucket that has
// Object Lock, and returns its endpoint.
func startMinIO(t *testing.T) string {
	t.Helper()
	bin, err := exec.LookPath("minio")
	if err != nil {
		t.Skip("minio isn't installed")
	}
	port, console := freePort(t), freePort(t)
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	cmd := exec.Command(bin, "server", t.TempDir(), "--address", fmt.Sprintf("127.0.0.1:%d", port), "--console-address", fmt.Sprintf("127.0.0.1:%d", console), "--quiet")
	cmd.Env = append(os.Environ(), "MINIO_ROOT_USER="+minioUser, "MINIO_ROOT_PASSWORD="+minioPassword)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cmd.Process.Kill()
		cmd.Wait()
	})
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		if resp, err := http.Get(endpoint + "/minio/health/live"); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("minio didn't start")
		}
	}
	// This test's own client: the root user, a reader.
	t.Setenv("AWS_ACCESS_KEY_ID", minioUser)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioPassword)
	st := offsiteStore(t, endpoint)
	if _, err := st.Client().CreateBucket(context.Background(), &s3.CreateBucketInput{Bucket: aws.String(offsiteBucket), ObjectLockEnabledForBucket: aws.Bool(true)}); err != nil {
		t.Fatal(err)
	}
	return endpoint
}

func offsiteStore(t *testing.T, endpoint string) *backup.Store {
	t.Helper()
	st, err := backup.NewStore(context.Background(), &backup.Config{Bucket: offsiteBucket, Region: "us-east-1", Endpoint: endpoint, Cluster: "e2e"})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// offsiteEnv is a node's environment for checkpoints, uploads to endpoint,
// and its security journal.
func offsiteEnv(t *testing.T, endpoint, recipient string) map[string][]string {
	env := checkpointEnv(t)
	for name := range env {
		env[name] = append(env[name],
			"BACKUP_S3_BUCKET="+offsiteBucket,
			"BACKUP_S3_ENDPOINT="+endpoint, "BACKUP_S3_REGION=us-east-1",
			"BACKUP_CLUSTER=e2e",
			"BACKUP_AGE_RECIPIENT="+recipient,
			"AWS_ACCESS_KEY_ID="+minioUser,
			"AWS_SECRET_ACCESS_KEY="+minioPassword,
			"SECURITY_JOURNAL_DIR="+t.TempDir(),
			"SECURITY_JOURNAL_EVERY=500ms",
		)
	}
	return env
}

// runBackupTool runs the backup command's tool with the reader's
// environment.
func runBackupTool(t *testing.T, bin, endpoint, identity string, args ...string) string {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(),
		"BACKUP_S3_BUCKET="+offsiteBucket, "BACKUP_S3_ENDPOINT="+endpoint, "BACKUP_S3_REGION=us-east-1", "BACKUP_CLUSTER=e2e",
		"AWS_ACCESS_KEY_ID="+minioUser, "AWS_SECRET_ACCESS_KEY="+minioPassword, "BACKUP_AGE_IDENTITY="+identity)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("backup %v: %v\n%s", args, err, out)
	}
	return string(out)
}

func TestBackupOffsiteRestoreReplaysJournal(t *testing.T) {
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
	contract := uint32(0x0e2edfdf)
	cid := newClientID(contract)
	var topics []string
	for _, n := range c.nodes {
		topics = append(topics, topicOwnedBy(n.name, contract, "groups.offsite.owned", names...))
	}
	publishAcked(t, c.nodes[0], cid, 20, topics...)
	time.Sleep(500 * time.Millisecond) // replication is asynchronous

	// The run, uploaded.
	m := runBackupUpload(t, bin, c.nodes)
	st := offsiteStore(t, endpoint)
	keys, err := st.List(context.Background(), "e2e/"+m.Run+"/")
	if err != nil || len(keys) != len(c.nodes)+1 {
		t.Fatalf("the run's objects: %v (%v)", keys, err)
	}

	// Locked: not even the bucket's root user deletes a version, and the
	// object isn't readable without the backup key.
	ckKey := "e2e/" + m.Run + "/" + c.nodes[0].name + ".tar.zst.age"
	head, err := st.Client().HeadObject(context.Background(), &s3.HeadObjectInput{Bucket: aws.String(offsiteBucket), Key: aws.String(ckKey)})
	if err != nil {
		t.Fatal(err)
	}
	if head.ObjectLockMode != s3types.ObjectLockModeCompliance || head.ObjectLockRetainUntilDate == nil || head.ObjectLockRetainUntilDate.Before(time.Now().Add(7*24*time.Hour)) {
		t.Errorf("%s: lock %q until %v", ckKey, head.ObjectLockMode, head.ObjectLockRetainUntilDate)
	}
	if _, err := st.Client().DeleteObject(context.Background(), &s3.DeleteObjectInput{Bucket: aws.String(offsiteBucket), Key: aws.String(ckKey), VersionId: head.VersionId}); err == nil {
		t.Errorf("deleted a locked version of %s", ckKey)
	}
	obj, err := st.Get(context.Background(), ckKey)
	if err != nil {
		t.Fatal(err)
	}
	var raw bytes.Buffer
	raw.ReadFrom(obj)
	obj.Close()
	if !bytes.HasPrefix(raw.Bytes(), []byte("age-encryption.org/v1")) {
		t.Errorf("%s isn't an age file: %.40q", ckKey, raw.Bytes())
	}

	// After the run: a client id revoked, and messages written; the
	// journal goes up.
	revokedID := newClientID(contract)
	uid, _, err := openClientID(revokedID)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := connectTo(t, c.nodes[1].tcpAddr, connectOpts{clientID: primaryClientID(contract)})
	if err != nil {
		t.Fatal(err)
	}
	revoke(t, pub, types.RevokeRequest{Uuid: strconv.FormatUint(uid.Uuid(), 10)})
	if _, err := connectTo(t, c.nodes[1].tcpAddr, connectOpts{clientID: revokedID}); err == nil {
		t.Fatal("control: the revoked client id connects")
	}
	publishBodies(t, c.nodes[0], cid, topics[0], bodies("lost", 5)...)
	for deadline := time.Now().Add(15 * time.Second); ; time.Sleep(250 * time.Millisecond) {
		if js, _ := st.List(context.Background(), "e2e/journal/"); len(js) > 0 && journalLagZero(t, c.nodes[1]) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the journal didn't go up")
		}
	}

	// The whole cluster lost; restored from the bucket, with the journal.
	for _, n := range c.nodes {
		n.stop()
	}
	restoreDir := t.TempDir()
	runBackupTool(t, bin, endpoint, id.String(), "fetch", "-run", m.Run, "-out", filepath.Join(restoreDir, "run"))
	journalDir := filepath.Join(restoreDir, "journal")
	out := runBackupTool(t, bin, endpoint, id.String(), "fetch-journal", "-since", m.Run, "-out", journalDir)
	if strings.HasPrefix(out, "0 ") {
		t.Fatalf("fetch-journal found nothing: %s", out)
	}
	// Control: the run predates the revocation. A copy of a node's
	// checkpoint, opened alone without the journal, takes the client id.
	copied := filepath.Join(t.TempDir(), "copy")
	if out, err := exec.Command("cp", "-R", filepath.Join(restoreDir, "run", c.nodes[1].name), copied).CombinedOutput(); err != nil {
		t.Fatalf("copy: %v %s", err, out)
	}
	lone := startServerWith(t, serverOpts{args: []string{"-db_path", copied}})
	if _, err := connectTo(t, lone.tcpAddr, connectOpts{clientID: revokedID}); err != nil {
		t.Fatalf("control: without the journal, the run's checkpoint refuses the client id revoked after it: %v", err)
	}
	lone.stop()

	for _, n := range c.nodes {
		if err := n.restartAt(filepath.Join(restoreDir, "run", n.name), "-restored", "-journal", journalDir); err != nil {
			t.Fatalf("restore %s: %v\nlogs:\n%s", n.name, err, n.logs.String())
		}
	}
	for _, n := range c.nodes {
		waitReadyz(t, n, 60*time.Second)
	}

	for _, n := range c.nodes {
		if _, err := connectTo(t, n.tcpAddr, connectOpts{clientID: revokedID}); err == nil {
			t.Errorf("the client id revoked after the run connects to %s after the restore", n.name)
		}
	}
	for _, n := range c.nodes {
		checkStored(t, n, cid, 20, topics...)
	}
	if got := relayCounts(t, c.nodes[0], cid, topics[0]); got["lost0"] != 0 {
		t.Errorf("a message written after the run came back: %v", got)
	}
}

// runBackupUpload runs a backup run that uploads, and returns its manifest.
func runBackupUpload(t *testing.T, bin string, nodes []*clusterNode) backupManifest {
	t.Helper()
	var urls []string
	for _, n := range nodes {
		urls = append(urls, "http://"+n.monitorAddr)
	}
	cmd := exec.Command(bin, "-nodes", strings.Join(urls, ","), "-tries", "2", "-retry_wait", "200ms", "-timeout", "30s", "-upload")
	cmd.Env = append(os.Environ(), "CHECKPOINT_TOKEN="+checkpointToken)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("backup -upload: %v\n%s\n%s", err, stdout.String(), stderr.String())
	}
	var m backupManifest
	if err := json.Unmarshal(stdout.Bytes(), &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// journalLagZero reports whether n's security journal has nothing left to
// upload.
func journalLagZero(t *testing.T, n *clusterNode) bool {
	_, metrics := monitorGet(t, n.server, "/_metrics")
	return strings.Contains(metrics, "unitdb_security_journal_lag_seconds 0\n")
}
