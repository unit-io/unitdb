package e2e

// The weekly restore test (docs/backup-restore.md): the
// backup command's verify opens each node's checkpoint of the newest run on
// a scratch server and checks it, and pushes its success; a corrupted
// archive, or an escrow without the run's key, fails it, and nothing is
// pushed.

import (
	"context"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/unit-io/unitdb/server/internal/backup"
)

func TestBackupRestoreTestVerifiesRun(t *testing.T) {
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
	cid := newClientID(0x0e2ee0e0)
	publishAcked(t, c.nodes[0], cid, 30, "groups.verify.a", "groups.verify.b")
	time.Sleep(500 * time.Millisecond)
	m := runBackupUpload(t, bin, c.nodes)

	// The Pushgateway, as the test sees it.
	var mu sync.Mutex
	var pushes []string
	gw := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		pushes = append(pushes, r.Method+" "+r.URL.Path+"\n"+string(b))
		mu.Unlock()
	}))
	defer gw.Close()
	keyringFile := filepath.Join(t.TempDir(), "keyring.json")
	os.WriteFile(keyringFile, []byte(`[{"id":0,"key":"`+base64.StdEncoding.EncodeToString([]byte(testKey))+`","use":"issue"}]`), 0600)
	verify := func(keyring string) (string, error) {
		cmd := exec.Command(bin, "verify", "-run", "latest", "-server", serverBinary(t), "-keyring", keyring, "-pushgateway", gw.URL, "-start_timeout", "60s")
		cmd.Env = append(os.Environ(),
			"BACKUP_S3_BUCKET="+offsiteBucket, "BACKUP_S3_ENDPOINT="+endpoint, "BACKUP_S3_REGION=us-east-1", "BACKUP_CLUSTER=e2e",
			"AWS_ACCESS_KEY_ID="+minioUser, "AWS_SECRET_ACCESS_KEY="+minioPassword, "BACKUP_AGE_IDENTITY="+id.String())
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	out, err := verify(keyringFile)
	if err != nil {
		t.Fatalf("verify: %v\n%s", err, out)
	}
	if !strings.Contains(out, "run "+m.Run+" verified: 3 nodes") {
		t.Errorf("verify said:\n%s", out)
	}
	mu.Lock()
	if len(pushes) != 1 || !strings.Contains(pushes[0], "PUT /metrics/job/unitdb_backup_restore_test/cluster/e2e") ||
		!strings.Contains(pushes[0], "unitdb_backup_restore_verified_timestamp_seconds ") {
		t.Errorf("pushed %q", pushes)
	}
	mu.Unlock()

	// An escrow without the run's key fails it.
	other := filepath.Join(t.TempDir(), "other.json")
	os.WriteFile(other, []byte(`[{"id":9,"key":"`+base64.StdEncoding.EncodeToString([]byte("abcdefghijabcdefghijabcdefghij12"))+`","use":"issue"}]`), 0600)
	if out, err := verify(other); err == nil || !strings.Contains(out, "lacks key ids [0]") {
		t.Errorf("verify with the wrong escrow: %v\n%s", err, out)
	}

	// A corrupted archive, put as the object's newest version, fails it.
	st := offsiteStore(t, endpoint)
	key := "e2e/" + m.Run + "/" + c.nodes[1].name + ".tar.zst.age"
	obj, err := st.Get(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(obj)
	obj.Close()
	b[len(b)/2] ^= 0xff
	if err := st.Put(context.Background(), key, b, backup.RunRetention(time.Now())); err != nil {
		t.Fatal(err)
	}
	if out, err := verify(keyringFile); err == nil || !strings.Contains(out, c.nodes[1].name+"'s checkpoint") {
		t.Errorf("verify of a corrupted archive: %v\n%s", err, out)
	}
	mu.Lock()
	if len(pushes) != 1 {
		t.Errorf("a failed test pushed: %d pushes", len(pushes))
	}
	mu.Unlock()
}
