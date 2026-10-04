package e2e

// Backups: a checkpoint taken through POST /_checkpoint while writes go on
// opens, on a second server with the same key, as the store was: the
// restore drill.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const checkpointToken = "e2e-checkpoint-token"

func checkpointPost(t *testing.T, s *server, token string) (int, string) {
	t.Helper()
	req, _ := http.NewRequest("POST", "http://"+s.monitorAddr+"/_checkpoint", nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(b))
}

// storedOn returns the messages stored on topic, relayed from s: at least n
// of them, and any more that come before the relay goes quiet. A topic can
// hold more than n (writes taken during a checkpoint), and the relay
// doesn't send them oldest first.
func storedOn(t *testing.T, s *server, cid, topic string, n int) map[int]string {
	t.Helper()
	sub, err := dial(context.Background(), s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.close()
	if _, err := sub.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}
	rid, err := sub.relay(topic, "24h")
	if err != nil {
		t.Fatal(err)
	}
	if !sub.waitAck(rid, 5*time.Second) {
		t.Fatalf("no relay ack")
	}
	got := map[int]string{}
	deadline := time.Now().Add(30 * time.Second)
	for {
		wait := time.Until(deadline)
		if len(got) >= n {
			wait = 500 * time.Millisecond
		}
		msg, ok := sub.waitPub(wait)
		if !ok {
			break
		}
		for _, m := range msg.Messages {
			if seq, body, ok := decodePayload(m.Payload); ok {
				got[seq] = string(body)
			}
		}
	}
	return got
}

func TestBackupCheckpointRestores(t *testing.T) {
	dir := t.TempDir()
	env := []string{"CHECKPOINT_DIR=" + dir, "CHECKPOINT_TOKEN=" + checkpointToken, "CHECKPOINT_MIN_MINUTES=0", "CHECKPOINT_KEEP=2"}
	s := startServerWith(t, serverOpts{env: env})
	ctx := context.Background()
	cid := newClientID(0x0e2eb4c7)
	topic := "groups.backup.x.message"

	pub, err := dial(ctx, s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.close()
	if _, err := pub.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}
	n := scaled(50)
	for i := 0; i < n; i++ {
		id, err := pub.publish(1, topic, encodePayload(i, fmt.Sprintf("m%d", i)), "24h")
		if err != nil {
			t.Fatal(err)
		}
		if !pub.waitAck(id, 5*time.Second) {
			t.Fatalf("publish %d: no RECEIPT", i)
		}
	}
	// A subscription made and dropped before the checkpoint: a delete.
	if _, err := pub.subscribe(1, topic); err == nil {
		pub.unsubscribe(topic)
	}

	// The token is needed, and writes go on while it runs.
	if code, _ := checkpointPost(t, s, ""); code != http.StatusUnauthorized {
		t.Errorf("no token: %d, want 401", code)
	}
	if code, _ := checkpointPost(t, s, "wrong"); code != http.StatusUnauthorized {
		t.Errorf("a wrong token: %d, want 401", code)
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := n; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := pub.publish(1, topic, encodePayload(i, fmt.Sprintf("m%d", i)), "24h"); err != nil {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	code, body := checkpointPost(t, s, checkpointToken)
	close(stop)
	wg.Wait()
	if code != http.StatusOK {
		t.Fatalf("checkpoint: %d %s\nlogs:\n%s", code, body, s.logs.String())
	}
	var ck struct {
		Dir     string  `json:"dir"`
		Seconds float64 `json:"seconds"`
	}
	if err := json.Unmarshal([]byte(body), &ck); err != nil || !strings.HasPrefix(ck.Dir, dir) {
		t.Fatalf("checkpoint answer %q", body)
	}
	t.Logf("checkpoint in %.2fs at %s", ck.Seconds, ck.Dir)

	// The metrics say so.
	if _, metrics := monitorGet(t, s, "/_metrics"); !strings.Contains(metrics, "unitdb_checkpoint_last_success_timestamp_seconds ") ||
		strings.Contains(metrics, "unitdb_checkpoint_last_success_timestamp_seconds 0\n") ||
		!strings.Contains(metrics, "unitdb_store_messages ") {
		t.Errorf("metrics after a checkpoint:\n%s", metrics)
	}

	// The drill: a second server on the checkpoint, with the same key.
	restored := startServerWith(t, serverOpts{args: []string{"-db_path", ck.Dir}})
	got := storedOn(t, restored, cid, topic, n)
	for i := 0; i < n; i++ {
		if want := fmt.Sprintf("m%d", i); got[i] != want {
			t.Fatalf("restored seq %d: %q, want %q (got %d messages)\nlogs:\n%s", i, got[i], want, len(got), restored.logs.String())
		}
	}
	t.Logf("restored %d messages, %d written during the checkpoint", len(got), len(got)-n)

	// Old checkpoints go: two are kept.
	for i := 0; i < 2; i++ {
		time.Sleep(1100 * time.Millisecond) // a new name each second
		if code, body := checkpointPost(t, s, checkpointToken); code != http.StatusOK {
			t.Fatalf("checkpoint %d: %d %s", i+2, code, body)
		}
	}
	entries, _ := os.ReadDir(dir)
	var kept []string
	for _, e := range entries {
		kept = append(kept, filepath.Base(e.Name()))
	}
	if len(kept) != 2 {
		t.Errorf("kept %v, want the newest two", kept)
	}
}

func TestBackupCheckpointOff(t *testing.T) {
	s := startServer(t)
	if code, _ := checkpointPost(t, s, checkpointToken); code != http.StatusNotFound {
		t.Errorf("checkpoints off: %d, want 404", code)
	}
}
