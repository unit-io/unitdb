package e2e

// Restoring a cluster's nodes (docs/backup-restore.md):
// a lost node starts empty and rebuilds from the others; a node started at a
// checkpoint while its peers run is refused; a whole cluster starts at one
// backup run's checkpoints.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	rh "github.com/unit-io/unitdb/server/internal/pkg/hash"
)

// restartAt restarts s with -db_path at dbPath and extra arguments (the
// last start's -restored and -journal dropped), and waits until it takes
// connections.
func (s *server) restartAt(dbPath string, extra ...string) error {
	s.stop()
	args := []string{s.cmd.Args[0]}
	for i := 1; i < len(s.cmd.Args); i++ {
		switch a := s.cmd.Args[i]; a {
		case "-restored": // a flag of the last start only
		case "-journal": // with its value, as -restored's companion
			i++
		case "-db_path":
			args = append(args, "-db_path", dbPath)
			i++
		default:
			args = append(args, a)
		}
	}
	s.cmd.Args = append(args, extra...)
	return s.start()
}

// dbPathArg returns the -db_path s was started with.
func (s *server) dbPathArg() string {
	for i, a := range s.cmd.Args {
		if a == "-db_path" && i+1 < len(s.cmd.Args) {
			return s.cmd.Args[i+1]
		}
	}
	return ""
}

// refusedAt starts s with -db_path at dbPath and extra arguments, expecting
// it to refuse: it returns what the server logged before it exited.
func (s *server) refusedAt(t *testing.T, dbPath string, extra ...string) string {
	t.Helper()
	saved := append([]string(nil), s.cmd.Args...)
	s.noWait = true
	defer func() { s.cmd.Args, s.noWait = saved, false }()
	before := len(s.logs.String())
	if err := s.restartAt(dbPath, extra...); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.exited:
	case <-time.After(20 * time.Second):
		s.stop()
		t.Fatalf("started at %s %v; want it refused\nlogs:\n%s", dbPath, extra, s.logs.String()[before:])
	}
	return s.logs.String()[before:]
}

// clusterCheckpoint takes a checkpoint on n and returns its directory and
// description.
func clusterCheckpoint(t *testing.T, n *clusterNode) (string, map[string]interface{}) {
	t.Helper()
	code, body := checkpointPost(t, n.server, checkpointToken)
	if code != http.StatusOK {
		t.Fatalf("checkpoint on %s: %d %s", n.name, code, body)
	}
	var ck map[string]interface{}
	if err := json.Unmarshal([]byte(body), &ck); err != nil {
		t.Fatalf("checkpoint answer %q", body)
	}
	dir, _ := ck["dir"].(string)
	return dir, ck
}

// checkpointEnv has every node take checkpoints into a directory of its own.
func checkpointEnv(t *testing.T) map[string][]string {
	env := map[string][]string{}
	for _, name := range names {
		env[name] = []string{"CHECKPOINT_DIR=" + t.TempDir(), "CHECKPOINT_TOKEN=" + checkpointToken, "CHECKPOINT_MIN_MINUTES=0"}
	}
	return env
}

// publishAcked publishes n messages on each topic through node, each
// acknowledged.
func publishAcked(t *testing.T, node *clusterNode, cid string, n int, topics ...string) {
	t.Helper()
	p, err := dial(context.Background(), node.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}
	for _, topic := range topics {
		for i := 0; i < n; i++ {
			id, err := p.publish(1, topic, encodePayload(i, fmt.Sprintf("%s/%d", topic, i)), "24h")
			if err != nil {
				t.Fatal(err)
			}
			if !p.waitAck(id, 5*time.Second) {
				t.Fatalf("publish %d on %s: no RECEIPT", i, topic)
			}
		}
	}
}

// checkStored fails t unless node relays all n messages of each topic.
func checkStored(t *testing.T, node *clusterNode, cid string, n int, topics ...string) {
	t.Helper()
	for _, topic := range topics {
		got := storedOn(t, node.server, cid, topic, n)
		for i := 0; i < n; i++ {
			if want := fmt.Sprintf("%s/%d", topic, i); got[i] != want {
				t.Errorf("%s relays message %d of %s as %q, want %q (has %d)", node.name, i, topic, got[i], want, len(got))
				break
			}
		}
	}
}

// TestRestoreLostNodeStartsEmpty is the drill for restoring a lost node,
// the other two up: its store is deleted and it starts empty. It takes no
// clients until it has every message acknowledged before (/_readyz), and
// then has them on its own, the topics' other holders stopped.
func TestRestoreLostNodeStartsEmpty(t *testing.T) {
	c := startCluster(t, names...)
	leader, err := c.waitLeader(c.nodes, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	victim := c.node(followerOf(leader))
	var live *clusterNode
	for _, n := range c.nodes {
		if n != victim {
			live = n
			break
		}
	}
	contract := uint32(0x0e2e5e5e)
	cid := newClientID(contract)
	ring := rh.NewRing(clusterHashReplicas, nil)
	ring.Add(names...)
	holders := func(topic string) []string { return ring.GetN(fmt.Sprintf("%d/%s", contract, topic), 2) }
	owned := topicOwnedBy(victim.name, contract, "groups.lost.owned", names...)
	var replicated string
	for i := 0; ; i++ {
		topic := fmt.Sprintf("groups.lost.replica.t%d", i)
		if h := holders(topic); h[0] != victim.name && h[1] == victim.name {
			replicated = topic
			break
		}
	}
	n := scaled(200)
	publishAcked(t, live, cid, n, owned, replicated)
	time.Sleep(500 * time.Millisecond) // replication is asynchronous

	// The node is lost with its store, and starts empty.
	victim.stop()
	if err := os.RemoveAll(filepath.Join(victim.dbPath, "db")); err != nil {
		t.Fatal(err)
	}
	if err := victim.start(); err != nil {
		t.Fatalf("restart %s: %v", victim.name, err)
	}
	t.Logf("%s before it was ready: %v", victim.name, waitReadyz(t, victim, 30*time.Second))

	// Ready, it has them all, alone.
	for _, topic := range []string{owned, replicated} {
		for _, h := range holders(topic) {
			if h != victim.name {
				c.node(h).stop()
			}
		}
	}
	checkStored(t, victim, cid, n, owned, replicated)
}

// TestRestoreCheckpointRefusedInRunningCluster checks the guard: a node
// started at its own checkpoint while its peers run is refused, and says to
// start empty or with -restored. With -restored it catches up with them:
// what was written since its checkpoint is on it, alone. -restored on a
// store that isn't a checkpoint is refused. Started empty, it rebuilds.
func TestRestoreCheckpointRefusedInRunningCluster(t *testing.T) {
	c := startClusterWith(t, clusterOpts{env: checkpointEnv(t)}, names...)
	leader, err := c.waitLeader(c.nodes, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	victim := c.node(followerOf(leader))
	cid := newClientID(0x0e2e6a4d)
	topic := topicOwnedBy(victim.name, 0x0e2e6a4d, "groups.guard.owned", names...)
	publishAcked(t, victim, cid, 20, topic)

	dir, ck := clusterCheckpoint(t, victim)
	if ck["node"] != victim.name || ck["engine"] == "" || ck["time"] == nil {
		t.Errorf("checkpoint answer %v", ck)
	}
	if stats, _ := ck["stats"].(map[string]interface{}); stats == nil || stats["messages"] == nil || stats["messages"].(float64) < 20 {
		t.Errorf("checkpoint stats %v", ck["stats"])
	}
	if _, err := os.Stat(filepath.Join(dir, "checkpoint.json")); err != nil {
		t.Errorf("no checkpoint.json in %s: %v", dir, err)
	}

	logs := victim.refusedAt(t, dir)
	if !strings.Contains(logs, "Refusing to start") || !strings.Contains(logs, "empty store") {
		t.Errorf("refused at a checkpoint, it says:\n%s", logs)
	}
	logs = victim.refusedAt(t, filepath.Join(victim.dbPath, "db"), "-restored")
	if !strings.Contains(logs, "isn't a checkpoint") {
		t.Errorf("refused with -restored on its own store, it says:\n%s", logs)
	}

	// Written since its checkpoint, while it was down: the topic's replica
	// took it.
	publishBodies(t, c.node(leader), cid, topic, bodies("since", 10)...)
	time.Sleep(500 * time.Millisecond)
	if err := victim.restartAt(dir, "-restored"); err != nil {
		t.Fatalf("start with -restored: %v\nlogs:\n%s", err, victim.logs.String())
	}
	waitReadyz(t, victim, 30*time.Second)
	var want []string
	for i := 0; i < 20; i++ {
		want = append(want, fmt.Sprintf("%s/%d", topic, i))
	}
	sameCounts(t, "the owner, restored into the running cluster", relayCounts(t, victim, cid, topic), append(want, bodies("since", 10)...))

	// Its own store, which isn't a checkpoint, starts as before; so does an
	// empty one.
	if err := victim.restartAt(filepath.Join(victim.dbPath, "db")); err != nil {
		t.Fatalf("restart on its own store: %v\nlogs:\n%s", err, victim.logs.String())
	}
	if err := victim.restartAt(filepath.Join(victim.dbPath, "empty")); err != nil {
		t.Fatalf("restart empty: %v\nlogs:\n%s", err, victim.logs.String())
	}
	waitReadyz(t, victim, 30*time.Second)
	sameCounts(t, "the owner, rebuilt", relayCounts(t, victim, cid, topic), append(want, bodies("since", 10)...))
}

// TestRestoreWholeClusterFromOneRun restores every node at its checkpoint of
// one run, started one after another: each finds its peers restored from
// the same run, and starts. A node whose checkpoint is of another run is
// refused.
func TestRestoreWholeClusterFromOneRun(t *testing.T) {
	c := startClusterWith(t, clusterOpts{env: checkpointEnv(t)}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	contract := uint32(0x0e2e7b0b)
	cid := newClientID(contract)
	var topics []string
	for _, n := range c.nodes {
		topics = append(topics, topicOwnedBy(n.name, contract, "groups.whole.owned", names...))
	}
	publishAcked(t, c.nodes[0], cid, 20, topics...)
	time.Sleep(500 * time.Millisecond) // replication is asynchronous

	// One run: a checkpoint of every node; then every node is lost.
	dirs := map[string]string{}
	for _, n := range c.nodes {
		dirs[n.name], _ = clusterCheckpoint(t, n)
	}
	for _, n := range c.nodes {
		n.stop()
	}

	// The last node's checkpoint is made to be of another run: refused once
	// the first two are up.
	last := c.nodes[len(c.nodes)-1]
	infoPath := filepath.Join(dirs[last.name], "checkpoint.json")
	orig, err := os.ReadFile(infoPath)
	if err != nil {
		t.Fatal(err)
	}
	var info map[string]interface{}
	json.Unmarshal(orig, &info)
	taken, _ := time.Parse(time.RFC3339Nano, info["time"].(string))
	info["time"] = taken.Add(-2 * time.Hour).Format(time.RFC3339Nano)
	other, _ := json.Marshal(info)

	for _, n := range c.nodes[:len(c.nodes)-1] {
		if err := n.restartAt(dirs[n.name]); err != nil {
			t.Fatalf("start %s at its checkpoint: %v\nlogs:\n%s", n.name, err, n.logs.String())
		}
	}
	if err := os.WriteFile(infoPath, other, 0600); err != nil {
		t.Fatal(err)
	}
	if logs := last.refusedAt(t, dirs[last.name]); !strings.Contains(logs, "not the same backup run") {
		t.Errorf("a checkpoint of another run, it says:\n%s", logs)
	}
	if err := os.WriteFile(infoPath, orig, 0600); err != nil {
		t.Fatal(err)
	}
	if err := last.restartAt(dirs[last.name]); err != nil {
		t.Fatalf("start %s at its checkpoint: %v\nlogs:\n%s", last.name, err, last.logs.String())
	}

	for _, n := range c.nodes {
		waitReadyz(t, n, 30*time.Second)
	}
	for _, n := range c.nodes {
		checkStored(t, n, cid, 20, topics...)
	}
}
