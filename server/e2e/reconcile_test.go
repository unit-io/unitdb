package e2e

// Reconciliation after a restore (docs/backup-restore.md):
// nodes started with -restored at one run's checkpoints settle each topic
// on the union of what their checkpoints hold; a node started with
// -restored into a running cluster catches up with it.

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	rh "github.com/unit-io/unitdb/server/internal/pkg/hash"
)

// relayCounts relays topic on node and returns how many times each message
// body came, all of them, until the relay is quiet.
func relayCounts(t *testing.T, node *clusterNode, cid, topic string) map[string]int {
	t.Helper()
	sub, err := dial(context.Background(), node.tcpAddr)
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
		t.Fatalf("no relay ack from %s", node.name)
	}
	got := map[string]int{}
	for {
		msg, ok := sub.waitPub(time.Second)
		if !ok {
			return got
		}
		for _, m := range msg.Messages {
			if _, body, ok := decodePayload(m.Payload); ok {
				got[string(body)]++
			}
		}
	}
}

// publishBodies publishes each body on topic through node, acknowledged.
func publishBodies(t *testing.T, node *clusterNode, cid, topic string, bodies ...string) {
	t.Helper()
	p, err := dial(context.Background(), node.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}
	for _, b := range bodies {
		id, err := p.publish(1, topic, encodePayload(0, b), "24h")
		if err != nil {
			t.Fatal(err)
		}
		if !p.waitAck(id, 5*time.Second) {
			t.Fatalf("publish %q on %s: no RECEIPT", b, topic)
		}
	}
}

func bodies(prefix string, n int) []string {
	var b []string
	for i := 0; i < n; i++ {
		b = append(b, fmt.Sprintf("%s%d", prefix, i))
	}
	return b
}

// sameCounts fails t unless got holds exactly want's bodies, as many times.
func sameCounts(t *testing.T, what string, got map[string]int, want []string) {
	t.Helper()
	w := map[string]int{}
	for _, b := range want {
		w[b]++
	}
	var diff []string
	for b, n := range w {
		if got[b] != n {
			diff = append(diff, fmt.Sprintf("%s: %d, want %d", b, got[b], n))
		}
	}
	for b, n := range got {
		if w[b] == 0 {
			diff = append(diff, fmt.Sprintf("%s: %d, want none", b, n))
		}
	}
	if len(diff) > 0 {
		sort.Strings(diff)
		t.Errorf("%s:\n  %s", what, strings.Join(diff, "\n  "))
	}
}

// TestRestoreReconcilesOneRun restores a cluster from one run whose nodes
// were checkpointed apart, writes going on between: with -restored, each
// topic's owner and replica settle on the union, each message once (two
// identical publishes stay two), and nothing written after the run comes
// back.
func TestRestoreReconcilesOneRun(t *testing.T) {
	c := startClusterWith(t, clusterOpts{env: checkpointEnv(t)}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	first := c.nodes[0] // checkpointed before the others
	contract := uint32(0x0e2ebcbc)
	cid := newClientID(contract)
	ring := rh.NewRing(clusterHashReplicas, nil)
	ring.Add(names...)
	holders := func(topic string) []string { return ring.GetN(fmt.Sprintf("%d/%s", contract, topic), 2) }
	// A topic the first node owns, and one it is the replica of.
	owned := topicOwnedBy(first.name, contract, "groups.reconcile.owned", names...)
	var replicated string
	for i := 0; ; i++ {
		topic := fmt.Sprintf("groups.reconcile.replica.t%d", i)
		if h := holders(topic); h[1] == first.name {
			replicated = topic
			break
		}
	}
	topics := []string{owned, replicated}

	// Before every checkpoint, with a message published twice.
	before := append(bodies("a", 10), "twice", "twice")
	between := bodies("b", 10)
	after := bodies("c", 5)
	for _, topic := range topics {
		publishBodies(t, c.nodes[1], cid, topic, before...)
	}
	time.Sleep(500 * time.Millisecond) // replication is asynchronous
	dirs := map[string]string{}
	dirs[first.name], _ = clusterCheckpoint(t, first)
	for _, topic := range topics {
		publishBodies(t, c.nodes[1], cid, topic, between...)
	}
	time.Sleep(500 * time.Millisecond)
	for _, n := range c.nodes[1:] {
		dirs[n.name], _ = clusterCheckpoint(t, n)
	}
	for _, topic := range topics {
		publishBodies(t, c.nodes[1], cid, topic, after...)
	}

	// The whole cluster lost, and restored from the run with -restored, the
	// first node first.
	for _, n := range c.nodes {
		n.stop()
	}
	for _, n := range c.nodes {
		if err := n.restartAt(dirs[n.name], "-restored"); err != nil {
			t.Fatalf("start %s with -restored: %v\nlogs:\n%s", n.name, err, n.logs.String())
		}
	}
	for _, n := range c.nodes {
		t.Logf("%s before it was ready: %v", n.name, waitReadyz(t, n, 60*time.Second))
	}
	want := append(append([]string(nil), before...), between...)

	// Each holder of each topic answers alone: the owner first, then the
	// replica with the owner stopped.
	for _, topic := range topics {
		h := holders(topic)
		owner, replica := c.node(h[0]), c.node(h[1])
		sameCounts(t, fmt.Sprintf("%s on its owner %s", topic, owner.name), relayCounts(t, owner, cid, topic), want)
		owner.stop()
		deadline := time.Now().Add(15 * time.Second)
		for {
			got := relayCounts(t, replica, cid, topic)
			if len(got) > 0 || time.Now().After(deadline) {
				sameCounts(t, fmt.Sprintf("%s on its replica %s", topic, replica.name), got, want)
				break
			}
			time.Sleep(250 * time.Millisecond) // until the ring drops the owner
		}
		if err := owner.restartAt(owner.dbPathArg()); err != nil {
			t.Fatalf("restart %s: %v", owner.name, err)
		}
		for _, n := range c.nodes {
			waitReadyz(t, n, 30*time.Second)
		}
	}

	// Reconciled, a node's checkpoint.json is its restored-from.json: its
	// next start is an ordinary one.
	for _, n := range c.nodes {
		if _, err := readFile(dirs[n.name], "restored-from.json"); err != nil {
			t.Errorf("%s: no restored-from.json after reconciling: %v", n.name, err)
		}
		if _, err := readFile(dirs[n.name], "checkpoint.json"); err == nil {
			t.Errorf("%s: checkpoint.json is still there after reconciling", n.name)
		}
	}
	if _, metrics := monitorGet(t, first.server, "/_metrics"); !strings.Contains(metrics, "unitdb_reconcile_topics_total ") {
		t.Errorf("no reconciliation metrics:\n%s", metrics)
	}
}

func readFile(dir, name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(dir, name))
}

// TestClusterRebuildFromRestartedNode rebuilds a node from a peer that has
// itself restarted: the peer's list of the topics it stores, loaded from its
// store when it opened, must name them. It didn't when records were sealed
// at rest: the list was loaded before the store could open them, and a
// rebuild (or a reconciliation, or a ring switch) skipped every topic the
// peer had stored before it restarted.
func TestClusterRebuildFromRestartedNode(t *testing.T) {
	c := startCluster(t, names...)
	leader, err := c.waitLeader(c.nodes, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	contract := uint32(0x0e2ecdcd)
	cid := newClientID(contract)
	ring := rh.NewRing(clusterHashReplicas, nil)
	ring.Add(names...)
	// Neither the victim nor the donor leads: a leader restarted gracefully
	// is TestClusterGracefulRestartRejoins's case, not this one's.
	victim := c.node(followerOf(leader))
	var topic string
	var donor *clusterNode
	for i := 0; ; i++ {
		topic = fmt.Sprintf("groups.donor.t%d", i)
		if h := ring.GetN(fmt.Sprintf("%d/%s", contract, topic), 2); h[0] == victim.name && h[1] != leader {
			donor = c.node(h[1])
			break
		}
	}
	publishAcked(t, c.nodes[2], cid, 20, topic)
	time.Sleep(500 * time.Millisecond) // replication is asynchronous

	// The donor restarts; then the victim loses its store.
	donor.shutdown()
	if err := donor.start(); err != nil {
		t.Fatal(err)
	}
	waitReadyz(t, donor, 30*time.Second)
	victim.stop()
	if err := os.RemoveAll(filepath.Join(victim.dbPath, "db")); err != nil {
		t.Fatal(err)
	}
	if err := victim.start(); err != nil {
		t.Fatal(err)
	}
	waitReadyz(t, victim, 30*time.Second)

	// The victim, the topic's owner, answers from its own store.
	checkStored(t, victim, cid, 20, topic)
}
