package internal

import (
	"sync/atomic"
	"testing"
	"time"
)

// replicaService stands in for a replica's Cluster.Replicate.
type replicaService struct{ entries int32 }

func (s *replicaService) Replicate(req *ReplicateReq, _ *bool) error {
	atomic.AddInt32(&s.entries, int32(len(req.Entries)))
	return nil
}

// TestReplicationDelayLetsWaitersThrough checks that a write someone waits
// for is not held by the test delay of an asynchronous batch queued before it.
func TestReplicationDelayLetsWaitersThrough(t *testing.T) {
	svc := &replicaService{}
	srv := startTestPeer(t, map[string]peerMethod{"Cluster.Replicate": method(svc.Replicate)})

	old := replicationDelay
	replicationDelay = 5 * time.Second
	defer func() { replicationDelay = old }()

	n := testNode(t, "replica", srv.addr)
	n.repl = make(chan replicaItem, replicationQueueSize)
	n.replDone = make(chan struct{})
	defer close(n.replDone)
	go n.replicateLoop("this")

	n.repl <- replicaItem{entry: &ReplicaEntry{ID: "async"}}
	time.Sleep(50 * time.Millisecond) // the loop takes it and starts the delay
	done := make(chan error, 1)
	n.repl <- replicaItem{entry: &ReplicaEntry{ID: "waited"}, done: done}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("waited write: %v", err)
		}
	case <-time.After(replicaAckTimeout):
		t.Fatal("a waited write was held by an asynchronous batch's delay")
	}
	if got := atomic.LoadInt32(&svc.entries); got != 2 {
		t.Fatalf("replica got %d entries, want both, in one batch", got)
	}
}

// TestSeenIDs checks the replicated ids a replica remembers: each once, the
// oldest dropped past the limit, and one forgotten stored again.
func TestSeenIDs(t *testing.T) {
	s := seenIDs{limit: 3}
	for _, id := range []string{"a", "b", "c"} {
		if !s.add(id) {
			t.Fatalf("%s new, taken as seen", id)
		}
	}
	if s.add("b") {
		t.Fatal("b seen, taken as new")
	}
	s.add("d") // drops a
	if !s.add("a") {
		t.Fatal("a dropped, still taken as seen")
	}
	s.remove("c")
	if !s.add("c") {
		t.Fatal("c forgotten, still taken as seen")
	}
}
