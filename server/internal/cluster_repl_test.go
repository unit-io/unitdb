package internal

import (
	"net"
	"net/rpc"
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
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	svc := &replicaService{}
	srv := rpc.NewServer()
	if err := srv.RegisterName("Cluster", svc); err != nil {
		t.Fatal(err)
	}
	go srv.Accept(l)

	old := replicationDelay
	replicationDelay = 5 * time.Second
	defer func() { replicationDelay = old }()

	n := connectedNode(t, l.Addr().String())
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
