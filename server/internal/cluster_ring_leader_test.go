package internal

import (
	"bytes"
	"log"
	"net"
	"net/rpc"
	"strings"
	"sync"
	"testing"
	"time"
)

// routedFollower answers a leader's pings as a follower that saw the cluster
// route by ring version routed, and supports versions 1 and 2.
type routedFollower struct{ routed int }

func (f *routedFollower) Ping(_ *ClusterPing, pong *ClusterPong) error {
	*pong = ClusterPong{Node: "follower", NodeCapabilities: NodeCapabilities{Version: clusterProtocolVersion, RingVersions: []int{1, 2}}, RingVersion: f.routed}
	return nil
}

// syncBuffer is a log output safe for the goroutine moveHistory runs on.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// TestNewLeaderMovesHistory starts a node at ring version 2 that leads before
// it saw the cluster route by any version, as a node upgraded and restarted
// does when it wins the next election, while its followers route by version
// 1: its first pings switch them to 2, and it moves the messages it stores by
// version 1, as they do. With its followers on 2 already, or on none, it moves
// none.
func TestNewLeaderMovesHistory(t *testing.T) {
	logOut := log.Writer()
	defer log.SetOutput(logOut)
	for _, tc := range []struct {
		routed   int
		wantMove bool
	}{{1, true}, {2, false}, {0, false}} {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		srv := rpc.NewServer()
		if err := srv.RegisterName("Cluster", &routedFollower{routed: tc.routed}); err != nil {
			t.Fatal(err)
		}
		go srv.Accept(l)

		var out syncBuffer
		log.SetOutput(&out)
		n := connectedNode(t, l.Addr().String())
		n.name = "follower"
		c := &Cluster{
			thisNodeName: "leader",
			ringVersion:  initialRingVersion(0),
			allNodes:     []string{"leader", "follower"},
			nodes:        map[string]*ClusterNode{"follower": n},
			replicas:     2,
			fo:           &clusterFailover{leader: "leader", heartBeat: time.Second, nodeFailCountLimit: 3},
		}
		c.fullRing = newRing(c.ringVersion, c.allNodes)
		c.rehash(nil)

		c.sendPings()
		if got := c.clusterRing.Load(); got != 2 {
			t.Errorf("followers on %d: the leader sees the cluster route by %d, want 2", tc.routed, got)
		}
		moved := false
		for deadline := time.Now().Add(time.Second); !moved && time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			moved = strings.Contains(out.String(), "moved history for ring version 2")
		}
		if moved != tc.wantMove {
			t.Errorf("followers on %d: moved history %v, want %v; log:\n%s", tc.routed, moved, tc.wantMove, out.String())
		}
		l.Close()
	}
}
