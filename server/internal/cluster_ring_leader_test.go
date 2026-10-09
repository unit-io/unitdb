package internal

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

// routedFollower answers a leader's heartbeats as a follower that saw the
// cluster route by ring version routed, and supports versions 1 and 2.
type routedFollower struct{ routed int }

func (f *routedFollower) Heartbeat(req *HeartbeatReq, resp *HeartbeatResp) error {
	*resp = HeartbeatResp{Node: "follower", Term: req.Term, Ok: true, Caps: NodeCapabilities{Version: clusterProtocolVersion, RingVersions: []int{1, 2}}, RoutedBy: f.routed}
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
// 1: its first round switches them to 2, and it moves the messages it stores
// by version 1, as they do. With its followers on 2 already, or on none, it
// moves none.
func TestNewLeaderMovesHistory(t *testing.T) {
	logOut := log.Writer()
	defer log.SetOutput(logOut)
	for _, tc := range []struct {
		routed   int
		wantMove bool
	}{{1, true}, {2, false}, {0, false}} {
		f := &routedFollower{routed: tc.routed}
		srv := startTestPeer(t, map[string]peerMethod{"Cluster.Heartbeat": method(f.Heartbeat)})

		var out syncBuffer
		log.SetOutput(&out)
		n := testNode(t, "follower", srv.addr)
		c := n.owner
		c.thisNodeName = "leader"
		c.ringVersion = initialRingVersion(0)
		c.allNodes = []string{"follower", "leader"}
		c.replicas = 2
		c.fo = &clusterFailover{leader: "leader", heartBeat: time.Second, voteTimeout: 3, nodeFailCountLimit: 3}
		c.fullRing = newRing(c.ringVersion, c.allNodes)
		c.setLive(c.allNodes)

		c.leaderRound()
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
		srv.stop()
	}
}
