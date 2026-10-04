package internal

import (
	"fmt"
	"sync/atomic"
	"time"
)

// clusterHealth is what the health checks read of the cluster: when this
// node last heard from a leader, or was one, and which. The failover runner sets
// it; the fields it keeps itself aren't safe to read from elsewhere.
type clusterHealth struct {
	lastLeader atomic.Int64 // unix nanoseconds; 0 before any
	leader     atomic.Value // string
}

// leaderSeen records a ping from leader, or this node pinging as the leader.
func (h *clusterHealth) leaderSeen(leader string) {
	h.leader.Store(leader)
	h.lastLeader.Store(time.Now().UnixNano())
}

// readiness says whether this node can serve its part of the cluster: it
// isn't leaving or stopped, isn't copying its topics from the others, is in
// the ring, and has heard from a leader lately. A nil cluster (a single
// server) is ready.
func (c *Cluster) readiness() (bool, string) {
	if c == nil {
		return true, "standalone"
	}
	switch {
	case c.stopped.Load():
		return false, "stopped"
	case c.leaving.Load():
		return false, "leaving the cluster"
	case c.rebuilding.Load():
		return false, "catching up: copying its topics from the other nodes"
	}
	ring := c.getRingNodes()
	configured := len(c.nodes) + 1
	if !containsNode(ring, c.thisNodeName) {
		return false, fmt.Sprintf("not in the ring yet (the ring has %d of %d nodes)", len(ring), configured)
	}
	if c.fo == nil {
		return true, fmt.Sprintf("in the ring: %d of %d nodes", len(ring), configured)
	}
	last := c.health.lastLeader.Load()
	if last == 0 {
		return false, "no leader heard from yet"
	}
	age := time.Since(time.Unix(0, last))
	// When a follower would start an election.
	if stale := c.fo.heartBeat * time.Duration(c.fo.voteTimeout+1); age > stale {
		return false, fmt.Sprintf("no leader for %s", age.Round(time.Second))
	}
	leader, _ := c.health.leader.Load().(string)
	return true, fmt.Sprintf("in the ring: %d of %d nodes; leader %s, %s ago",
		len(ring), configured, leader, age.Round(time.Millisecond))
}
