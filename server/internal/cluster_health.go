package internal

import (
	"fmt"
	"sort"
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

// writeMetrics writes this node's view of the cluster: only what is cheap
// and safe to read per scrape. Peers are labelled by their node names, a
// fixed set.
func (c *Cluster) writeMetrics(m *metricsWriter) {
	b := func(v bool) float64 {
		if v {
			return 1
		}
		return 0
	}
	m.one("unitdb_cluster_nodes", "gauge", "Nodes in the cluster's configuration, this one included.", float64(len(c.nodes)+1))
	m.one("unitdb_cluster_members", "gauge", "Nodes in the ring this node routes by.", float64(len(c.getRingNodes())))
	m.one("unitdb_cluster_ring_version", "gauge", "The ring version last seen from the leader; 0 before any.", float64(c.clusterRing.Load()))
	m.one("unitdb_cluster_rebuilding", "gauge", "Whether this node is copying its topics from the others.", b(c.rebuilding.Load()))
	m.one("unitdb_cluster_leaving", "gauge", "Whether this node is leaving the cluster.", b(c.leaving.Load()))
	if last := c.health.lastLeader.Load(); last != 0 {
		m.one("unitdb_cluster_leader_age_seconds", "gauge", "Seconds since this node last heard from a leader, or was one.", time.Since(time.Unix(0, last)).Seconds())
	}
	c.pendingMu.Lock()
	pending := len(c.pending)
	c.pendingMu.Unlock()
	m.one("unitdb_cluster_pending_hints", "gauge", "Replicas waiting in memory for a peer to take them.", float64(pending))

	names := make([]string, 0, len(c.nodes))
	for name := range c.nodes {
		names = append(names, name)
	}
	sort.Strings(names)
	m.head("unitdb_replication_queue", "gauge", "Replicas queued for a peer.")
	withoutTLS := 0
	for _, name := range names {
		n := c.nodes[name]
		m.value("unitdb_replication_queue", "peer="+quote(name), float64(len(n.repl)))
		if caps, ok := n.capabilities(); !ok || !containsNode(caps.Capabilities, capTLS) {
			withoutTLS++
		}
	}
	m.one("unitdb_cluster_peers_without_tls", "gauge", "Peers that don't advertise TLS for cluster traffic.", float64(withoutTLS))
}
