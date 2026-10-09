package internal

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

// clusterHealth is what the health checks and metrics read of the
// cluster: when this node
// last heard from a leader, or was one, and which. The failover runner sets
// it; the fields it keeps itself aren't safe to read from elsewhere.
type clusterHealth struct {
	lastLeader atomic.Int64 // unix nanoseconds; 0 before any
	leader     atomic.Value // string

	// elections counts this node's elections by how they ended, and its
	// step-downs as the leader; forwardTimeouts the forwarded client
	// requests not answered within forwardTimeout.
	elections       [numElectionResults]atomic.Uint64
	forwardTimeouts atomic.Uint64
}

// electionResult is how an election of this node ended, or that it stopped
// leading: the label of unitdb_cluster_elections_total.
type electionResult int

const (
	// electionWon: a majority voted for this node; it leads.
	electionWon electionResult = iota
	// electionLost: the pre-vote passed but the vote did not make it
	// leader (too few votes, a later term or a leader turned up, or it
	// started leaving).
	electionLost
	// electionPrevoteRefused: too few nodes would vote for it; the term
	// stayed as it was.
	electionPrevoteRefused
	// electionSteppedDown: it led and stopped: another node leads or a
	// later term was seen, no majority answered for a lease, or it left.
	electionSteppedDown
	numElectionResults
)

var electionResultNames = [numElectionResults]string{"won", "lost", "prevote_refused", "stepped_down"}

// election counts an election's result.
func (h *clusterHealth) election(r electionResult) {
	h.elections[r].Add(1)
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
	case c.reconciling.Load():
		return false, "catching up: reconciling its restored topics with the other nodes"
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

// writeMetrics writes this node's view of the cluster (observability phase
// 3): only what is cheap and safe to read per scrape. Peers are labelled by
// their node names, a fixed set.
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
	c.writeReconcileMetrics(m)
	m.one("unitdb_cluster_leaving", "gauge", "Whether this node is leaving the cluster.", b(c.leaving.Load()))
	if last := c.health.lastLeader.Load(); last != 0 {
		m.one("unitdb_cluster_leader_age_seconds", "gauge", "Seconds since this node last heard from a leader, or was one.", time.Since(time.Unix(0, last)).Seconds())
	}
	term, leads := c.termAndLeader()
	m.one("unitdb_cluster_term", "gauge", "The highest leadership term this node has seen; 0 before any, or without failover.", float64(term))
	m.one("unitdb_cluster_is_leader", "gauge", "Whether this node leads the cluster.", b(leads))
	m.head("unitdb_cluster_elections_total", "counter", "Elections this node ran, by result, and its step-downs as the leader.")
	for r, name := range electionResultNames {
		m.value("unitdb_cluster_elections_total", "result="+quote(name), float64(c.health.elections[r].Load()))
	}
	m.one("unitdb_cluster_forward_timeouts_total", "counter", "Client requests forwarded to a peer that got no answer within the forward timeout.", float64(c.health.forwardTimeouts.Load()))
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
	m.head("unitdb_cluster_peer_up", "gauge", "Whether this node's connection to a peer is up.")
	m.head("unitdb_cluster_peer_calls_failed_total", "counter", "Calls to a peer that failed in transport: not sent, timed out or lost (not the peer's own errors).")
	withoutTLS := 0
	for _, name := range names {
		n := c.nodes[name]
		m.value("unitdb_replication_queue", "peer="+quote(name), float64(len(n.repl)))
		m.value("unitdb_cluster_peer_up", "peer="+quote(name), b(n.currentLink() != nil))
		m.value("unitdb_cluster_peer_calls_failed_total", "peer="+quote(name), float64(n.callsFailed.Load()))
		if caps, ok := n.capabilities(); !ok || !contains(caps.Capabilities, capTLS) {
			withoutTLS++
		}
	}
	m.one("unitdb_cluster_peers_without_tls", "gauge", "Peers that don't advertise TLS for cluster traffic.", float64(withoutTLS))
}
