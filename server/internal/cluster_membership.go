/*
 * Copyright 2026 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package internal

// Membership and the leader: an independent implementation of
// docs/design/cluster-spec.md, section 1.3, used only when failover is on.
//
// Leadership is by numbered terms. A node that has not heard from a leader
// for vote_after heartbeats (randomized up to half again) first canvasses
// the others without changing anything (a pre-vote), and only if a majority
// of the configured nodes would vote for it, none of them hearing from a
// leader lately, asks for their votes in a new term. Each node votes once a
// term. A majority of the configured nodes makes it leader, so there is at
// most one per term, and none in a minority.
//
// The leader probes every peer each heartbeat with the live set, the ring
// version and every node's capabilities. A peer that answers is in the live
// set, unless it leaves; one that misses node_fail_after probes in a row is
// out. A leader without answers from a majority for vote_after heartbeats
// steps down, as does one that learns of a later term. Followers adopt the
// leader's live set and ring version, and answer with their capabilities and
// the ring version they routed by.
//
// A draining node tells every peer it leaves (Leave): they drop it from
// their live sets at once, and keep it out until it runs again. A leader
// that drains also steps down, and its successor, the first remaining live
// node by name, elects itself a heartbeat later.

import (
	stdlog "log"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
)

// clusterFailover is the membership state of this node.
type clusterFailover struct {
	heartBeat          time.Duration
	voteTimeout        int // heartbeats without a leader before an election
	nodeFailCountLimit int // probes a peer misses before it leaves the live set

	mu       sync.Mutex
	term     uint64
	votedFor string
	// leader is the node this one follows, itself when it leads, or "".
	leader string
	// lastContact is when this node last heard from its leader, or, as the
	// leader, last had answers from a majority.
	lastContact time.Time
	// timeout is how long without a leader before this node elects; electAt
	// an earlier election, as a leaving leader's successor.
	timeout time.Duration
	electAt time.Time
	// misses are the probes each peer missed in a row.
	misses map[string]int
	// logged is the leader this node last logged.
	logged string
	// kick starts the next round at once.
	kick chan struct{}
	// viewMu serializes adopting the leader's view; unsupported is the
	// last ring version named that this node lacks, under it.
	viewMu      sync.Mutex
	unsupported int
}

func newFailover(conf *clusterFailoverConfig) *clusterFailover {
	fo := &clusterFailover{
		heartBeat:          time.Duration(conf.Heartbeat) * time.Millisecond,
		voteTimeout:        conf.VoteAfter,
		nodeFailCountLimit: conf.NodeFailAfter,
		kick:               make(chan struct{}, 1),
	}
	if fo.heartBeat <= 0 {
		fo.heartBeat = 100 * time.Millisecond
	}
	if fo.voteTimeout <= 0 {
		fo.voteTimeout = 8
	}
	if fo.nodeFailCountLimit <= 0 {
		fo.nodeFailCountLimit = 16
	}
	return fo
}

// lease is how long a leader's contact holds: a follower doesn't vote for
// another node meanwhile, and a leader without a majority that long steps
// down.
func (fo *clusterFailover) lease() time.Duration {
	return fo.heartBeat * time.Duration(fo.voteTimeout)
}

// probeTimeout bounds one probe of a peer: about a heartbeat.
func (fo *clusterFailover) probeTimeout() time.Duration {
	return fo.heartBeat
}

// voteWait bounds a vote request.
func (fo *clusterFailover) voteWait() time.Duration {
	return 2 * fo.heartBeat
}

// resetTimer restarts the wait for a leader. The caller holds mu.
func (fo *clusterFailover) resetTimer() {
	base := fo.lease()
	fo.timeout = base + time.Duration(rand.Int63n(int64(base/2)+1))
	fo.lastContact = time.Now()
}

func (fo *clusterFailover) poke() {
	select {
	case fo.kick <- struct{}{}:
	default:
	}
}

// HeartbeatReq is the leader's probe.
type HeartbeatReq struct {
	Node string // the leader
	Term uint64
	// Live is the live set; RingVersion the ring version to route by, 0
	// until the leader has settled it.
	Live        []string
	RingVersion int
	// Caps are the capabilities of every node the leader knows of.
	Caps map[string]NodeCapabilities
}

// HeartbeatResp is a node's answer to the leader.
type HeartbeatResp struct {
	Node string
	Term uint64
	// Ok is set if the node takes the sender as its leader; if not, Leader
	// names the node it follows or is.
	Ok     bool
	Leader string
	Caps   NodeCapabilities
	// RoutedBy is the ring version the node saw the cluster route by
	// before this probe; 0 if none.
	RoutedBy int
	Leaving  bool
}

// VoteReq asks for a node's vote, or with Pre whether it would give it.
type VoteReq struct {
	Node string
	Term uint64
	Pre  bool
}

// VoteResp is a node's vote.
type VoteResp struct {
	Term    uint64
	Granted bool
}

// LeaveReq tells a node that the sender leaves the cluster.
type LeaveReq struct {
	Node      string
	WasLeader bool
}

// membershipLoop runs the leader's rounds, or waits for a leader and elects
// when none is heard from.
func (c *Cluster) membershipLoop() {
	fo := c.fo
	fo.mu.Lock()
	fo.resetTimer()
	fo.mu.Unlock()
	t := time.NewTicker(fo.heartBeat)
	defer t.Stop()
	for {
		select {
		case <-c.quit:
			return
		case <-t.C:
		case <-fo.kick:
		}
		if c.isLeader() {
			c.leaderRound()
			continue
		}
		if !c.leaving.Load() && c.electionDue() {
			c.runElection()
		}
	}
}

// termAndLeader returns the highest term this node has seen and whether
// it leads: 0 and false without failover.
func (c *Cluster) termAndLeader() (uint64, bool) {
	fo := c.fo
	if fo == nil {
		return 0, false
	}
	fo.mu.Lock()
	defer fo.mu.Unlock()
	return fo.term, fo.leader == c.thisNodeName
}

// isLeader reports whether this node leads.
func (c *Cluster) isLeader() bool {
	fo := c.fo
	if fo == nil {
		return false
	}
	fo.mu.Lock()
	defer fo.mu.Unlock()
	return fo.leader == c.thisNodeName
}

func (c *Cluster) electionDue() bool {
	fo := c.fo
	fo.mu.Lock()
	defer fo.mu.Unlock()
	now := time.Now()
	if !fo.electAt.IsZero() && now.After(fo.electAt) {
		return true
	}
	return now.Sub(fo.lastContact) > fo.timeout
}

// quorum is a majority of the configured nodes.
func (c *Cluster) quorum() int {
	return len(c.allNodes)/2 + 1
}

// runElection canvasses the others, and if they would, elects this node.
func (c *Cluster) runElection() {
	fo := c.fo
	fo.mu.Lock()
	proposed := fo.term + 1
	fo.electAt = time.Time{}
	fo.mu.Unlock()
	if !c.canvass(proposed, true) {
		fo.mu.Lock()
		fo.resetTimer()
		fo.mu.Unlock()
		c.health.election(electionPrevoteRefused)
		return
	}
	fo.mu.Lock()
	if fo.term >= proposed || fo.leader != "" && fo.leader != c.thisNodeName && time.Since(fo.lastContact) < fo.lease() {
		fo.mu.Unlock()
		c.health.election(electionLost)
		return
	}
	fo.term = proposed
	fo.votedFor = c.thisNodeName
	fo.leader = ""
	fo.mu.Unlock()
	won := c.canvass(proposed, false)
	fo.mu.Lock()
	if !won || fo.term != proposed || c.leaving.Load() {
		fo.resetTimer()
		fo.mu.Unlock()
		c.health.election(electionLost)
		return
	}
	fo.leader = c.thisNodeName
	fo.lastContact = time.Now()
	fo.misses = make(map[string]int)
	fo.logged = c.thisNodeName
	fo.mu.Unlock()
	c.health.election(electionWon)
	stdlog.Printf("cluster: Elected myself as a new leader (term %d)", proposed)
	c.health.leaderSeen(c.thisNodeName)
	c.leaderRound()
}

// canvass asks every peer for its vote, or with pre whether it would give
// it, in term, and reports whether a majority (this node included) did.
func (c *Cluster) canvass(term uint64, pre bool) bool {
	fo := c.fo
	type answer struct {
		resp *VoteResp
		err  error
	}
	answers := make(chan answer, len(c.nodes))
	for _, n := range c.nodes {
		resp := &VoteResp{}
		n.goCall("Cluster.Vote", &VoteReq{Node: c.thisNodeName, Term: term, Pre: pre}, resp, fo.voteWait(), func(err error) {
			answers <- answer{resp, err}
		})
	}
	votes := 1
	deadline := time.After(fo.voteWait() + 50*time.Millisecond)
	for i := 0; i < len(c.nodes); i++ {
		select {
		case a := <-answers:
			if a.err != nil {
				continue
			}
			if a.resp.Granted {
				votes++
				continue
			}
			fo.mu.Lock()
			if a.resp.Term > fo.term {
				fo.term = a.resp.Term
				fo.votedFor = ""
			}
			fo.mu.Unlock()
		case <-deadline:
			return votes >= c.quorum()
		}
	}
	return votes >= c.quorum()
}

// Vote answers a candidate. A node that heard from a leader lately votes
// for no other node.
func (c *Cluster) Vote(req *VoteReq, resp *VoteResp) error {
	fo := c.fo
	if fo == nil {
		return nil
	}
	fo.mu.Lock()
	defer fo.mu.Unlock()
	fresh := fo.leader != "" && fo.leader != req.Node && time.Since(fo.lastContact) < fo.lease()
	resp.Term = fo.term
	if req.Pre {
		resp.Granted = req.Term > fo.term && !fresh
		return nil
	}
	if req.Term < fo.term || fresh {
		return nil
	}
	if req.Term > fo.term {
		if fo.leader == c.thisNodeName {
			c.health.election(electionSteppedDown)
		}
		fo.term = req.Term
		fo.votedFor = ""
		fo.leader = ""
	}
	if fo.votedFor == "" || fo.votedFor == req.Node {
		fo.votedFor = req.Node
		resp.Granted = true
		fo.resetTimer()
	}
	resp.Term = fo.term
	return nil
}

// Heartbeat takes the leader's probe: it follows the sender, unless it
// knows of a later term, and adopts its view.
func (c *Cluster) Heartbeat(req *HeartbeatReq, resp *HeartbeatResp) error {
	resp.Node = c.thisNodeName
	resp.Caps = ownNodeCapabilities()
	resp.RoutedBy = int(c.clusterRing.Load())
	resp.Leaving = c.leaving.Load()
	fo := c.fo
	if fo == nil {
		return nil
	}
	fo.mu.Lock()
	resp.Term = fo.term
	if req.Term < fo.term || (fo.leader == c.thisNodeName && req.Term == fo.term && c.thisNodeName < req.Node) {
		// A stale leader, or another of this term: of two, the first by
		// name keeps leading.
		resp.Leader = fo.leader
		fo.mu.Unlock()
		return nil
	}
	if req.Term > fo.term {
		fo.term = req.Term
		fo.votedFor = ""
	}
	if fo.leader == c.thisNodeName && req.Node != c.thisNodeName {
		c.health.election(electionSteppedDown)
	}
	fo.leader = req.Node
	fo.resetTimer()
	fo.electAt = time.Time{}
	logIt := fo.logged != req.Node
	fo.logged = req.Node
	fo.mu.Unlock()
	if logIt {
		stdlog.Printf("cluster: leader '%s' elected (term %d)", req.Node, req.Term)
	}
	c.health.leaderSeen(req.Node)
	for name, nc := range req.Caps {
		if n := c.nodes[name]; n != nil {
			n.setCapabilities(nc)
		}
	}
	c.adoptView(req.Live, req.RingVersion)
	resp.Ok = true
	resp.Term = req.Term
	return nil
}

// adoptView routes by the leader's live set and ring version (spec 1.4).
func (c *Cluster) adoptView(live []string, version int) {
	fo := c.fo
	fo.viewMu.Lock()
	defer fo.viewMu.Unlock()
	rehash := false
	if version != 0 {
		switch {
		case !supportsRingVersion(version):
			if fo.unsupported != version {
				fo.unsupported = version
				log.ErrLogger.Error().Int("version", version).Msg("cluster: the cluster routes by a ring version this node does not support; keeping its own")
			}
		case version != c.getRingVersion():
			c.adoptRingVersion(version)
			rehash = true
		case int(c.clusterRing.Load()) != version:
			c.adoptRingVersion(version)
		}
	}
	next := c.withoutDeparting(live)
	if rehash || !sameNodes(next, c.getRingNodes()) {
		c.setLive(next)
	}
}

// leaderRound probes every peer and settles the live set and ring version.
func (c *Cluster) leaderRound() {
	fo := c.fo
	self := c.thisNodeName
	fo.mu.Lock()
	if fo.leader != self || c.leaving.Load() {
		fo.mu.Unlock()
		return
	}
	term := fo.term
	fo.mu.Unlock()

	version := 0
	if c.clusterRing.Load() > 0 {
		version = c.getRingVersion()
	}
	req := &HeartbeatReq{Node: self, Term: term, Live: c.getRingNodes(), RingVersion: version, Caps: c.knownCapabilities()}
	acks := c.probe(req, fo.probeTimeout())

	fo.mu.Lock()
	if fo.leader != self || fo.term != term {
		fo.mu.Unlock()
		return
	}
	for name, a := range acks {
		if a.Ok {
			continue
		}
		if a.Term > term || (a.Term == term && a.Leader != "" && a.Leader != self && a.Leader < self) {
			if a.Term > fo.term {
				fo.term = a.Term
				fo.votedFor = ""
			}
			fo.leader = ""
			fo.resetTimer()
			fo.mu.Unlock()
			c.health.election(electionSteppedDown)
			log.ErrLogger.Info().Str("context", "cluster.leaderRound").Str("peer", name).Msg("cluster: another node leads: stepping down")
			return
		}
		delete(acks, name)
	}
	if len(acks)+1 >= c.quorum() {
		fo.lastContact = time.Now()
	} else if time.Since(fo.lastContact) > fo.lease() {
		fo.leader = ""
		fo.resetTimer()
		fo.mu.Unlock()
		c.health.election(electionSteppedDown)
		log.ErrLogger.Warn().Str("context", "cluster.leaderRound").Int("answers", len(acks)).Msg("cluster: no majority answers: stepping down")
		return
	}
	majority := len(acks)+1 >= c.quorum()
	if fo.misses == nil {
		fo.misses = make(map[string]int)
	}
	cur := c.getRingNodes()
	next := []string{}
	if !c.leaving.Load() {
		next = append(next, self)
	}
	for name := range c.nodes {
		a := acks[name]
		if a != nil {
			fo.misses[name] = 0
		} else {
			fo.misses[name]++
		}
		switch {
		case c.isDeparting(name) || (a != nil && a.Leaving):
		case a != nil:
			next = append(next, name)
		case fo.misses[name] >= fo.nodeFailCountLimit:
		case containsNode(cur, name):
			next = append(next, name)
		}
	}
	fo.mu.Unlock()
	sort.Strings(next)
	if majority {
		c.health.leaderSeen(self)
	}
	for name, a := range acks {
		c.nodes[name].setCapabilities(a.Caps)
	}

	// The ring version: the first round of a leader that has not seen the
	// cluster route by any takes the lowest its followers routed by.
	if c.clusterRing.Load() == 0 {
		base := 0
		for _, a := range acks {
			if a.RoutedBy > 0 && (base == 0 || a.RoutedBy < base) {
				base = a.RoutedBy
			}
		}
		if base == 0 {
			base = c.getRingVersion()
		}
		c.clusterRing.Store(int32(base))
	}
	// The version changes only in a round every live peer answered: each
	// has then been told, and routes by, the current one, so a switch moves
	// history on every node (a node just restarted included).
	live := make([]*ClusterNode, 0, len(next))
	allAnswered := true
	for _, name := range next {
		if n := c.nodes[name]; n != nil {
			live = append(live, n)
			allAnswered = allAnswered && acks[name] != nil
		}
	}
	v := c.getRingVersion()
	if allAnswered {
		v = c.chooseRingVersion(live)
	}
	rehash := false
	if v != c.getRingVersion() {
		c.adoptRingVersion(v)
		rehash = true
	} else if int(c.clusterRing.Load()) != v {
		c.adoptRingVersion(v)
	}
	if rehash || !sameNodes(next, cur) {
		c.setLive(next)
		fo.poke()
	}
}

// probe sends req to every peer and returns the answers that came within
// d, by peer.
func (c *Cluster) probe(req *HeartbeatReq, d time.Duration) map[string]*HeartbeatResp {
	type answer struct {
		name string
		resp *HeartbeatResp
		err  error
	}
	answers := make(chan answer, len(c.nodes))
	for name, n := range c.nodes {
		name, resp := name, &HeartbeatResp{}
		n.goCall("Cluster.Heartbeat", req, resp, d, func(err error) {
			answers <- answer{name, resp, err}
		})
	}
	acks := make(map[string]*HeartbeatResp, len(c.nodes))
	deadline := time.After(d + 20*time.Millisecond)
	for i := 0; i < len(c.nodes); i++ {
		select {
		case a := <-answers:
			if a.err == nil {
				acks[a.name] = a.resp
			}
		case <-deadline:
			return acks
		}
	}
	return acks
}

// knownCapabilities returns the capabilities of every node this one knows.
func (c *Cluster) knownCapabilities() map[string]NodeCapabilities {
	caps := map[string]NodeCapabilities{c.thisNodeName: ownNodeCapabilities()}
	for name, n := range c.nodes {
		if nc, ok := n.capabilities(); ok {
			caps[name] = nc
		}
	}
	return caps
}

// resign steps down as the leader, as this node leaves; it reports whether
// it led.
func (c *Cluster) resign() bool {
	fo := c.fo
	if fo == nil {
		return false
	}
	fo.mu.Lock()
	defer fo.mu.Unlock()
	was := fo.leader == c.thisNodeName
	if was {
		fo.leader = ""
		c.health.election(electionSteppedDown)
	}
	return was
}

// Leave takes a peer that leaves out of the live set until it runs again.
// If it led, the first remaining live node by name elects itself.
func (c *Cluster) Leave(req *LeaveReq, unused *bool) error {
	c.departMu.Lock()
	c.departing[req.Node] = true
	c.departMu.Unlock()
	live := c.withoutDeparting(c.getRingNodes())
	if fo := c.fo; fo != nil {
		fo.mu.Lock()
		if fo.leader == req.Node {
			fo.leader = ""
			fo.resetTimer()
			if len(live) > 0 && live[0] == c.thisNodeName && !c.leaving.Load() {
				fo.electAt = time.Now().Add(fo.heartBeat)
			}
		}
		leads := fo.leader == c.thisNodeName
		fo.mu.Unlock()
		if leads {
			fo.poke()
		}
	}
	c.setLive(live)
	log.ErrLogger.Info().Str("context", "cluster.Leave").Str("peer", req.Node).Msg("cluster: peer leaves")
	return nil
}
