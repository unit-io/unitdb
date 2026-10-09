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

// The cluster: configuration, the node's lifecycle (init, start, drain,
// shutdown) and the ring state every other part reads. An independent
// implementation of docs/design/cluster-spec.md; the transport is in
// cluster_transport.go, membership and the leader in cluster_membership.go,
// request routing and proxied sessions in cluster_routing.go, replication
// and hints in cluster_replication.go, rebuild and sessions in
// cluster_sessions.go.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	rh "github.com/unit-io/unitdb/server/internal/pkg/hash"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
)

// Timing and limits (spec 1.19).
const (
	// reconnectEvery is how often a peer that is down is dialed again.
	reconnectEvery = 200 * time.Millisecond
	// dialTimeout bounds connecting to a peer, its hello included.
	dialTimeout = time.Second
	// forwardRetryFor and forwardRetry: a request a topic's owner did not
	// take, or that could not be sent, is sent again by the current ring.
	forwardRetryFor = 3 * time.Second
	forwardRetry    = 100 * time.Millisecond
	// forwardTimeout bounds one forwarded client request (spec 7.6): a
	// frozen owner fails the request instead of holding the client.
	forwardTimeout = 5 * time.Second
	// replicaAckTimeout is how long a write waits for a replica.
	replicaAckTimeout = time.Second
	// replicationQueueSize and replicationBatchSize: a peer's queue, and
	// the most items sent in one call.
	replicationQueueSize = 4096
	replicationBatchSize = 256
	// replicateCallTimeout bounds one batch's call.
	replicateCallTimeout = 10 * time.Second
	// maxPendingHints are hints kept in memory while the store refuses them.
	maxPendingHints = 10000
	// seenIDsKept is how many replicated message ids a replica remembers.
	seenIDsKept = 100000
	// sessionHintTTL is how long a session hint is kept.
	sessionHintTTL = "24h"
	// fetchSessionWait is how long a node waits for the others' copies of
	// a session, in all.
	fetchSessionWait = time.Second
	// startResyncWait is how long a starting node waits for each peer to
	// send its clients' subscriptions again.
	startResyncWait = 3 * time.Second
	// rebalanceAttempts and rebalanceRetry: a connection whose
	// subscriptions could not all be placed is tried again.
	rebalanceAttempts = 10
	rebalanceRetry    = 200 * time.Millisecond
	// rebuildAttempts, rebuildRetry and rebuildTimeout: asking a peer for
	// the topics of a node rebuilding (also used by cluster_ring.go and
	// cluster_reconcile.go).
	rebuildAttempts = 20
	rebuildRetry    = 500 * time.Millisecond
	rebuildTimeout  = 30 * time.Second
	// deliverTimeout bounds a delivery call to a client's node.
	deliverTimeout = 5 * time.Second
	// leaveWait bounds the calls telling the others this node leaves.
	leaveWait = 2 * time.Second
	// clusterHashReplicas is the points per node of ring version 2.
	clusterHashReplicas = 160
	// defaultHandoffInterval is how often hints go to a connected peer.
	defaultHandoffInterval = 5 * time.Second
)

// Test delays: replicationDelay delays each batch of asynchronous
// replication, deliverDelay each call delivering to another node's clients,
// for tests of what waits for a replica and how long a fan-out takes. Set by
// UNITDB_REPLICATION_DELAY and UNITDB_DELIVER_DELAY, as durations.
var (
	replicationDelay = envDuration("UNITDB_REPLICATION_DELAY")
	deliverDelay     = envDuration("UNITDB_DELIVER_DELAY")
)

func envDuration(name string) time.Duration {
	d, _ := time.ParseDuration(os.Getenv(name))
	return d
}

// handoffInterval is how often hints are handed to connected peers.
var handoffInterval = func() time.Duration {
	if d, err := time.ParseDuration(os.Getenv("UNITDB_HANDOFF_INTERVAL")); err == nil && d > 0 {
		return d
	}
	return defaultHandoffInterval
}()

// errRejected is a forwarded request the receiver did not take: its own
// ring does not give it the topic. The request was not processed.
var errRejected = errors.New("cluster: the node did not take the request: not its topic by its ring")

// clusterNodeConfig is one entry of cluster_config.nodes.
type clusterNodeConfig struct {
	Name    string `json:"name"`
	Addr    string `json:"addr"`
	TLSAddr string `json:"tls_addr"`
}

// clusterFailoverConfig is cluster_config.failover.
type clusterFailoverConfig struct {
	Enabled       bool `json:"enabled"`
	Heartbeat     int  `json:"heartbeat"`
	VoteAfter     int  `json:"vote_after"`
	NodeFailAfter int  `json:"node_fail_after"`
}

// clusterConfig is cluster_config. Its keys are an external contract.
type clusterConfig struct {
	Node             string                 `json:"node"`
	Nodes            []clusterNodeConfig    `json:"nodes"`
	Replicas         int                    `json:"replicas"`
	AsyncReplication bool                   `json:"async_replication"`
	RebuildTTL       string                 `json:"rebuild_ttl"`
	DrainTimeout     string                 `json:"drain_timeout"`
	RingVersion      int                    `json:"ring_version"`
	Failover         *clusterFailoverConfig `json:"failover"`
	TLS              *clusterTLSConfig      `json:"tls"`
}

// Cluster is this node's part of the cluster. Globals.Cluster is nil for a
// single server, and every caller takes nil as such.
type Cluster struct {
	thisNodeName string
	// nodes are the peers: every configured node but this one. Not changed
	// after ClusterInit.
	nodes map[string]*ClusterNode
	// allNodes are the configured nodes' names, sorted.
	allNodes []string

	replicas         int
	asyncReplication bool
	rebuildTTL       time.Duration
	drainTimeout     time.Duration
	selfAddr         string

	tls *clusterTLS
	// fo is the membership state, nil when failover is off.
	fo     *clusterFailover
	health clusterHealth

	// The ring state, under ringMu: ring is over the live set ringNodes,
	// fullRing over allNodes, both of version ringVersion. ringTarget is
	// cluster_config.ring_version.
	ringMu      sync.RWMutex
	ring        *rh.Ring
	ringNodes   []string
	ringVersion int
	ringTarget  int
	fullRing    *rh.Ring
	// clusterRing is the ring version this node last saw the cluster route
	// by; 0 before any.
	clusterRing atomic.Int32

	stopped     atomic.Bool
	leaving     atomic.Bool
	rebuilding  atomic.Bool
	reconciling atomic.Bool
	started     atomic.Bool

	// departing are the peers that said they leave, kept out of every live
	// set until they run again.
	departMu  sync.Mutex
	departing map[string]bool

	// pending are hints the store refused, retried later.
	pendingMu sync.Mutex
	pending   []pendingHint

	seen         seenIDs
	replSeq      atomic.Uint64
	replInFlight atomic.Int64

	// incarnation tells this run of the process from others.
	incarnation int64

	proxyMu sync.Mutex
	proxies map[proxyKey]*standIn

	quit      chan struct{}
	stopOnce  sync.Once
	listenMu  sync.Mutex
	listeners []net.Listener
	// starting is set while Start dials the peers the first time: a peer of
	// an incompatible protocol then stops this node.
	starting atomic.Bool
}

// ClusterInit reads cluster_config and sets Globals.Cluster, unless the node
// runs alone: no config, or no node name. self, if set, names this node
// (-cluster_node). It returns this node's 1-based index among the sorted
// node names, 1 for a single server. It neither needs the store nor listens.
func ClusterInit(configString json.RawMessage, self *string) int {
	if Globals.Cluster != nil {
		log.Fatal("ClusterInit", "cluster already initialized", errors.New("ClusterInit called twice"))
	}
	if len(configString) == 0 {
		log.Info("ClusterInit", "no cluster_config: running as a single server")
		return 1
	}
	var conf clusterConfig
	if err := json.Unmarshal(configString, &conf); err != nil {
		log.Fatal("ClusterInit", "unable to parse cluster_config", err)
	}
	name := conf.Node
	if self != nil && *self != "" {
		name = *self
	}
	if name == "" {
		log.Info("ClusterInit", "no cluster node name: running as a single server")
		return 1
	}
	c, err := newCluster(name, &conf)
	if err != nil {
		log.Fatal("ClusterInit", "invalid cluster_config", err)
	}
	Globals.Cluster = c
	logRingVersion(c.ringVersion)
	for i, n := range c.allNodes {
		if n == name {
			return i + 1
		}
	}
	return 1
}

// newCluster builds the cluster of node name from conf.
func newCluster(name string, conf *clusterConfig) (*Cluster, error) {
	c := &Cluster{
		thisNodeName:     name,
		nodes:            make(map[string]*ClusterNode),
		replicas:         conf.Replicas,
		asyncReplication: conf.AsyncReplication,
		rebuildTTL:       24 * time.Hour,
		drainTimeout:     10 * time.Second,
		ringTarget:       conf.RingVersion,
		departing:        make(map[string]bool),
		proxies:          make(map[proxyKey]*standIn),
		quit:             make(chan struct{}),
		incarnation:      time.Now().UnixNano(),
	}
	if c.replicas <= 0 {
		c.replicas = 2
	}
	var err error
	if conf.RebuildTTL != "" {
		if c.rebuildTTL, err = time.ParseDuration(conf.RebuildTTL); err != nil {
			return nil, fmt.Errorf("rebuild_ttl: %v", err)
		}
	}
	if conf.DrainTimeout != "" {
		if c.drainTimeout, err = time.ParseDuration(conf.DrainTimeout); err != nil {
			return nil, fmt.Errorf("drain_timeout: %v", err)
		}
	}
	tlsAddr := ""
	for _, nc := range conf.Nodes {
		if nc.Name == "" {
			return nil, errors.New("a node without a name")
		}
		if containsNode(c.allNodes, nc.Name) {
			return nil, fmt.Errorf("node %q is configured twice", nc.Name)
		}
		c.allNodes = append(c.allNodes, nc.Name)
		if nc.Name == name {
			c.selfAddr, tlsAddr = nc.Addr, nc.TLSAddr
			continue
		}
		c.nodes[nc.Name] = &ClusterNode{
			name:       nc.Name,
			address:    nc.Addr,
			tlsAddress: nc.TLSAddr,
			owner:      c,
			repl:       make(chan replicaItem, replicationQueueSize),
			replDone:   make(chan struct{}),
		}
	}
	if !containsNode(c.allNodes, name) {
		return nil, fmt.Errorf("this node, %q, is not in cluster_config.nodes", name)
	}
	sort.Strings(c.allNodes)
	if conf.TLS != nil {
		if c.tls, err = loadClusterTLS(conf.TLS, name, tlsAddr); err != nil {
			return nil, fmt.Errorf("invalid cluster_config.tls: %v", err)
		}
	}
	if c.selfAddr == "" && (c.tls == nil || !c.tls.require) {
		return nil, fmt.Errorf("node %q has no addr", name)
	}
	if fo := conf.Failover; fo != nil && fo.Enabled {
		if len(c.allNodes) < 3 {
			log.ErrLogger.Warn().Str("context", "ClusterInit").Int("nodes", len(c.allNodes)).Msg("failover needs at least 3 nodes: it is off")
		} else {
			c.fo = newFailover(fo)
		}
	}
	c.ringVersion = initialRingVersion(c.ringTarget)
	c.fullRing = newRing(c.ringVersion, c.allNodes)
	c.ringNodes = append([]string(nil), c.allNodes...)
	c.ring = newRing(c.ringVersion, c.ringNodes)
	return c, nil
}

// Start starts serving the cluster: it listens for peers, connects to them,
// starts replication, membership and catching up, and returns once its
// peers have sent it their clients' subscriptions it holds, so that the
// service can take clients.
func (c *Cluster) Start() {
	if err := c.listen(); err != nil {
		log.Fatal("cluster.Start", "unable to listen for cluster traffic", err)
	}
	c.started.Store(true)

	// Dial every peer once, at the same time, before anything else: a peer
	// that speaks an incompatible protocol stops this node here.
	c.starting.Store(true)
	var wg sync.WaitGroup
	for _, n := range c.nodes {
		wg.Add(1)
		go func(n *ClusterNode) {
			defer wg.Done()
			n.redial(true)
		}(n)
	}
	wg.Wait()
	c.starting.Store(false)

	for _, n := range c.nodes {
		go n.reconnectLoop()
		go n.replicateLoop(c.thisNodeName)
	}
	go c.handoffLoop()

	store.OnLogChange = c.onLogChange
	c.loadSeen()

	if c.replicas >= 2 && store.WasEmpty() && hasCapability(capReplicate) {
		c.rebuilding.Store(true)
		go c.rebuild()
	}
	if restoreRequested {
		if c.replicas >= 2 && hasCapability(capReconcile) {
			c.reconciling.Store(true)
			for name := range c.nodes {
				c.dropMessageHints(name)
			}
			go c.reconcileAll()
		} else {
			markRestored(restoredPath)
		}
	}
	if c.fo != nil {
		go c.membershipLoop()
	}
	c.startResync()
	log.ErrLogger.Info().Str("context", "cluster.Start").Str("node", c.thisNodeName).Strs("nodes", c.allNodes).Msg("cluster started")
}

// listen opens the plain listener, unless TLS is required, and the TLS one
// when configured.
func (c *Cluster) listen() error {
	if c.tls == nil || !c.tls.require {
		l, err := net.Listen("tcp", c.selfAddr)
		if err != nil {
			return err
		}
		c.addListener(l)
		go c.servePlain(l)
	}
	if c.tls != nil {
		l, err := net.Listen("tcp", c.tls.listenOn)
		if err != nil {
			return err
		}
		tl := tlsListener(l, c.tls)
		c.addListener(tl)
		go c.serveTLS(tl)
	}
	return nil
}

func (c *Cluster) addListener(l net.Listener) {
	c.listenMu.Lock()
	c.listeners = append(c.listeners, l)
	c.listenMu.Unlock()
}

// startResync asks each peer that answers, and can, to send again the
// subscriptions of its clients this node holds, and waits for each.
func (c *Cluster) startResync() {
	var wg sync.WaitGroup
	for _, n := range c.nodes {
		if n.currentLink() == nil || !n.supports(capResync) {
			continue
		}
		wg.Add(1)
		go func(n *ClusterNode) {
			defer wg.Done()
			var unused bool
			if err := n.callTimeout("Cluster.Resync", &ResyncReq{Node: c.thisNodeName}, &unused, startResyncWait); err != nil && !n.lacks(err, capResync) {
				log.ErrLogger.Warn().Err(err).Str("context", "cluster.Start").Str("peer", n.name).Msg("peer did not resend its subscriptions")
			}
		}(n)
	}
	wg.Wait()
}

// drain takes this node out of the cluster before its clients are closed:
// out of its own ring at once, then out of the others', then its hints and
// replication queues to the others; never longer than drain_timeout.
func (c *Cluster) drain() {
	if c == nil || !c.leaving.CompareAndSwap(false, true) {
		return
	}
	start := time.Now()
	deadline := start.Add(c.drainTimeout)
	wasLeader := c.resign()
	c.setLive(c.getRingNodes())

	// The others drop this node from their live sets now, not once they
	// notice it is gone.
	wait := leaveWait
	if left := time.Until(deadline); left < wait {
		wait = left
	}
	var wg sync.WaitGroup
	for _, n := range c.nodes {
		wg.Add(1)
		go func(n *ClusterNode) {
			defer wg.Done()
			var unused bool
			if err := n.callTimeout("Cluster.Leave", &LeaveReq{Node: c.thisNodeName, WasLeader: wasLeader}, &unused, wait); err != nil {
				log.ErrLogger.Debug().Err(err).Str("peer", n.name).Msg("cluster: peer not told this node leaves")
			}
		}(n)
	}
	wg.Wait()

	// Hand off what is kept for the others, and wait for the queues.
	done := make(chan struct{})
	go func() {
		var wg sync.WaitGroup
		for _, n := range c.nodes {
			if n.currentLink() == nil {
				continue
			}
			wg.Add(1)
			go func(n *ClusterNode) {
				defer wg.Done()
				c.handoff(n)
			}(n)
		}
		wg.Wait()
		for !c.replicationIdle() && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Until(deadline)):
		log.ErrLogger.Warn().Str("context", "cluster.drain").Msg("drain_timeout reached: leaving with work left")
	}
	log.ErrLogger.Info().Str("context", "cluster.drain").Dur("took", time.Since(start)).Msg("left the cluster")
}

// replicationIdle reports whether every replication queue is empty and no
// batch is in flight.
func (c *Cluster) replicationIdle() bool {
	if c.replInFlight.Load() != 0 {
		return false
	}
	for _, n := range c.nodes {
		if len(n.repl) > 0 {
			return false
		}
	}
	return true
}

// shutdown stops the cluster's work, after the store closed. Globals.Cluster
// stays set.
func (c *Cluster) shutdown() {
	if c == nil {
		return
	}
	c.stopOnce.Do(func() {
		c.stopped.Store(true)
		close(c.quit)
		c.listenMu.Lock()
		for _, l := range c.listeners {
			l.Close()
		}
		c.listenMu.Unlock()
		for _, n := range c.nodes {
			n.closeLink()
		}
		store.OnLogChange = nil
	})
}

// getRing returns the ring the cluster routes by.
func (c *Cluster) getRing() *rh.Ring {
	c.ringMu.RLock()
	defer c.ringMu.RUnlock()
	if c.ring == nil {
		return newRing(latestRingVersion, nil)
	}
	return c.ring
}

// getRingNodes returns the live set: the nodes of the ring.
func (c *Cluster) getRingNodes() []string {
	c.ringMu.RLock()
	defer c.ringMu.RUnlock()
	return c.ringNodes
}

// setLive makes live the live set, without the nodes leaving, and rebuilds
// the ring at the current ring version. The nodes it adds get their hints
// and are asked to resend their clients' subscriptions; every client's
// subscriptions move to where the new ring holds them.
func (c *Cluster) setLive(live []string) {
	next := c.withoutDeparting(live)
	c.ringMu.Lock()
	prev := c.ringNodes
	c.ringNodes = next
	c.ring = newRing(c.ringVersion, next)
	c.ringMu.Unlock()
	var added []string
	for _, n := range next {
		if !containsNode(prev, n) {
			added = append(added, n)
		}
	}
	if !sameNodes(prev, next) {
		log.ErrLogger.Info().Str("context", "cluster.setLive").Strs("live", next).Strs("added", added).Msg("cluster: live set changed")
	}
	if !c.started.Load() {
		return
	}
	go c.afterRingChange(added)
}

// afterRingChange catches up the nodes added to the live set, and moves the
// clients' subscriptions.
func (c *Cluster) afterRingChange(added []string) {
	resend := make(map[string]bool, len(added))
	for _, name := range added {
		n := c.nodes[name]
		if n == nil {
			continue
		}
		resend[name] = true
		go c.handoff(n)
		go c.askResync(n)
	}
	c.rebalance(resend)
}

// withoutDeparting returns nodes, sorted, without the peers leaving and
// without this node if it leaves.
func (c *Cluster) withoutDeparting(nodes []string) []string {
	c.departMu.Lock()
	defer c.departMu.Unlock()
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if c.departing[n] || (n == c.thisNodeName && c.leaving.Load()) {
			continue
		}
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// isDeparting reports whether peer said it leaves.
func (c *Cluster) isDeparting(name string) bool {
	c.departMu.Lock()
	defer c.departMu.Unlock()
	return c.departing[name]
}

// peerRestarted forgets what this node kept of a peer's previous run: that
// it was leaving, and the sessions it held for its clients.
func (c *Cluster) peerRestarted(n *ClusterNode) {
	c.departMu.Lock()
	delete(c.departing, n.name)
	c.departMu.Unlock()
	c.dropStandInsOf(n.name)
}

// isWildcardTopic reports whether topic has wildcards: it has no owner.
func isWildcardTopic(topic string) bool {
	return strings.Contains(topic, topicWildcard) || strings.HasSuffix(topic, topicMultiWildcard)
}

// topicRingKey is the ring key of a topic: its contract and name, without
// options.
func topicRingKey(contract uint32, topic string) string {
	if i := strings.IndexByte(topic, '?'); i >= 0 {
		topic = topic[:i]
	}
	return strconv.FormatUint(uint64(contract), 10) + "/" + topic
}

// sessionRingKey is the ring key of a session.
func sessionRingKey(sessID uint32) string {
	return "session/" + strconv.FormatUint(uint64(sessID), 10)
}

func containsNode(nodes []string, name string) bool {
	for _, n := range nodes {
		if n == name {
			return true
		}
	}
	return false
}

// sameNodes reports whether two sorted lists name the same nodes.
func sameNodes(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// hasOlderPeers reports whether the cluster has nodes older than this one.
// Peers speak protocol 3 (peerwire) or not at all: an older node can't join,
// so there are none.
func (c *Cluster) hasOlderPeers() bool { return false }

// The topic wildcards: one level, and any number.
const (
	topicWildcard      = "*"
	topicMultiWildcard = "..."
)

// contains reports whether list holds s.
func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
