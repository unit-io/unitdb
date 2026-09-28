/*
 * Copyright 2020 Saffat Technologies, Ltd.
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

import (
	stdlog "log"
	"os"
	"strconv"
	"strings"
	"time"

	rh "github.com/unit-io/unitdb/server/internal/pkg/hash"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/store"
)

// The ring is versioned, so that it can change while nodes of different
// versions run together: every node builds the rings of the versions it
// supports, and routes by the one the leader's pings name. The leader picks
// the highest version every live node supports, up to cluster_config's
// "ring_version", and switches to it as a rehash: subscriptions move with
// the rebalance. See docs/rolling-deploys.md.

// ringSpec is how a version of the ring places keys.
type ringSpec struct {
	points int     // points each node has on the ring
	hash   rh.Hash // nil for the ring's default
}

var ringSpecs = map[int]ringSpec{
	// FNV-1a alone, 20 points per node.
	1: {points: 20, hash: rh.FNV32a},
	// FNV-1a mixed by fmix32, 160 points per node.
	2: {points: clusterHashReplicas},
}

// latestRingVersion is the highest ring version this node supports.
const latestRingVersion = 2

// preVersionRing is the ring of nodes that tell no ring versions: those
// built before rings were versioned.
const preVersionRing = 2

// ownRingVersions are the ring versions this node supports: all of them,
// unless the UNITDB_RING_VERSIONS environment variable lists fewer, so that
// tests can run a node as an older one.
var ownRingVersions = func() map[int]bool {
	versions := make(map[int]bool)
	if env, set := os.LookupEnv("UNITDB_RING_VERSIONS"); set {
		for _, s := range strings.Split(env, ",") {
			if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && ringSpecs[v].points > 0 {
				versions[v] = true
			}
		}
		return versions
	}
	for v := range ringSpecs {
		versions[v] = true
	}
	return versions
}()

func supportsRingVersion(v int) bool {
	return ownRingVersions[v]
}

// ringVersions returns the ring versions this node supports, lowest first.
func ringVersions() []int {
	var versions []int
	for v := 1; v <= latestRingVersion; v++ {
		if ownRingVersions[v] {
			versions = append(versions, v)
		}
	}
	return versions
}

// peerRingVersions returns the ring versions a node supports, as it told.
func peerRingVersions(nc NodeCapabilities) []int {
	if len(nc.RingVersions) == 0 {
		return []int{preVersionRing}
	}
	return nc.RingVersions
}

// newRing builds the ring of version over nodes.
func newRing(version int, nodes []string) *rh.Ring {
	spec := ringSpecs[version]
	ring := rh.NewRing(spec.points, spec.hash)
	ring.Add(nodes...)
	return ring
}

// initialRingVersion is the version a node starts with, before the leader's
// pings name one: the highest it supports, up to target (0 for the latest).
func initialRingVersion(target int) int {
	if target <= 0 || target > latestRingVersion {
		target = latestRingVersion
	}
	for v := target; v >= 1; v-- {
		if supportsRingVersion(v) {
			return v
		}
	}
	return preVersionRing
}

// chooseRingVersion returns the ring version the leader routes the cluster
// by: the highest, up to the target, that this node and every live node
// support. Until every live node has told what it supports, it keeps the
// current one.
func (c *Cluster) chooseRingVersion(live []*ClusterNode) int {
	current := c.getRingVersion()
	peers := make([][]int, 0, len(live))
	for _, n := range live {
		nc, known := n.capabilities()
		if !known {
			return current
		}
		peers = append(peers, peerRingVersions(nc))
	}
	for v := initialRingVersion(c.ringTarget); v >= 1; v-- {
		if !supportsRingVersion(v) {
			continue
		}
		all := true
		for _, versions := range peers {
			found := false
			for _, pv := range versions {
				found = found || pv == v
			}
			all = all && found
		}
		if all {
			return v
		}
	}
	return current
}

// getRingVersion returns the version the ring is built by.
func (c *Cluster) getRingVersion() int {
	c.ringMu.RLock()
	defer c.ringMu.RUnlock()
	return c.ringVersion
}

// setRingVersion sets the version the ring is built by, and rebuilds the
// ring of every configured node with it; the caller rehashes the ring of
// the live nodes. It logs the version for operators, and tests, to see.
func (c *Cluster) setRingVersion(v int) {
	c.ringMu.Lock()
	c.ringVersion = v
	c.fullRing = newRing(v, c.allNodes)
	c.ringMu.Unlock()
	logRingVersion(v)
}

// logRingVersion logs the version the ring is built by, for operators, and
// tests, to see.
func logRingVersion(v int) {
	stdlog.Printf("cluster: ring version %d", v)
}

// getFullRing returns the ring of every configured node, live or not.
func (c *Cluster) getFullRing() *rh.Ring {
	c.ringMu.RLock()
	defer c.ringMu.RUnlock()
	return c.fullRing
}

// adoptRingVersion sets the ring version the cluster routes by, as the
// leader's pings name it or as the leader switches it. A change of the
// version the cluster was seen routing by moves stored messages to the
// topics' new holders; the first version seen, as when this node starts,
// moves none. The caller rehashes if the version changed.
func (c *Cluster) adoptRingVersion(v int) {
	prev := int(c.clusterRing.Swap(int32(v)))
	if v != c.getRingVersion() {
		c.setRingVersion(v)
	}
	if prev != 0 && prev != v {
		go c.moveHistory(prev, v)
	}
}

// moveHistory hands the messages this node stores to the holders a switch of
// the ring from version from to version to gives them, that did not hold them
// before. Each topic's messages come from one node, the first of its holders
// before the switch, so that a new holder gets each once. The old holders
// keep theirs until they expire. It logs when it is done, for operators, and
// tests, to see.
func (c *Cluster) moveHistory(from, to int) {
	nodes := c.getRingNodes()
	before, after := newRing(from, nodes), newRing(to, nodes)
	topics, moved := 0, 0
	for _, t := range store.Message.Topics() {
		key := topicRingKey(t.Contract, t.Topic)
		was := before.GetN(key, c.replicas)
		if len(was) == 0 || was[0] != c.thisNodeName {
			continue // another node sends this topic's messages
		}
		var targets []*ClusterNode
		for _, h := range after.GetN(key, c.replicas) {
			if n := c.nodes[h]; n != nil && !containsNode(was, h) && n.supports(capReplicate) {
				targets = append(targets, n)
			}
		}
		if len(targets) == 0 {
			continue
		}
		history, err := store.Message.History(t.Contract, t.Topic)
		if err != nil || len(history) == 0 {
			continue
		}
		entries := make([]ReplicaEntry, 0, len(history))
		for _, h := range history {
			expiresAt := h.ExpiresAt
			if !h.Known {
				expiresAt = time.Now().Add(c.rebuildTTL).Unix()
			}
			entries = append(entries, ReplicaEntry{Contract: t.Contract, Topic: t.Topic, Payload: h.Payload, ExpiresAt: expiresAt})
		}
		for _, n := range targets {
			c.sendHistory(n, entries)
		}
		topics++
		moved += len(entries)
	}
	stdlog.Printf("cluster: moved history for ring version %d: %d topics, %d messages", to, topics, moved)
}

// sendHistory sends messages to a node's replica store, in batches, trying
// each batch again for a while if the node does not take it.
func (c *Cluster) sendHistory(n *ClusterNode, entries []ReplicaEntry) {
	for start := 0; start < len(entries); start += replicationBatchSize {
		end := start + replicationBatchSize
		if end > len(entries) {
			end = len(entries)
		}
		req := &ReplicateReq{Node: c.thisNodeName, Entries: entries[start:end]}
		var err error
		for attempt := 0; attempt < rebuildAttempts; attempt++ {
			var unused bool
			if err = n.callTimeout("Cluster.Replicate", req, &unused, rebuildTimeout); err == nil || n.lacks(err, capReplicate) {
				break
			}
			time.Sleep(rebuildRetry)
		}
		if err != nil {
			log.ErrLogger.Error().Err(err).Str("context", "cluster.sendHistory").Int("messages", end-start).Msg("history not moved to " + n.name)
			return
		}
	}
}
