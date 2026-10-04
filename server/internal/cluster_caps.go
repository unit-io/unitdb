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
	"errors"
	"net/rpc"
	"os"
	"strings"
	"sync"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
)

// Nodes of different versions can run in one cluster: each node tells the
// others its protocol version and what it can do, and a node calls another
// only for what that one can do, falling back otherwise. See
// docs/rolling-deploys.md.
//
// The leader's pings carry what each node can do, and a ping's answer what
// the pinged node can. A node not heard from yet is taken to do everything:
// a call it then refuses, as a method it lacks, marks that capability as
// missing, and the call falls back.

// clusterProtocolVersion is the version of the protocol between nodes.
const clusterProtocolVersion = 2

// Capabilities a node can have.
const (
	// capReplicate: stored messages sent to replicas (Replicate's entries),
	// hints of them, and rebuilds (RebuildTopics, RebuildHistory).
	capReplicate = "replicate"
	// capDeliver: messages for a node's clients in one call (Deliver).
	capDeliver = "deliver"
	// capSessions: session log changes sent to replicas (Replicate's log),
	// and sessions fetched and dropped (FetchSession, ForgetSession).
	capSessions = "sessions"
	// capResync: a node back in the ring asked for its clients'
	// subscriptions (Resync).
	capResync = "resync"
	// capService: the node sets a forwarded connection's Insecure only for a
	// trusted service's connection (a client id with uid.AllowService, or a
	// connection one vouched for with unitdb/service), so the flag can be
	// taken from it. An older node sends its client's own CONNECT flag.
	capService = "service"
	// capV2Keys: the node reads v2 client ids and v2 topic keys. A v0.6.0
	// node issues v2 ones only once every other node is known to have it,
	// and v1 ones until then, so it is still sent although no call depends
	// on it: since v0.7.0 a node issues and reads v2 ones only. A node
	// without it (v0.5.0 or before) can't serve a v0.7.0 node's clients, nor
	// take its keys: it is warned of (warnV1Peer).
	capV2Keys = "v2keys"
	// capTLS: the node listens for cluster connections over mutual TLS. It
	// is not in allCapabilities: a node has it when cluster_config.tls is
	// set. No call depends on it; it tells which nodes have moved to TLS.
	capTLS = "tls"
	// capRevocations: the node holds the cluster's security state, what
	// was revoked in each contract (unitdb/revoke), and takes it from the
	// others (Revocations). An older node is sent none, and refuses no id
	// or key for being revoked.
	capRevocations = "revocations"
)

var allCapabilities = []string{capReplicate, capDeliver, capSessions, capResync, capService, capV2Keys, capRevocations, capReconcile}

// ownCapabilities are what this node can do: all of them, unless the
// UNITDB_CLUSTER_CAPS environment variable lists fewer ("none" for none), so
// that tests can run a node as an older one.
var ownCapabilities = func() map[string]bool {
	caps := make(map[string]bool)
	env, set := os.LookupEnv("UNITDB_CLUSTER_CAPS")
	if !set {
		for _, c := range allCapabilities {
			caps[c] = true
		}
		return caps
	}
	for _, c := range strings.Split(env, ",") {
		if c = strings.TrimSpace(c); c != "" && c != "none" {
			caps[c] = true
		}
	}
	return caps
}()

// hasCapability reports whether this node can do cap.
func hasCapability(cap string) bool {
	return ownCapabilities[cap]
}

// NodeCapabilities is a node's protocol version and what it can do.
type NodeCapabilities struct {
	Version      int
	Capabilities []string
	// RingVersions are the ring versions the node supports; none for a node
	// built before rings were versioned, which supports preVersionRing.
	RingVersions []int
}

// ownNodeCapabilities returns this node's protocol version and capabilities.
func ownNodeCapabilities() NodeCapabilities {
	nc := NodeCapabilities{Version: clusterProtocolVersion, RingVersions: ringVersions()}
	for _, c := range allCapabilities {
		if ownCapabilities[c] {
			nc.Capabilities = append(nc.Capabilities, c)
		}
	}
	if c := Globals.Cluster; c != nil && c.tls != nil {
		nc.Capabilities = append(nc.Capabilities, capTLS)
	}
	return nc
}

// errCapabilityOff is what a node answers for a call it cannot take, as a
// method it lacks, so that the caller falls back as for an older node.
func errCapabilityOff(cap string) error {
	return errors.New("rpc: can't find method (capability " + cap + " not enabled)")
}

// refuse returns an error for a call that needs cap, if this node lacks it.
func refuse(cap string) error {
	if !hasCapability(cap) {
		return errCapabilityOff(cap)
	}
	return nil
}

// missingMethod reports whether err is a node's answer that it lacks the
// method called, or the capability cap it needs.
func missingMethod(err error, cap string) bool {
	var se rpc.ServerError
	if !errors.As(err, &se) || !strings.Contains(string(se), "can't find method") {
		return false
	}
	i := strings.Index(string(se), "capability ")
	return i < 0 || strings.Contains(string(se)[i:], cap)
}

// peerCapabilities is what this node knows of what another node can do.
type peerCapabilities struct {
	mu      sync.Mutex
	known   *NodeCapabilities
	missing map[string]bool
}

// setCapabilities records what the node told it can do.
func (n *ClusterNode) setCapabilities(nc NodeCapabilities) {
	n.caps.mu.Lock()
	defer n.caps.mu.Unlock()
	if n.caps.known == nil || hasCap(n.caps.known.Capabilities, capV2Keys) {
		warnV1Peer(n.name, nc)
	}
	n.caps.known = &nc
	n.caps.missing = nil
}

// hasCap reports whether caps holds cap.
func hasCap(caps []string, cap string) bool {
	for _, c := range caps {
		if c == cap {
			return true
		}
	}
	return false
}

// warnV1Peer warns, when node first tells it can't do capV2Keys, that the
// node reads no v2 client ids or topic keys, the only ones this node issues
// and reads: such a node runs v0.5.0 or before, and must be upgraded to
// v0.6.0 before the cluster moves to v0.7.0 (docs/rolling-deploys.md).
func warnV1Peer(node string, nc NodeCapabilities) {
	if hasCap(nc.Capabilities, capV2Keys) {
		return
	}
	log.ErrLogger.Warn().Str("context", "cluster").Str("node", node).Msg("the node reads no v2 client ids or topic keys, the only ones this node issues and takes: it runs v0.5.0 or before; upgrade it to v0.6.0 first, then to v0.7.0")
}

// capabilities returns what the node told it can do, if it did.
func (n *ClusterNode) capabilities() (NodeCapabilities, bool) {
	n.caps.mu.Lock()
	defer n.caps.mu.Unlock()
	if n.caps.known == nil {
		return NodeCapabilities{}, false
	}
	return *n.caps.known, true
}

// supports reports whether the node can do cap: what it told, or yes if it
// has not told yet and not refused cap.
func (n *ClusterNode) supports(cap string) bool {
	n.caps.mu.Lock()
	defer n.caps.mu.Unlock()
	if n.caps.missing[cap] {
		return false
	}
	if n.caps.known == nil {
		return true
	}
	for _, c := range n.caps.known.Capabilities {
		if c == cap {
			return true
		}
	}
	return false
}

// knownToSupport reports whether the node told it can do cap. Unlike
// supports, a node not heard from yet is taken not to: for what a node is
// trusted with, rather than a call that falls back.
func (n *ClusterNode) knownToSupport(cap string) bool {
	nc, ok := n.capabilities()
	if !ok {
		return false
	}
	for _, c := range nc.Capabilities {
		if c == cap {
			return true
		}
	}
	return false
}

// hasOlderPeers reports whether a node of the cluster is known to run a
// version before capService, such as v0.5.0. A node not heard from yet is
// taken not to be: what this enables (sessions found as such nodes find
// them, see the CONNECT handler) is weaker than what replaces it.
func (c *Cluster) hasOlderPeers() bool {
	if c == nil {
		return false
	}
	for _, n := range c.nodes {
		if !n.supports(capService) {
			return true
		}
	}
	return false
}

// hasNow records that the node can do cap after all, as a call of it the
// node made shows, until the node tells what it can do again.
func (n *ClusterNode) hasNow(cap string) {
	n.caps.mu.Lock()
	defer n.caps.mu.Unlock()
	delete(n.caps.missing, cap)
}

// lacks reports whether err is the node's answer that it cannot do cap, and
// if so records it, until the node tells what it can do again.
func (n *ClusterNode) lacks(err error, cap string) bool {
	if !missingMethod(err, cap) {
		return false
	}
	n.caps.mu.Lock()
	defer n.caps.mu.Unlock()
	if n.caps.missing == nil {
		n.caps.missing = make(map[string]bool)
	}
	n.caps.missing[cap] = true
	return true
}
