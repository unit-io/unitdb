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
	"os"
	"strings"
	"sync"

	"github.com/unit-io/unitdb/server/internal/peerwire"
)

// Nodes of different versions can run in one cluster: each node tells the
// others its protocol version and what it can do, and a node calls another
// only for what that one can do, falling back otherwise. See
// docs/rolling-deploys.md.
//
// A node's hello on each connection says what it can do, and so do the
// leader's heartbeats, for every node, and their answers. A node not heard
// from yet is taken to do everything: a call it then refuses, as a method
// it lacks, marks that capability as missing, and the call falls back.

// clusterProtocolVersion is the version of the protocol between nodes. 3 is
// the peerwire protocol (cluster_transport.go); 2 and before spoke net/rpc.
const clusterProtocolVersion = 3

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
	// capRevocations: the security state sent between nodes (Revocations).
	capRevocations = "revocations"
	// capService: the node sets a forwarded connection's insecure flag only
	// for a trusted service (a client id with uid.AllowService, or a
	// connection one vouched for), so its flag can be taken.
	capService = "service"
	// capTLS: the node listens for cluster connections over mutual TLS. It
	// is not in allCapabilities: a node has it when cluster_config.tls is
	// set.
	capTLS = "tls"
)

var allCapabilities = []string{capReplicate, capDeliver, capSessions, capResync, capRevocations, capService, capReconcile}

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
	return &peerwire.RemoteError{Code: peerwire.CodeNoMethod, Message: "cluster: method off (capability " + cap + " not enabled)"}
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
	var re *peerwire.RemoteError
	if !errors.As(err, &re) || re.Code != peerwire.CodeNoMethod {
		return false
	}
	i := strings.Index(re.Message, "capability ")
	return i < 0 || strings.Contains(re.Message[i:], cap)
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
	n.caps.known = &nc
	n.caps.missing = nil
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

// knownToSupport reports whether the node told this one it can do cap. Unlike
// supports, a node not heard from yet is taken not to.
func (n *ClusterNode) knownToSupport(cap string) bool {
	nc, ok := n.capabilities()
	return ok && contains(nc.Capabilities, cap)
}
