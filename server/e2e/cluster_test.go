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

package e2e

// Cluster tests run a real 3-node cluster, each node its own server process
// with its own ports and data, with failover enabled. Contracts are owned by a
// node chosen by the same ring hash the server uses, so the tests pick
// contracts owned by each node and cover every subscriber/publisher/owner
// combination.

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/message/security"
	rh "github.com/unit-io/unitdb/server/internal/pkg/hash"
)

// Must match internal/cluster.go.
const clusterHashReplicas = 20

type clusterNode struct {
	name string
	addr string // cluster RPC address
	*server
}

type cluster struct {
	t     *testing.T
	nodes []*clusterNode
}

func (c *cluster) node(name string) *clusterNode {
	for _, n := range c.nodes {
		if n.name == name {
			return n
		}
	}
	c.t.Fatalf("no node %q", name)
	return nil
}

// startCluster starts a cluster of the named nodes with failover enabled.
func startCluster(t *testing.T, names ...string) *cluster {
	return startClusterWith(t, "", names...)
}

func startClusterWith(t *testing.T, logLevel string, names ...string) *cluster {
	t.Helper()
	type nodeConf struct {
		Name string `json:"name"`
		Addr string `json:"addr"`
	}
	c := &cluster{t: t}
	var confNodes []nodeConf
	for _, name := range names {
		n := &clusterNode{name: name, addr: fmt.Sprintf("127.0.0.1:%d", freePort(t))}
		c.nodes = append(c.nodes, n)
		confNodes = append(confNodes, nodeConf{name, n.addr})
	}
	conf, _ := json.Marshal(map[string]interface{}{
		"self":  "",
		"nodes": confNodes,
		"failover": map[string]interface{}{
			"enabled":         true,
			"heartbeat":       100,
			"vote_after":      8,
			"node_fail_after": 16,
		},
	})
	for _, n := range c.nodes {
		n.server = startServerWith(t, serverOpts{cluster: string(conf), args: []string{"-cluster_self", n.name}, logLevel: logLevel})
	}
	return c
}

// owner returns the node owning contract, as the server's ring computes it
// over the given live nodes.
func owner(contract uint32, live ...string) string {
	ring := rh.NewRing(clusterHashReplicas, nil)
	ring.Add(live...)
	return ring.Get(fmt.Sprint(contract))
}

// contractOwnedBy returns a contract the ring assigns to name.
func contractOwnedBy(name string, live ...string) uint32 {
	for c := uint32(0x0c1a0000); ; c++ {
		if owner(c, live...) == name {
			return c
		}
	}
}

var (
	electedSelf = regexp.MustCompile(`Elected myself as a new leader`)
	leaderIs    = regexp.MustCompile(`leader (?:set to )?'([a-z]+)'(?: elected)?`)
)

// leaderSeen returns the leader each node last logged, or "" if none.
func (c *cluster) leaderSeen(n *clusterNode) string {
	leader := ""
	for _, line := range strings.Split(n.logs.String(), "\n") {
		if electedSelf.MatchString(line) {
			leader = n.name
		} else if m := leaderIs.FindStringSubmatch(line); m != nil && !strings.Contains(line, "wrong leader") {
			leader = m[1]
		}
	}
	return leader
}

// waitLeader waits until every live node agrees on one leader and returns it.
func (c *cluster) waitLeader(live []*clusterNode, timeout time.Duration) (string, error) {
	deadline := time.Now().Add(timeout)
	for {
		leaders := map[string]bool{}
		for _, n := range live {
			leaders[c.leaderSeen(n)] = true
		}
		if len(leaders) == 1 && !leaders[""] {
			for l := range leaders {
				return l, nil
			}
		}
		if time.Now().After(deadline) {
			seen := map[string]string{}
			for _, n := range live {
				seen[n.name] = c.leaderSeen(n)
			}
			return "", fmt.Errorf("nodes did not agree on a leader: %v", seen)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// delivers subscribes on sub and publishes on pub, both on contract, and
// reports whether the subscriber received the published payload. Clients
// connect insecure (no topic keys).
func delivers(t *testing.T, sub, pub *clusterNode, contract uint32, topic string) bool {
	return deliversMode(t, sub, pub, contract, topic, false)
}

// deliversSecure is delivers with clients in secure mode using a read/write
// topic key.
func deliversSecure(t *testing.T, sub, pub *clusterNode, contract uint32, topic string) bool {
	return deliversMode(t, sub, pub, contract, topic, true)
}

func deliversMode(t *testing.T, sub, pub *clusterNode, contract uint32, topic string, secure bool) bool {
	t.Helper()
	ctx := context.Background()
	cid := newClientID(contract)
	wire := topic
	if secure {
		wire = keyed(topicKey(contract, topic, security.AllowReadWrite), topic)
	}
	s, err := dial(ctx, sub.tcpAddr)
	if err != nil {
		t.Fatalf("dial %s: %v", sub.name, err)
	}
	defer s.close()
	if _, err := s.connect(cid, !secure, nextSess()); err != nil {
		t.Fatalf("connect to %s: %v", sub.name, err)
	}
	sid, _ := s.subscribe(0, wire)
	s.waitAck(sid, 3*time.Second)
	time.Sleep(150 * time.Millisecond) // let a forwarded subscription settle

	p, err := dial(ctx, pub.tcpAddr)
	if err != nil {
		t.Fatalf("dial %s: %v", pub.name, err)
	}
	defer p.close()
	if _, err := p.connect(cid, !secure, nextSess()); err != nil {
		t.Fatalf("connect to %s: %v", pub.name, err)
	}
	p.publish(0, wire, encodePayload(0, "cluster"), "1m")

	deadline := time.Now().Add(2 * time.Second)
	for {
		msg, ok := s.waitPub(time.Until(deadline))
		if !ok {
			return false
		}
		for _, m := range msg.Messages {
			if m.Topic != topic && !strings.HasSuffix(m.Topic, "/"+topic) {
				continue
			}
			if _, body, ok := decodePayload(m.Payload); ok && string(body) == "cluster" {
				return true
			}
		}
	}
}

func (c *cluster) assertAlive(t *testing.T, nodes []*clusterNode, after string) {
	t.Helper()
	for _, n := range nodes {
		if !n.alive() {
			t.Fatalf("node %s died %s\nlogs:\n%s", n.name, after, n.logs.String())
		}
	}
}

var names = []string{"one", "two", "three"}

func TestClusterElectsOneLeader(t *testing.T) {
	c := startCluster(t, names...)
	leader, err := c.waitLeader(c.nodes, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("leader: %s", leader)
	// Leadership must be stable once elected.
	time.Sleep(2 * time.Second)
	if again, err := c.waitLeader(c.nodes, time.Second); err != nil || again != leader {
		t.Fatalf("leadership changed without a failure: %q -> %q (%v)", leader, again, err)
	}
	c.assertAlive(t, c.nodes, "while forming the cluster")
}

// TestClusterDelivery checks every combination of subscriber node, publisher
// node and owning node, for secure clients (topic keys) and insecure ones.
func TestClusterDelivery(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"secure", "insecure"} {
		for _, own := range names {
			contract := contractOwnedBy(own, names...)
			for _, sub := range c.nodes {
				for _, pub := range c.nodes {
					name := fmt.Sprintf("%s/owner=%s/sub=%s/pub=%s", mode, own, sub.name, pub.name)
					t.Run(name, func(t *testing.T) {
						topic := fmt.Sprintf("groups.cluster.%s.%s.%s.%s", mode, own, sub.name, pub.name)
						if !deliversMode(t, sub, pub, contract, topic, mode == "secure") {
							t.Errorf("message not delivered")
						}
					})
				}
			}
		}
	}
	c.assertAlive(t, c.nodes, "during cross-node delivery")
}

// TestClusterNodeFailure kills one node and checks the survivors stay up,
// agree on a leader, and keep serving the dead node's contracts.
func TestClusterNodeFailure(t *testing.T) {
	c := startCluster(t, names...)
	leader, err := c.waitLeader(c.nodes, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	// Kill a follower, then separately the leader.
	for _, victim := range []string{followerOf(leader), leader} {
		t.Run("kill "+roleOf(victim, leader), func(t *testing.T) {
			c = startCluster(t, names...)
			if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
				t.Fatal(err)
			}
			dead := c.node(victim)
			var live []*clusterNode
			var liveNames []string
			for _, n := range c.nodes {
				if n != dead {
					live = append(live, n)
					liveNames = append(liveNames, n.name)
				}
			}
			contract := contractOwnedBy(victim, names...)

			dead.stop()
			// Allow for failure detection (node_fail_after * heartbeat) and rehash.
			time.Sleep(4 * time.Second)
			c.assertAlive(t, live, "after node "+victim+" was killed")
			if _, err := c.waitLeader(live, 10*time.Second); err != nil {
				t.Fatalf("survivors: %v", err)
			}
			if got := owner(contract, liveNames...); got == victim {
				t.Fatalf("test bug: contract still maps to dead node")
			}
			for _, sub := range live {
				for _, pub := range live {
					if !delivers(t, sub, pub, contract, fmt.Sprintf("groups.failover.%s.%s", sub.name, pub.name)) {
						t.Errorf("after %s died: sub=%s pub=%s not delivered for its former contract", victim, sub.name, pub.name)
					}
				}
			}
			c.assertAlive(t, live, "while serving after the failure")
		})
	}
}

// TestClusterNodeRejoin restarts a killed follower, and separately the leader,
// and checks it rejoins: all nodes alive, one leader, and delivery through the
// rejoined node in both directions.
func TestClusterNodeRejoin(t *testing.T) {
	for _, role := range []string{"follower", "leader"} {
		t.Run("restart "+role, func(t *testing.T) {
			c := startCluster(t, names...)
			leader, err := c.waitLeader(c.nodes, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			victim := c.node(leader)
			if role == "follower" {
				victim = c.node(followerOf(leader))
			}
			victim.stop()
			time.Sleep(3 * time.Second)
			if err := victim.start(); err != nil {
				t.Fatalf("restart %s: %v", victim.name, err)
			}
			time.Sleep(3 * time.Second)
			c.assertAlive(t, c.nodes, "after "+victim.name+" rejoined")
			if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
				t.Fatalf("after rejoin: %v", err)
			}
			contract := contractOwnedBy(victim.name, names...)
			if !delivers(t, victim, victim, contract, "groups.rejoin.same.node") {
				t.Errorf("rejoined node does not deliver its own contract")
			}
			for _, other := range c.nodes {
				if other == victim {
					continue
				}
				if !delivers(t, other, victim, contract, "groups.rejoin.to."+other.name) {
					t.Errorf("sub=%s pub=%s (rejoined) not delivered", other.name, victim.name)
				}
				if !delivers(t, victim, other, contract, "groups.rejoin.from."+other.name) {
					t.Errorf("sub=%s (rejoined) pub=%s not delivered", victim.name, other.name)
				}
			}
		})
	}
}

func followerOf(leader string) string {
	for _, n := range names {
		if n != leader {
			return n
		}
	}
	return ""
}

func roleOf(victim, leader string) string {
	if victim == leader {
		return "leader"
	}
	return "follower"
}
