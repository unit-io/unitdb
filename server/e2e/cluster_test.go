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
// with its own ports and data, with failover enabled.
//
// The cluster routes by topic: the ring assigns each contract and topic to one
// node, the topic's owner, which holds every subscription to the topic, stores
// its messages and delivers them. A request on a topic another node owns is
// forwarded to the owner, and a wildcard subscription is held by every node.
// So the tests pick topics owned by each node, with the same ring hash the
// server uses, and cover every subscriber/publisher/owner combination.

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/message/security"
	rh "github.com/unit-io/unitdb/server/internal/pkg/hash"
	"github.com/unit-io/unitdb/server/utp"
)

// Must match server/internal/cluster.go.
const clusterHashReplicas = 160

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
	t.Helper()
	return startClusterReplicas(t, 0, names...)
}

// startClusterReplicas starts a cluster of the named nodes with failover
// enabled, storing each message and session on replicas nodes, or the
// server's default number if 0.
func startClusterReplicas(t *testing.T, replicas int, names ...string) *cluster {
	t.Helper()
	return startClusterWith(t, clusterOpts{replicas: replicas}, names...)
}

// clusterOpts are what startClusterWith varies.
type clusterOpts struct {
	// replicas is the number of nodes storing each message and session, or
	// the server's default if 0.
	replicas int
	// env is added to the environment of the named nodes.
	env map[string][]string
	// asyncReplication sets async_replication: express publishes and
	// session changes don't wait for a replica.
	asyncReplication bool
	// nodeFailAfter is the heartbeats a node misses before it leaves the
	// ring; 16 if 0.
	nodeFailAfter int
}

// startClusterWith starts a cluster of the named nodes with failover enabled.
func startClusterWith(t *testing.T, opts clusterOpts, names ...string) *cluster {
	t.Helper()
	replicas := opts.replicas
	type nodeConf struct {
		Name string `json:"name"`
		Addr string `json:"addr"`
	}
	c := &cluster{t: t}
	nodeFailAfter := opts.nodeFailAfter
	if nodeFailAfter == 0 {
		nodeFailAfter = 16
	}
	var confNodes []nodeConf
	for _, name := range names {
		n := &clusterNode{name: name, addr: fmt.Sprintf("127.0.0.1:%d", freePort(t))}
		c.nodes = append(c.nodes, n)
		confNodes = append(confNodes, nodeConf{name, n.addr})
	}
	clusterConf := map[string]interface{}{
		"self":  "", // set per process with -cluster_self
		"nodes": confNodes,
		"failover": map[string]interface{}{
			"enabled":         true,
			"heartbeat":       100,
			"vote_after":      8,
			"node_fail_after": nodeFailAfter,
		},
	}
	if replicas > 0 {
		clusterConf["replicas"] = replicas
	}
	if opts.asyncReplication {
		clusterConf["async_replication"] = true
	}
	conf, _ := json.Marshal(clusterConf)
	for _, n := range c.nodes {
		n.server = startServerWith(t, serverOpts{cluster: string(conf), args: []string{"-cluster_self", n.name}, env: opts.env[n.name]})
	}
	return c
}

// owner returns the node owning username, as the server's ring computes it
// over the given live nodes.
func owner(username string, live ...string) string {
	ring := rh.NewRing(clusterHashReplicas, nil)
	ring.Add(live...)
	return ring.Get(username)
}

// topicOwner returns the node owning topic on contract, as the server's ring
// computes it over the given live nodes. Must match topicRingKey in
// server/internal/cluster.go.
func topicOwner(contract uint32, topic string, live ...string) string {
	return owner(fmt.Sprintf("%d/%s", contract, topic), live...)
}

// topicOwnedBy returns a topic starting with prefix that the ring assigns to
// name.
func topicOwnedBy(name string, contract uint32, prefix string, live ...string) string {
	for i := 0; ; i++ {
		t := fmt.Sprintf("%s.t%d", prefix, i)
		if topicOwner(contract, t, live...) == name {
			return t
		}
	}
}

var (
	electedSelf = regexp.MustCompile(`Elected myself as a new leader`)
	leaderIs    = regexp.MustCompile(`leader (?:set to )?'([a-z]+)'(?: elected)?`)
)

// leaderSeen returns the leader a node last logged, or "" if none.
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

// route is a subscriber on sub and a publisher on pub, both connected as
// username on contract unless pubUsername names another user for the
// publisher. The subscriber subscribes with delivery mode and the publisher
// publishes with pubMode.
type route struct {
	sub, pub    *clusterNode
	username    string
	pubUsername string
	contract    uint32
	topic       string
	secure      bool
	mode        uint8
	pubMode     uint8
}

// delivers subscribes on sub and publishes on pub, both as username, and
// reports whether the subscriber received the published payload. Clients
// connect insecure (no topic keys).
func delivers(t *testing.T, sub, pub *clusterNode, username string, contract uint32, topic string) bool {
	return deliversRoute(t, route{sub: sub, pub: pub, username: username, contract: contract, topic: topic})
}

func deliversRoute(t *testing.T, r route) bool {
	t.Helper()
	ctx := context.Background()
	cid := newClientID(r.contract)
	wire := r.topic
	if r.secure {
		wire = keyed(topicKey(r.contract, r.topic, security.AllowReadWrite), r.topic)
	}
	s, err := dial(ctx, r.sub.tcpAddr)
	if err != nil {
		t.Fatalf("dial %s: %v", r.sub.name, err)
	}
	defer s.close()
	if _, err := s.connectWith(connectOpts{clientID: cid, insecure: !r.secure, sessKey: nextSess(), username: r.username}); err != nil {
		t.Fatalf("connect to %s: %v", r.sub.name, err)
	}
	sid, _ := s.subscribe(r.mode, wire)
	s.waitAck(sid, 3*time.Second)
	time.Sleep(150 * time.Millisecond) // let a forwarded subscription settle

	pubUsername := r.pubUsername
	if pubUsername == "" {
		pubUsername = r.username
	}

	p, err := dial(ctx, r.pub.tcpAddr)
	if err != nil {
		t.Fatalf("dial %s: %v", r.pub.name, err)
	}
	defer p.close()
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: !r.secure, sessKey: nextSess(), username: pubUsername}); err != nil {
		t.Fatalf("connect to %s: %v", r.pub.name, err)
	}
	p.publish(r.pubMode, wire, encodePayload(0, "cluster"), "1m")

	deadline := time.Now().Add(2 * time.Second)
	for {
		msg, ok := s.waitPub(time.Until(deadline))
		if !ok {
			return false
		}
		for _, m := range msg.Messages {
			if m.Topic != r.topic && !strings.HasSuffix(m.Topic, "/"+r.topic) {
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
// node and the node owning the topic, for secure clients (topic keys) and
// insecure ones. The subscriber and the publisher are different users.
func TestClusterDelivery(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"secure", "insecure"} {
		for i, own := range names {
			contract := uint32(0x0c1a0000 + i)
			for _, sub := range c.nodes {
				for _, pub := range c.nodes {
					name := fmt.Sprintf("%s/owner=%s/sub=%s/pub=%s", mode, own, sub.name, pub.name)
					t.Run(name, func(t *testing.T) {
						topic := topicOwnedBy(own, contract, fmt.Sprintf("groups.cluster.%s.%s.%s", mode, sub.name, pub.name), names...)
						r := route{sub: sub, pub: pub, username: "subscriber@e2e.test", pubUsername: "publisher@e2e.test", contract: contract, topic: topic, secure: mode == "secure"}
						if !deliversRoute(t, r) {
							t.Errorf("message not delivered")
						}
					})
				}
			}
		}
	}
	c.assertAlive(t, c.nodes, "during cross-node delivery")
}

// TestClusterRoutesByTopic checks that a subscription lives on the node owning
// its topic, wherever the subscriber connects: a subscriber connected to a
// node that does not own the topic gets messages published directly on the
// owner, which never pass through the subscriber's node otherwise.
func TestClusterRoutesByTopic(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	contract := uint32(0x0c1b0000)
	for _, own := range names {
		ownerNode := c.node(own)
		for _, sub := range c.nodes {
			if sub == ownerNode {
				continue
			}
			t.Run(fmt.Sprintf("owner=%s/sub=%s", own, sub.name), func(t *testing.T) {
				ctx := context.Background()
				cid := newClientID(contract)
				topic := topicOwnedBy(own, contract, "groups.route."+sub.name, names...)
				s, err := dial(ctx, sub.tcpAddr)
				if err != nil {
					t.Fatal(err)
				}
				defer s.close()
				if _, err := s.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}); err != nil {
					t.Fatal(err)
				}
				sid, _ := s.subscribe(0, topic)
				s.waitAck(sid, 3*time.Second)
				time.Sleep(150 * time.Millisecond)

				p, err := dial(ctx, ownerNode.tcpAddr)
				if err != nil {
					t.Fatal(err)
				}
				defer p.close()
				if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
					t.Fatal(err)
				}
				p.publish(0, topic, encodePayload(0, "routed"), "1m")
				msg, ok := s.waitPub(3 * time.Second)
				if !ok {
					t.Fatalf("subscription to %s (owned by %s) made on %s was not held by the owner", topic, own, sub.name)
				}
				if _, body, ok := decodePayload(msg.Messages[0].Payload); !ok || string(body) != "routed" {
					t.Fatalf("unexpected delivery %q", msg.Messages[0].Payload)
				}
			})
		}
	}
	c.assertAlive(t, c.nodes, "while routing by topic")
}

// TestClusterNodeFailure kills one node and checks the survivors stay up,
// agree on a leader, and keep serving the dead node's topics.
func TestClusterNodeFailure(t *testing.T) {
	for _, role := range []string{"follower", "leader"} {
		t.Run("kill "+role, func(t *testing.T) {
			c := startCluster(t, names...)
			leader, err := c.waitLeader(c.nodes, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			victim := leader
			if role == "follower" {
				victim = followerOf(leader)
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
			contract := uint32(0x0c1c0000)

			dead.stop()
			// Allow for failure detection (node_fail_after * heartbeat) and rehash.
			time.Sleep(4 * time.Second)
			c.assertAlive(t, live, "after node "+victim+" was killed")
			if _, err := c.waitLeader(live, 10*time.Second); err != nil {
				t.Fatalf("survivors: %v", err)
			}
			for _, sub := range live {
				for _, pub := range live {
					topic := topicOwnedBy(victim, contract, fmt.Sprintf("groups.failover.%s.%s", sub.name, pub.name), names...)
					if got := topicOwner(contract, topic, liveNames...); got == victim {
						t.Fatalf("test bug: topic still maps to dead node")
					}
					if !delivers(t, sub, pub, "user@e2e.test", contract, topic) {
						t.Errorf("after %s died: sub=%s pub=%s not delivered on its former topic %s", victim, sub.name, pub.name, topic)
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
			contract := uint32(0x0c1d0000)
			owned := func(prefix string) string { return topicOwnedBy(victim.name, contract, prefix, names...) }
			if !delivers(t, victim, victim, "user@e2e.test", contract, owned("groups.rejoin.same.node")) {
				t.Errorf("rejoined node does not deliver on its own topic")
			}
			for _, other := range c.nodes {
				if other == victim {
					continue
				}
				if !delivers(t, other, victim, "user@e2e.test", contract, owned("groups.rejoin.to."+other.name)) {
					t.Errorf("sub=%s pub=%s (rejoined) not delivered", other.name, victim.name)
				}
				if !delivers(t, victim, other, "user@e2e.test", contract, owned("groups.rejoin.from."+other.name)) {
					t.Errorf("sub=%s (rejoined) pub=%s not delivered", victim.name, other.name)
				}
			}
		})
	}
}

// TestClusterReliableDelivery checks reliable and batch delivery for every
// combination of subscriber node, publisher node and topic owner. The owner
// hands a message for a subscriber connected elsewhere to the subscriber's
// node, which logs it, so the client's RECEIVE finds it there.
func TestClusterReliableDelivery(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	modes := []struct {
		name     string
		sub, pub uint8
	}{
		{"reliable", 1, 1},
		{"batch", 2, 0},
	}
	for i, own := range names {
		contract := uint32(0x0c1f0000 + i)
		for _, mode := range modes {
			for _, sub := range c.nodes {
				for _, pub := range c.nodes {
					t.Run(fmt.Sprintf("%s/owner=%s/sub=%s/pub=%s", mode.name, own, sub.name, pub.name), func(t *testing.T) {
						r := route{
							sub:      sub,
							pub:      pub,
							username: "user@e2e.test",
							contract: contract,
							topic:    topicOwnedBy(own, contract, fmt.Sprintf("groups.reliable.%s.%s.%s", mode.name, sub.name, pub.name), names...),
							mode:     mode.sub,
							pubMode:  mode.pub,
						}
						if !deliversRoute(t, r) {
							t.Errorf("%s publish on %s not delivered to the subscriber on %s (topic owner %s)", mode.name, pub.name, sub.name, own)
						}
					})
				}
			}
		}
	}
	c.assertAlive(t, c.nodes, "during reliable and batch delivery")
}

// TestClusterWildcardDelivery subscribes to a wildcard on each node, then
// publishes from every node on matching topics owned by every node. A wildcard
// subscription is held by every node, and a message is delivered only by its
// topic's owner, so each message must arrive exactly once.
func TestClusterWildcardDelivery(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	contract := uint32(0x0c200000)
	cid := newClientID(contract)
	for _, sub := range c.nodes {
		t.Run("sub="+sub.name, func(t *testing.T) {
			prefix := "groups.wild." + sub.name
			s, err := dial(ctx, sub.tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			if _, err := s.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}); err != nil {
				t.Fatal(err)
			}
			sid, _ := s.subscribe(0, prefix+".*")
			if !s.waitAck(sid, 3*time.Second) {
				t.Fatal("no subscribe ack")
			}
			time.Sleep(150 * time.Millisecond) // let the forwarded subscriptions settle

			want := map[int]string{}
			for _, pub := range c.nodes {
				p, err := dial(ctx, pub.tcpAddr)
				if err != nil {
					t.Fatal(err)
				}
				defer p.close()
				if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
					t.Fatal(err)
				}
				for _, own := range names {
					seq := len(want)
					want[seq] = fmt.Sprintf("pub=%s/owner=%s", pub.name, own)
					p.publish(0, topicOwnedBy(own, contract, prefix, names...), encodePayload(seq, want[seq]), "1m")
				}
			}

			got, dups, err := collectUnique(s, len(want), 10*time.Second, 3*time.Second)
			if err != nil {
				var missing []string
				for seq, body := range want {
					if _, ok := got[seq]; !ok {
						missing = append(missing, body)
					}
				}
				t.Fatalf("wildcard delivery: %v; missing %v", err, missing)
			}
			// A second copy would arrive shortly after the first.
			if _, ok := s.waitPub(500 * time.Millisecond); ok {
				dups++
			}
			if dups > 0 {
				t.Errorf("wildcard delivery: %d duplicate(s)", dups)
			}
		})
	}
	c.assertAlive(t, c.nodes, "during wildcard delivery")
}

// TestClusterRelay publishes on topics owned by every node, from every node,
// and relays each from every node. Messages are stored by the topic's owner,
// which a relay request is forwarded to.
func TestClusterRelay(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	contract := uint32(0x0c210000)
	cid := newClientID(contract)
	for _, own := range names {
		for _, pub := range c.nodes {
			for _, rel := range c.nodes {
				t.Run(fmt.Sprintf("owner=%s/pub=%s/relay=%s", own, pub.name, rel.name), func(t *testing.T) {
					topic := topicOwnedBy(own, contract, fmt.Sprintf("groups.relay.%s.%s", pub.name, rel.name), names...)
					p, err := dial(ctx, pub.tcpAddr)
					if err != nil {
						t.Fatal(err)
					}
					defer p.close()
					if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
						t.Fatal(err)
					}
					// The publish is acknowledged once the owner has stored it.
					if id, _ := p.publish(0, topic, encodePayload(0, "stored"), "1m"); !p.waitAck(id, 3*time.Second) {
						t.Fatal("no publish ack")
					}

					r, err := dial(ctx, rel.tcpAddr)
					if err != nil {
						t.Fatal(err)
					}
					defer r.close()
					if _, err := r.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "reader@e2e.test"}); err != nil {
						t.Fatal(err)
					}
					r.relay(topic, "1m")
					msg, ok := r.waitPub(3 * time.Second)
					if !ok {
						t.Fatalf("relay on %s returned nothing for %s (owned by %s, published on %s)", rel.name, topic, own, pub.name)
					}
					if _, body, ok := decodePayload(msg.Messages[0].Payload); !ok || string(body) != "stored" {
						t.Fatalf("unexpected relay %q", msg.Messages[0].Payload)
					}
					// The message is stored on its owner and a replica, and
					// must come back once.
					count := len(msg.Messages)
					for {
						more, ok := r.waitPub(500 * time.Millisecond)
						if !ok {
							break
						}
						count += len(more.Messages)
					}
					if count != 1 {
						t.Errorf("relay on %s returned the message %d times", rel.name, count)
					}
				})
			}
		}
	}
	c.assertAlive(t, c.nodes, "during relay")
}

// TestClusterFailoverKeepsSubscriptions keeps a subscriber connected while a
// node dies and restarts, and checks its subscriptions follow the topics'
// owners without the client resubscribing: to the node taking over each of
// the dead node's topics (the subscriber's own node, or another), and back to
// the node once it rejoins with nothing stored. A wildcard subscription must
// reach the restarted node again too.
func TestClusterFailoverKeepsSubscriptions(t *testing.T) {
	for _, role := range []string{"follower", "leader"} {
		t.Run("kill "+role, func(t *testing.T) {
			c := startCluster(t, names...)
			leader, err := c.waitLeader(c.nodes, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			victim := leader
			if role == "follower" {
				victim = followerOf(leader)
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
			contract := uint32(0x0c220000)
			cid := newClientID(contract)
			sub, pub := live[0], live[1]

			// A topic of the dead node for each survivor to take over.
			var topics []string
			for _, heir := range liveNames {
				for i := 0; ; i++ {
					topic := fmt.Sprintf("groups.keep.%s.t%d", heir, i)
					if topicOwner(contract, topic, names...) == victim && topicOwner(contract, topic, liveNames...) == heir {
						topics = append(topics, topic)
						break
					}
				}
			}
			wildTopic := topicOwnedBy(victim, contract, "groups.keepwild", names...)

			s, err := dial(context.Background(), sub.tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			if _, err := s.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}); err != nil {
				t.Fatal(err)
			}
			for _, topic := range append(topics, "groups.keepwild.*") {
				if sid, _ := s.subscribe(0, topic); !s.waitAck(sid, 3*time.Second) {
					t.Fatalf("no subscribe ack for %s", topic)
				}
			}
			time.Sleep(150 * time.Millisecond) // let the forwarded subscriptions settle

			check := func(when string) {
				t.Helper()
				for _, topic := range append(topics, wildTopic) {
					if !publishReaches(t, s, pub, cid, topic) {
						t.Errorf("%s: publish on %s (on %s) did not reach the subscriber on %s", when, topic, pub.name, sub.name)
					}
				}
			}
			check("before the failure")

			dead.stop()
			// Allow for failure detection (node_fail_after * heartbeat) and rehash.
			time.Sleep(4 * time.Second)
			if _, err := c.waitLeader(live, 10*time.Second); err != nil {
				t.Fatalf("survivors: %v", err)
			}
			check("after " + victim + " died")
			// Stored by the topics' owners during the outage, which the
			// restarted node takes the topics back from.
			stored := storeOn(t, pub, cid, topics, "outage")

			if err := dead.start(); err != nil {
				t.Fatalf("restart %s: %v", victim, err)
			}
			time.Sleep(3 * time.Second)
			if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
				t.Fatalf("after rejoin: %v", err)
			}
			check("after " + victim + " rejoined")
			for _, topic := range stored {
				if !relayFinds(t, sub, cid, topic, "outage") {
					t.Errorf("after %s rejoined: relay on %s did not return the message stored on %s during the outage", victim, sub.name, topic)
				}
			}
			c.assertAlive(t, c.nodes, "while subscriptions followed their topics")
		})
	}
}

// storeOn publishes body on each topic from node pub, waits for each to be
// acknowledged, stored by its owner, and returns the topics.
func storeOn(t *testing.T, pub *clusterNode, cid string, topics []string, body string) []string {
	t.Helper()
	p, err := dial(context.Background(), pub.tcpAddr)
	if err != nil {
		t.Fatalf("dial %s: %v", pub.name, err)
	}
	defer p.close()
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatalf("connect to %s: %v", pub.name, err)
	}
	for _, topic := range topics {
		if id, _ := p.publish(0, topic, encodePayload(0, body), "1h"); !p.waitAck(id, 3*time.Second) {
			t.Fatalf("no publish ack for %s", topic)
		}
	}
	return topics
}

// relayFinds relays topic from node n and reports whether a message with body
// comes back.
func relayFinds(t *testing.T, n *clusterNode, cid, topic, body string) bool {
	t.Helper()
	r, err := dial(context.Background(), n.tcpAddr)
	if err != nil {
		t.Fatalf("dial %s: %v", n.name, err)
	}
	defer r.close()
	if _, err := r.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "reader@e2e.test"}); err != nil {
		t.Fatalf("connect to %s: %v", n.name, err)
	}
	r.relay(topic, "1h")
	deadline := time.Now().Add(3 * time.Second)
	for {
		msg, ok := r.waitPub(time.Until(deadline))
		if !ok {
			return false
		}
		for _, m := range msg.Messages {
			if _, b, ok := decodePayload(m.Payload); ok && string(b) == body {
				return true
			}
		}
	}
}

// publishReaches publishes on topic from node pub and reports whether the
// already subscribed client s receives it.
func publishReaches(t *testing.T, s *client, pub *clusterNode, cid, topic string) bool {
	t.Helper()
	p, err := dial(context.Background(), pub.tcpAddr)
	if err != nil {
		t.Fatalf("dial %s: %v", pub.name, err)
	}
	defer p.close()
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatalf("connect to %s: %v", pub.name, err)
	}
	p.publish(0, topic, encodePayload(0, topic), "1m")
	deadline := time.Now().Add(3 * time.Second)
	for {
		msg, ok := s.waitPub(time.Until(deadline))
		if !ok {
			return false
		}
		for _, m := range msg.Messages {
			if _, body, ok := decodePayload(m.Payload); ok && string(body) == topic {
				return true
			}
		}
	}
}

// TestClusterReplicatedRelay stores messages on a topic, kills the topic's
// owner, and checks that a relay from each surviving node still returns every
// message: the next node on the ring stored them too, as a replica. Messages
// stored during the outage are handed to the owner when it restarts, and a
// relay from any node then returns every message exactly once.
func TestClusterReplicatedRelay(t *testing.T) {
	for _, role := range []string{"follower", "leader"} {
		t.Run("kill "+role, func(t *testing.T) {
			c := startCluster(t, names...)
			leader, err := c.waitLeader(c.nodes, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			victim := leader
			if role == "follower" {
				victim = followerOf(leader)
			}
			dead := c.node(victim)
			var live []*clusterNode
			for _, n := range c.nodes {
				if n != dead {
					live = append(live, n)
				}
			}
			ctx := context.Background()
			contract := uint32(0x0c230000)
			cid := newClientID(contract)
			topic := topicOwnedBy(victim, contract, "groups.replicated", names...)

			// Published on a survivor, so each message is forwarded to the
			// owner, which acknowledges it once stored and queued for its replica.
			publish := func(from, to int) {
				t.Helper()
				p, err := dial(ctx, live[0].tcpAddr)
				if err != nil {
					t.Fatal(err)
				}
				defer p.close()
				if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
					t.Fatal(err)
				}
				for i := from; i < to; i++ {
					if id, _ := p.publish(0, topic, encodePayload(i, fmt.Sprintf("m%d", i)), "1h"); !p.waitAck(id, 3*time.Second) {
						t.Fatalf("no publish ack for message %d", i)
					}
				}
				time.Sleep(500 * time.Millisecond) // replication is asynchronous
			}
			// relayAll relays the topic from each node and checks it returns
			// messages 0 to n-1, each once.
			relayAll := func(nodes []*clusterNode, n int, when string) {
				t.Helper()
				for _, rel := range nodes {
					r, err := dial(ctx, rel.tcpAddr)
					if err != nil {
						t.Fatal(err)
					}
					if _, err := r.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "reader@e2e.test"}); err != nil {
						t.Fatal(err)
					}
					r.relay(topic, "1h")
					got, dups, err := collectUnique(r, n, 10*time.Second, 3*time.Second)
					for {
						more, ok := r.waitPub(500 * time.Millisecond)
						if !ok {
							break
						}
						dups += len(more.Messages)
					}
					r.close()
					if err != nil {
						t.Errorf("relay on %s %s: %v", rel.name, when, err)
						continue
					}
					if dups > 0 {
						t.Errorf("relay on %s %s: %d duplicate(s)", rel.name, when, dups)
					}
					for i := 0; i < n; i++ {
						if want := fmt.Sprintf("m%d", i); got[i] != want {
							t.Errorf("relay on %s %s: message %d is %q, want %q", rel.name, when, i, got[i], want)
						}
					}
				}
			}
			publish(0, 20)

			dead.stop()
			// Allow for failure detection (node_fail_after * heartbeat) and rehash.
			time.Sleep(4 * time.Second)
			if _, err := c.waitLeader(live, 10*time.Second); err != nil {
				t.Fatalf("survivors: %v", err)
			}

			relayAll(live, 20, "after "+victim+" (the owner) died")

			// Stored during the outage by the topic's owner of the moment.
			publish(20, 30)
			if err := dead.start(); err != nil {
				t.Fatalf("restart %s: %v", victim, err)
			}
			time.Sleep(3 * time.Second)
			if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
				t.Fatalf("after rejoin: %v", err)
			}
			relayAll(c.nodes, 30, "after "+victim+" (the owner) rejoined")
			c.assertAlive(t, c.nodes, "while relaying from replicas")
		})
	}
}

// TestClusterSessionFailover leaves reliable deliveries pending for a
// subscriber on one node, kills the node, and resumes the session on another
// node: the session's replicas hand over its log, and the pending messages
// arrive.
func TestClusterSessionFailover(t *testing.T) {
	for _, role := range []string{"follower", "leader"} {
		t.Run("kill "+role, func(t *testing.T) {
			c := startCluster(t, names...)
			leader, err := c.waitLeader(c.nodes, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			victim := leader
			if role == "follower" {
				victim = followerOf(leader)
			}
			dead := c.node(victim)
			var live []*clusterNode
			for _, n := range c.nodes {
				if n != dead {
					live = append(live, n)
				}
			}
			ctx := context.Background()
			contract := uint32(0x0c270000)
			cid := newClientID(contract)
			sessKey := nextSess()
			topic := "groups.session." + victim
			opts := connectOpts{clientID: cid, insecure: true, sessKey: sessKey, username: "subscriber@e2e.test"}

			s, err := dial(ctx, dead.tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			s.holdNotify.Store(true)
			if _, err := s.connectWith(opts); err != nil {
				t.Fatal(err)
			}
			if sid, _ := s.subscribe(1, topic); !s.waitAck(sid, 3*time.Second) {
				t.Fatal("no subscribe ack")
			}
			time.Sleep(150 * time.Millisecond) // let a forwarded subscription settle

			p, err := dial(ctx, live[0].tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer p.close()
			if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
				t.Fatal(err)
			}
			n := 5
			for i := 0; i < n; i++ {
				if id, _ := p.publish(1, topic, encodePayload(i, fmt.Sprintf("r%d", i)), "1h"); !p.waitAck(id, 3*time.Second) {
					t.Fatalf("no publish ack for message %d", i)
				}
			}
			// Each NOTIFY left unanswered is a message logged for the session.
			deadline := time.After(5 * time.Second)
			for notified := 0; notified < n; {
				select {
				case m := <-s.ctrl:
					if m.FlowControl == utp.NOTIFY {
						notified++
					}
				case <-deadline:
					t.Fatalf("subscriber on %s was notified of %d of %d messages", victim, notified, n)
				}
			}
			time.Sleep(500 * time.Millisecond) // replication is asynchronous

			dead.stop()
			s.close()
			// Allow for failure detection (node_fail_after * heartbeat) and rehash.
			time.Sleep(4 * time.Second)
			if _, err := c.waitLeader(live, 10*time.Second); err != nil {
				t.Fatalf("survivors: %v", err)
			}

			resumed := opts
			resumed.resume = true
			r, err := dial(ctx, live[0].tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer r.close()
			if _, err := r.connectWith(resumed); err != nil {
				t.Fatal(err)
			}
			got, _, err := collectUnique(r, n, 10*time.Second, 3*time.Second)
			if err != nil {
				t.Fatalf("session resumed on %s after %s died: %v", live[0].name, victim, err)
			}
			for i := 0; i < n; i++ {
				if want := fmt.Sprintf("r%d", i); got[i] != want {
					t.Errorf("session resumed on %s: message %d is %q, want %q", live[0].name, i, got[i], want)
				}
			}
			c.assertAlive(t, live, "while resuming a session")
		})
	}
}

// TestClusterReliablePublishSurvivesCrash publishes reliable messages to a
// topic's owner and kills it right after the last acknowledgement, with no
// time for asynchronous replication: a reliable publish is acknowledged only
// once a replica stored it too, so the survivors still relay every message.
func TestClusterReliablePublishSurvivesCrash(t *testing.T) {
	// Asynchronous replication would not be done by the time of the crash.
	t.Setenv("UNITDB_REPLICATION_DELAY", "3s")
	for _, role := range []string{"follower", "leader"} {
		t.Run("kill "+role, func(t *testing.T) {
			c := startCluster(t, names...)
			leader, err := c.waitLeader(c.nodes, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			victim := leader
			if role == "follower" {
				victim = followerOf(leader)
			}
			dead := c.node(victim)
			var live []*clusterNode
			for _, n := range c.nodes {
				if n != dead {
					live = append(live, n)
				}
			}
			ctx := context.Background()
			contract := uint32(0x0c280000)
			cid := newClientID(contract)
			topic := topicOwnedBy(victim, contract, "groups.crash", names...)

			p, err := dial(ctx, dead.tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
				t.Fatal(err)
			}
			n := 100
			for i := 0; i < n; i++ {
				if id, _ := p.publish(1, topic, encodePayload(i, fmt.Sprintf("m%d", i)), "1h"); !p.waitAck(id, 3*time.Second) {
					t.Fatalf("no publish ack for message %d", i)
				}
			}
			dead.stop()
			p.close()
			// Allow for failure detection (node_fail_after * heartbeat) and rehash.
			time.Sleep(4 * time.Second)
			if _, err := c.waitLeader(live, 10*time.Second); err != nil {
				t.Fatalf("survivors: %v", err)
			}

			for _, rel := range live {
				r, err := dial(ctx, rel.tcpAddr)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := r.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "reader@e2e.test"}); err != nil {
					t.Fatal(err)
				}
				r.relay(topic, "1h")
				got, _, err := collectUnique(r, n, 10*time.Second, 3*time.Second)
				r.close()
				if err != nil {
					t.Errorf("relay on %s after %s (the owner) crashed: %v", rel.name, victim, err)
				}
				for i := 0; i < n; i++ {
					if _, ok := got[i]; !ok {
						t.Errorf("relay on %s: acknowledged message %d lost in the crash", rel.name, i)
						break
					}
				}
			}
			c.assertAlive(t, live, "after the owner crashed")
		})
	}
}

// TestClusterReliablePublishHungReplica freezes a topic's replica, without
// killing it, and checks that a reliable publish to the owner is still
// acknowledged: the owner waits for a replica only up to a timeout. The
// replica stores the message when it resumes, and gets it again as a hint:
// it keeps one copy.
func TestClusterReliablePublishHungReplica(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	contract := uint32(0x0c290000)
	cid := newClientID(contract)
	own := names[0]
	topic := topicOwnedBy(own, contract, "groups.hung", names...)
	ring := rh.NewRing(clusterHashReplicas, nil)
	ring.Add(names...)
	replica := c.node(ring.GetN(fmt.Sprintf("%d/%s", contract, topic), 2)[1])

	p, err := dial(context.Background(), c.node(own).tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(replica.cmd.Process.Pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	defer syscall.Kill(replica.cmd.Process.Pid, syscall.SIGCONT)

	start := time.Now()
	if id, _ := p.publish(1, topic, encodePayload(0, "hung"), "1h"); !p.waitAck(id, 5*time.Second) {
		t.Fatalf("reliable publish not acknowledged while its replica %s is frozen", replica.name)
	}
	t.Logf("acknowledged after %v with replica %s frozen", time.Since(start).Round(time.Millisecond), replica.name)

	if err := syscall.Kill(replica.cmd.Process.Pid, syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	time.Sleep(7 * time.Second) // the next handoff of hints
	c.node(own).stop()
	r, err := dial(context.Background(), replica.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	if _, err := r.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "reader@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	r.relay(topic, "1h")
	count := 0
	for {
		msg, ok := r.waitPub(2 * time.Second)
		if !ok {
			break
		}
		count += len(msg.Messages)
	}
	if count != 1 {
		t.Errorf("relay on %s, with the owner stopped, returned the message %d times", replica.name, count)
	}
}

// TestClusterRebuildEmptyNode wipes a node's disk and restarts it: the node
// copies back the messages of the topics it owns and holds as a replica, from
// the other nodes, each once and with its original expiry. It then answers
// relays on its own, with the other replicas stopped.
func TestClusterRebuildEmptyNode(t *testing.T) {
	for _, role := range []string{"follower", "leader"} {
		t.Run("wipe "+role, func(t *testing.T) {
			c := startCluster(t, names...)
			leader, err := c.waitLeader(c.nodes, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			victim := leader
			if role == "follower" {
				victim = followerOf(leader)
			}
			dead := c.node(victim)
			var live []*clusterNode
			for _, n := range c.nodes {
				if n != dead {
					live = append(live, n)
				}
			}
			ctx := context.Background()
			contract := uint32(0x0c2a0000)
			cid := newClientID(contract)
			ring := rh.NewRing(clusterHashReplicas, nil)
			ring.Add(names...)
			holders := func(topic string) []string { return ring.GetN(fmt.Sprintf("%d/%s", contract, topic), 2) }

			owned := topicOwnedBy(victim, contract, "groups.rebuild.owned", names...)
			short := topicOwnedBy(victim, contract, "groups.rebuild.short", names...)
			var replicated string
			for i := 0; ; i++ {
				topic := fmt.Sprintf("groups.rebuild.replica.t%d", i)
				if h := holders(topic); h[0] != victim && h[1] == victim {
					replicated = topic
					break
				}
			}

			p, err := dial(ctx, live[0].tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
				t.Fatal(err)
			}
			published := time.Now()
			for _, m := range []struct {
				topic, ttl string
				n          int
			}{{owned, "1h", 20}, {replicated, "1h", 20}, {short, "10s", 5}} {
				for i := 0; i < m.n; i++ {
					if id, _ := p.publish(0, m.topic, encodePayload(i, fmt.Sprintf("%s/%d", m.topic, i)), m.ttl); !p.waitAck(id, 3*time.Second) {
						t.Fatalf("no publish ack on %s", m.topic)
					}
				}
			}
			p.close()
			time.Sleep(500 * time.Millisecond) // replication is asynchronous

			dead.stop()
			if err := os.RemoveAll(filepath.Join(dead.dbPath, "db")); err != nil {
				t.Fatal(err)
			}
			if err := dead.start(); err != nil {
				t.Fatalf("restart %s: %v", victim, err)
			}
			time.Sleep(4 * time.Second) // rejoin and rebuild
			if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
				t.Fatalf("after rejoin: %v", err)
			}

			// Stop the topics' other replicas: the rebuilt node answers alone.
			for _, topic := range []string{owned, replicated, short} {
				for _, h := range holders(topic) {
					if h != victim {
						c.node(h).stop()
					}
				}
			}
			relayOn := func(topic string, n int) (map[int]string, int, error) {
				t.Helper()
				r, err := dial(ctx, dead.tcpAddr)
				if err != nil {
					t.Fatal(err)
				}
				defer r.close()
				if _, err := r.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "reader@e2e.test"}); err != nil {
					t.Fatal(err)
				}
				r.relay(topic, "1h")
				got, dups, err := collectUnique(r, n, 10*time.Second, 3*time.Second)
				for {
					more, ok := r.waitPub(500 * time.Millisecond)
					if !ok {
						break
					}
					dups += len(more.Messages)
				}
				return got, dups, err
			}
			for _, m := range []struct {
				topic string
				n     int
			}{{owned, 20}, {replicated, 20}, {short, 5}} {
				got, dups, err := relayOn(m.topic, m.n)
				if err != nil {
					t.Errorf("relay on rebuilt %s of %s: %v", victim, m.topic, err)
				}
				if dups > 0 {
					t.Errorf("relay on rebuilt %s of %s: %d duplicate(s)", victim, m.topic, dups)
				}
				for i := 0; i < m.n; i++ {
					if want := fmt.Sprintf("%s/%d", m.topic, i); got[i] != want {
						t.Errorf("relay on rebuilt %s of %s: message %d is %q, want %q", victim, m.topic, i, got[i], want)
						break
					}
				}
			}

			// The short messages keep their expiry, not the default one.
			time.Sleep(time.Until(published.Add(12 * time.Second)))
			if got, _, _ := relayOn(short, 1); len(got) > 0 {
				t.Errorf("relay on rebuilt %s of %s returned %d message(s) after their 10s TTL", victim, short, len(got))
			}
		})
	}
}

// TestClusterSessionHandoff stops a replica of a session while reliable
// deliveries for it are logged, restarts it, and then leaves it the only node
// up: the session log changes it missed were kept as hints and handed to it
// when it came back, and the session resumes on it with every pending message.
func TestClusterSessionHandoff(t *testing.T) {
	// Every node is a replica of every session.
	c := startClusterReplicas(t, 3, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	contract := uint32(0x0c2b0000)
	cid := newClientID(contract)
	home, replica, other := c.nodes[0], c.nodes[1], c.nodes[2]
	topic := topicOwnedBy(home.name, contract, "groups.sessionhint", names...)
	opts := connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}

	s, err := dial(ctx, home.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	s.holdNotify.Store(true)
	if _, err := s.connectWith(opts); err != nil {
		t.Fatal(err)
	}
	if sid, _ := s.subscribe(1, topic); !s.waitAck(sid, 3*time.Second) {
		t.Fatal("no subscribe ack")
	}
	time.Sleep(500 * time.Millisecond) // the session row reaches every replica

	replica.stop()
	p, err := dial(ctx, home.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	n := 5
	for i := 0; i < n; i++ {
		if id, _ := p.publish(1, topic, encodePayload(i, fmt.Sprintf("h%d", i)), "1h"); !p.waitAck(id, 3*time.Second) {
			t.Fatalf("no publish ack for message %d", i)
		}
	}
	deadline := time.After(5 * time.Second)
	for notified := 0; notified < n; {
		select {
		case m := <-s.ctrl:
			if m.FlowControl == utp.NOTIFY {
				notified++
			}
		case <-deadline:
			t.Fatalf("subscriber was notified of %d of %d messages", notified, n)
		}
	}
	time.Sleep(500 * time.Millisecond) // replication to the live replica

	if err := replica.start(); err != nil {
		t.Fatalf("restart %s: %v", replica.name, err)
	}
	time.Sleep(3 * time.Second) // rejoin and handoff
	home.stop()
	other.stop()
	s.close()

	r, err := dial(ctx, replica.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	resumed := opts
	resumed.resume = true
	if _, err := r.connectWith(resumed); err != nil {
		t.Fatal(err)
	}
	got, _, err := collectUnique(r, n, 10*time.Second, 3*time.Second)
	if err != nil {
		t.Fatalf("session resumed on %s, the only node up: %v", replica.name, err)
	}
	for i := 0; i < n; i++ {
		if want := fmt.Sprintf("h%d", i); got[i] != want {
			t.Errorf("session resumed on %s: message %d is %q, want %q", replica.name, i, got[i], want)
		}
	}
}

// TestClusterSessionMoveForgetsStaleCopy leaves reliable deliveries pending
// for a session on a node that is not one of its replicas, resumes the
// session on one replica, which completes them, and then on the other: the
// first node's copy, which still has them pending, was dropped when the
// session moved, and nothing is delivered again.
func TestClusterSessionMoveForgetsStaleCopy(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	contract := uint32(0x0c2c0000)
	cid := newClientID(contract)
	ring := rh.NewRing(clusterHashReplicas, nil)
	ring.Add(names...)
	home := c.nodes[0]
	topic := "groups.sessionmove"

	// A new session's id is its connection's: retry until the session's
	// replicas are the two other nodes.
	var s *client
	var opts connectOpts
	var replicas []string
	for attempt := 0; attempt < 50 && s == nil; attempt++ {
		opts = connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}
		cl, err := dial(ctx, home.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		cl.holdNotify.Store(true)
		ack, err := cl.connectWith(opts)
		if err != nil {
			t.Fatal(err)
		}
		replicas = ring.GetN(fmt.Sprintf("session/%d", uint32(ack.ConnID)), 2)
		if replicas[0] != home.name && replicas[1] != home.name {
			s = cl
		} else {
			cl.close()
		}
	}
	if s == nil {
		t.Fatal("no session found whose replicas exclude its node")
	}
	defer s.close()
	if sid, _ := s.subscribe(1, topic); !s.waitAck(sid, 3*time.Second) {
		t.Fatal("no subscribe ack")
	}
	time.Sleep(150 * time.Millisecond) // let a forwarded subscription settle

	p, err := dial(ctx, home.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	n := 3
	for i := 0; i < n; i++ {
		if id, _ := p.publish(1, topic, encodePayload(i, fmt.Sprintf("s%d", i)), "1h"); !p.waitAck(id, 3*time.Second) {
			t.Fatalf("no publish ack for message %d", i)
		}
	}
	deadline := time.After(5 * time.Second)
	for notified := 0; notified < n; {
		select {
		case m := <-s.ctrl:
			if m.FlowControl == utp.NOTIFY {
				notified++
			}
		case <-deadline:
			t.Fatalf("subscriber was notified of %d of %d messages", notified, n)
		}
	}
	time.Sleep(500 * time.Millisecond) // replication is asynchronous
	s.close()

	resumed := opts
	resumed.resume = true
	resume := func(node *clusterNode) *client {
		t.Helper()
		r, err := dial(ctx, node.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.connectWith(resumed); err != nil {
			t.Fatal(err)
		}
		return r
	}
	r1 := resume(c.node(replicas[0]))
	if _, _, err := collectUnique(r1, n, 10*time.Second, 3*time.Second); err != nil {
		t.Fatalf("session resumed on %s: %v", replicas[0], err)
	}
	time.Sleep(time.Second) // the completions reach the other replica
	r1.close()

	r2 := resume(c.node(replicas[1]))
	defer r2.close()
	if msg, ok := r2.waitPub(2 * time.Second); ok {
		t.Errorf("session resumed on %s after its messages were completed on %s: %d delivered again from %s's stale copy", replicas[1], replicas[0], len(msg.Messages), home.name)
	}
}

// TestClusterRequestsDuringFailover kills a topic's owner and, before the
// others notice, subscribes to and publishes on its topics from the other
// nodes: requests the dead owner, or a node whose ring lags, does not take are
// sent again to the owner the ring gives next, and every one succeeds.
func TestClusterRequestsDuringFailover(t *testing.T) {
	for _, role := range []string{"follower", "leader"} {
		t.Run("kill "+role, func(t *testing.T) {
			c := startCluster(t, names...)
			leader, err := c.waitLeader(c.nodes, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			victim := leader
			if role == "follower" {
				victim = followerOf(leader)
			}
			dead := c.node(victim)
			var live []*clusterNode
			for _, n := range c.nodes {
				if n != dead {
					live = append(live, n)
				}
			}
			ctx := context.Background()
			contract := uint32(0x0c2e0000)
			cid := newClientID(contract)
			pubTopic := topicOwnedBy(victim, contract, "groups.failover.pub", names...)
			subTopic := topicOwnedBy(victim, contract, "groups.failover.sub", names...)

			s, err := dial(ctx, live[1].tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			if _, err := s.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}); err != nil {
				t.Fatal(err)
			}
			p, err := dial(ctx, live[0].tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer p.close()
			if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
				t.Fatal(err)
			}

			dead.stop()
			// Subscribe at once, before failure detection.
			if sid, _ := s.subscribe(0, subTopic); !s.waitAck(sid, 5*time.Second) {
				t.Errorf("subscribe to %s, owned by the dead %s, not acknowledged", subTopic, victim)
			}
			// Publish through failure detection and the rehash.
			n := 30
			unacked := 0
			for i := 0; i < n; i++ {
				if id, _ := p.publish(0, pubTopic, encodePayload(i, fmt.Sprintf("f%d", i)), "1h"); !p.waitAck(id, 5*time.Second) {
					unacked++
				}
				time.Sleep(80 * time.Millisecond)
			}
			if unacked > 0 {
				t.Errorf("%d of %d publishes on %s, owned by the dead %s, not acknowledged", unacked, n, pubTopic, victim)
			}
			if _, err := c.waitLeader(live, 10*time.Second); err != nil {
				t.Fatalf("survivors: %v", err)
			}

			// Every acknowledged publish was stored by the topic's new owner.
			r, err := dial(ctx, live[1].tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer r.close()
			if _, err := r.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "reader@e2e.test"}); err != nil {
				t.Fatal(err)
			}
			r.relay(pubTopic, "1h")
			if got, _, err := collectUnique(r, n, 10*time.Second, 3*time.Second); err != nil {
				t.Errorf("relay of %s after the failover: %v (%d of %d)", pubTopic, err, len(got), n)
			}
			// The subscription made during the failover delivers.
			if !publishReaches(t, s, live[0], cid, subTopic) {
				t.Errorf("publish on %s did not reach the subscription made while %s was dying", subTopic, victim)
			}
			c.assertAlive(t, live, "during requests through a failover")
		})
	}
}

// TestClusterSubscribeOutlastsFailureDetection subscribes to a topic whose
// owner just died, with failure detection slower than the subscribe's
// retries: the subscription is kept, and placed with the topic's new owner
// once the ring drops the dead one. It was dropped, although acknowledged.
func TestClusterSubscribeOutlastsFailureDetection(t *testing.T) {
	// Out of the ring after 5 s, past the 3 s the subscribe is retried for.
	c := startClusterWith(t, clusterOpts{nodeFailAfter: 50}, names...)
	leader, err := c.waitLeader(c.nodes, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	victim := followerOf(leader)
	dead := c.node(victim)
	var live []*clusterNode
	for _, n := range c.nodes {
		if n != dead {
			live = append(live, n)
		}
	}
	contract := uint32(0x0c2f0000)
	cid := newClientID(contract)
	topic := topicOwnedBy(victim, contract, "groups.slowfail", names...)

	s, err := dial(context.Background(), live[1].tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if _, err := s.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	dead.stop()
	if sid, _ := s.subscribe(0, topic); !s.waitAck(sid, 10*time.Second) {
		t.Fatalf("subscribe to %s, owned by the dead %s, not acknowledged", topic, victim)
	}
	time.Sleep(4 * time.Second) // the rest of failure detection, and the rehash
	if !publishReaches(t, s, live[0], cid, topic) {
		t.Errorf("publish on %s did not reach the subscription made after %s died", topic, victim)
	}
	c.assertAlive(t, live, "while placing a subscription after a failover")
}

// TestClusterDeliveryFanOut subscribes many clients of one node to a topic
// another node owns, with each delivery call slowed down, and publishes a
// message: the owner hands the node the message for all of them in one call,
// not one call per client, so they all get it quickly.
func TestClusterDeliveryFanOut(t *testing.T) {
	t.Setenv("UNITDB_DELIVER_DELAY", "100ms")
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	contract := uint32(0x0c2f0000)
	cid := newClientID(contract)
	own, far := c.nodes[0], c.nodes[1]
	topic := topicOwnedBy(own.name, contract, "groups.fanout", names...)

	n := 30
	subs := make([]*client, n)
	for i := range subs {
		s, err := dial(ctx, far.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer s.close()
		if _, err := s.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}); err != nil {
			t.Fatal(err)
		}
		if sid, _ := s.subscribe(0, topic); !s.waitAck(sid, 3*time.Second) {
			t.Fatalf("no subscribe ack for subscriber %d", i)
		}
		subs[i] = s
	}
	time.Sleep(300 * time.Millisecond) // let the forwarded subscriptions settle

	p, err := dial(ctx, own.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	p.publish(0, topic, encodePayload(0, "fanout"), "1m")
	deadline := start.Add(1500 * time.Millisecond)
	got := 0
	for _, s := range subs {
		if _, ok := s.waitPub(time.Until(deadline)); ok {
			got++
		}
	}
	t.Logf("%d of %d subscribers got the message in %v", got, n, time.Since(start).Round(time.Millisecond))
	if got != n {
		t.Errorf("%d of %d subscribers on %s got the message within 1.5s, with 100ms per delivery call", got, n, far.name)
	}
}

// TestClusterPartitionKeepsSubscriptions freezes a node, without killing it,
// for long enough that the others take it out of the ring and drop what they
// held for its clients, then lets it go on: its connections never failed, and
// the others ask it to send its clients' subscriptions again when it is back,
// so a client of it still gets messages on a topic another node owns.
func TestClusterPartitionKeepsSubscriptions(t *testing.T) {
	for _, role := range []string{"follower", "leader"} {
		t.Run("freeze "+role, func(t *testing.T) {
			c := startCluster(t, names...)
			leader, err := c.waitLeader(c.nodes, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			frozen := c.node(leader)
			if role == "follower" {
				frozen = c.node(followerOf(leader))
			}
			var others []*clusterNode
			for _, n := range c.nodes {
				if n != frozen {
					others = append(others, n)
				}
			}
			ctx := context.Background()
			contract := uint32(0x0c300000)
			cid := newClientID(contract)
			owner := others[0]
			topic := topicOwnedBy(owner.name, contract, "groups.partition", names...)

			s, err := dial(ctx, frozen.tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer s.close()
			if _, err := s.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}); err != nil {
				t.Fatal(err)
			}
			if sid, _ := s.subscribe(0, topic); !s.waitAck(sid, 3*time.Second) {
				t.Fatal("no subscribe ack")
			}
			time.Sleep(150 * time.Millisecond) // let the forwarded subscription settle
			if !publishReaches(t, s, owner, cid, topic) {
				t.Fatalf("publish on %s did not reach the subscriber on %s before the freeze", topic, frozen.name)
			}

			pid := frozen.cmd.Process.Pid
			if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
				t.Fatal(err)
			}
			// Out of the ring after node_fail_after heartbeats.
			time.Sleep(3 * time.Second)
			if err := syscall.Kill(pid, syscall.SIGCONT); err != nil {
				t.Fatal(err)
			}
			time.Sleep(4 * time.Second) // back in the ring, and resynced
			if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
				t.Fatalf("after the freeze: %v", err)
			}
			if !publishReaches(t, s, owner, cid, topic) {
				t.Errorf("publish on %s did not reach the subscriber on %s after it was frozen and back", topic, frozen.name)
			}
			c.assertAlive(t, c.nodes, "after a node froze and came back")
		})
	}
}

// TestClusterFrozenNodeIsFailedOver freezes a follower, without killing it:
// the leader's pings to it time out, and count as failures, so the follower
// is taken out of the ring like a dead one, and a publish on one of its
// topics goes to the topic's next owner.
func TestClusterFrozenNodeIsFailedOver(t *testing.T) {
	c := startCluster(t, names...)
	leader, err := c.waitLeader(c.nodes, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	frozen := c.node(followerOf(leader))
	var others []*clusterNode
	for _, n := range c.nodes {
		if n != frozen {
			others = append(others, n)
		}
	}
	contract := uint32(0x0c310000)
	cid := newClientID(contract)
	topic := topicOwnedBy(frozen.name, contract, "groups.frozen", names...)
	p, err := dial(context.Background(), others[0].tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatal(err)
	}

	pid := frozen.cmd.Process.Pid
	if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	defer syscall.Kill(pid, syscall.SIGCONT)
	time.Sleep(3 * time.Second) // node_fail_after heartbeats, and the rehash
	if id, _ := p.publish(0, topic, encodePayload(0, "frozen"), "1h"); !p.waitAck(id, 2*time.Second) {
		t.Errorf("publish on %s, owned by the frozen %s, not acknowledged: the node was not failed over", topic, frozen.name)
	}
}

// TestClusterReplicaRestartStoresOnce has a replica store a message after the
// owner stopped waiting for it, and so kept it as a hint, then restarts the
// replica before the hint is handed off: the replica remembers, across the
// restart, that it stored the message, and keeps one copy.
func TestClusterReplicaRestartStoresOnce(t *testing.T) {
	// Hints are handed off on the replica's reconnect only.
	t.Setenv("UNITDB_HANDOFF_INTERVAL", "1h")
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	contract := uint32(0x0c320000)
	cid := newClientID(contract)
	own := c.node(names[0])
	topic := topicOwnedBy(own.name, contract, "groups.restartonce", names...)
	ring := rh.NewRing(clusterHashReplicas, nil)
	ring.Add(names...)
	replica := c.node(ring.GetN(fmt.Sprintf("%d/%s", contract, topic), 2)[1])

	p, err := dial(ctx, own.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	// Frozen for less than failure detection: the replica stays in the ring.
	if err := syscall.Kill(replica.cmd.Process.Pid, syscall.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	if id, _ := p.publish(1, topic, encodePayload(0, "once"), "1h"); !p.waitAck(id, 5*time.Second) {
		syscall.Kill(replica.cmd.Process.Pid, syscall.SIGCONT)
		t.Fatal("reliable publish not acknowledged")
	}
	if err := syscall.Kill(replica.cmd.Process.Pid, syscall.SIGCONT); err != nil {
		t.Fatal(err)
	}
	// The replica stores the late copy, and the store syncs it to disk
	// (every second) before the replica is killed.
	time.Sleep(2500 * time.Millisecond)

	replica.stop()
	if err := replica.start(); err != nil {
		t.Fatalf("restart %s: %v", replica.name, err)
	}
	time.Sleep(3 * time.Second) // reconnect, and the handoff of the hint
	own.stop()

	r, err := dial(ctx, replica.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer r.close()
	if _, err := r.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "reader@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	r.relay(topic, "1h")
	count := 0
	for {
		msg, ok := r.waitPub(2 * time.Second)
		if !ok {
			break
		}
		count += len(msg.Messages)
	}
	if count != 1 {
		t.Errorf("relay on %s, restarted with the owner stopped, returned the message %d times", replica.name, count)
	}
}

// TestClusterMixedCapabilities runs a cluster with one node that can do none
// of the calls added for replication and batched delivery, as an older node,
// and checks that the others fall back where it is concerned: delivery still
// works across every node, messages are still replicated between the
// others, and sessions still resume.
func TestClusterMixedCapabilities(t *testing.T) {
	old := "three"
	start := func(t *testing.T) *cluster {
		t.Helper()
		c := startClusterWith(t, clusterOpts{env: map[string][]string{old: {"UNITDB_CLUSTER_CAPS=none"}}}, names...)
		if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
			t.Fatal(err)
		}
		return c
	}
	var capable []string
	for _, n := range names {
		if n != old {
			capable = append(capable, n)
		}
	}
	ring := rh.NewRing(clusterHashReplicas, nil)
	ring.Add(names...)
	holders := func(key string) []string { return ring.GetN(key, 2) }

	t.Run("delivery", func(t *testing.T) {
		c := start(t)
		contract := uint32(0x0c330000)
		for _, mode := range []struct {
			name     string
			sub, pub uint8
		}{{"express", 0, 0}, {"reliable", 1, 1}} {
			for _, own := range names {
				for _, sub := range c.nodes {
					for _, pub := range c.nodes {
						r := route{
							sub:         sub,
							pub:         pub,
							username:    "subscriber@e2e.test",
							pubUsername: "publisher@e2e.test",
							contract:    contract,
							topic:       topicOwnedBy(own, contract, fmt.Sprintf("groups.mixed.%s.%s.%s", mode.name, sub.name, pub.name), names...),
							mode:        mode.sub,
							pubMode:     mode.pub,
						}
						if !deliversRoute(t, r) {
							t.Errorf("%s: owner %s, sub on %s, pub on %s: not delivered", mode.name, own, sub.name, pub.name)
						}
					}
				}
			}
		}
		c.assertAlive(t, c.nodes, "with a node of fewer capabilities")
	})

	t.Run("replication", func(t *testing.T) {
		c := start(t)
		ctx := context.Background()
		contract := uint32(0x0c340000)
		cid := newClientID(contract)
		// A topic the two capable nodes hold, and one the old node is a
		// replica of.
		var shared, withOld string
		for i := 0; shared == "" || withOld == ""; i++ {
			topic := fmt.Sprintf("groups.mixedrep.t%d", i)
			h := holders(fmt.Sprintf("%d/%s", contract, topic))
			switch {
			case h[0] != old && h[1] != old && shared == "":
				shared = topic
			case h[0] != old && h[1] == old && withOld == "":
				withOld = topic
			}
		}
		owner := c.node(holders(fmt.Sprintf("%d/%s", contract, shared))[0])
		other := c.node(holders(fmt.Sprintf("%d/%s", contract, shared))[1])

		p, err := dial(ctx, other.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer p.close()
		if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
			t.Fatal(err)
		}
		n := 10
		for i := 0; i < n; i++ {
			if id, _ := p.publish(1, shared, encodePayload(i, fmt.Sprintf("m%d", i)), "1h"); !p.waitAck(id, 3*time.Second) {
				t.Fatalf("no publish ack on %s", shared)
			}
		}
		// Its replica cannot take it: the publish is acknowledged anyway.
		if id, _ := p.publish(1, withOld, encodePayload(0, "old"), "1h"); !p.waitAck(id, 3*time.Second) {
			t.Errorf("reliable publish on %s, whose replica is %s, not acknowledged", withOld, old)
		}
		time.Sleep(500 * time.Millisecond)

		owner.stop()
		r, err := dial(ctx, other.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer r.close()
		if _, err := r.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "reader@e2e.test"}); err != nil {
			t.Fatal(err)
		}
		r.relay(shared, "1h")
		if _, _, err := collectUnique(r, n, 10*time.Second, 3*time.Second); err != nil {
			t.Errorf("relay of %s on %s after its owner %s stopped: %v", shared, other.name, owner.name, err)
		}
	})

	t.Run("sessions", func(t *testing.T) {
		c := start(t)
		ctx := context.Background()
		contract := uint32(0x0c350000)
		cid := newClientID(contract)
		home := c.node(capable[0])
		spare := c.node(capable[1])
		// A session whose replicas include the other capable node.
		var s *client
		var opts connectOpts
		for attempt := 0; attempt < 50 && s == nil; attempt++ {
			opts = connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}
			cl, err := dial(ctx, home.tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			cl.holdNotify.Store(true)
			ack, err := cl.connectWith(opts)
			if err != nil {
				t.Fatal(err)
			}
			h := holders(fmt.Sprintf("session/%d", uint32(ack.ConnID)))
			if h[0] == spare.name || h[1] == spare.name {
				s = cl
			} else {
				cl.close()
			}
		}
		if s == nil {
			t.Fatal("no session found replicated to " + spare.name)
		}
		defer s.close()
		topic := "groups.mixedsession"
		if sid, _ := s.subscribe(1, topic); !s.waitAck(sid, 3*time.Second) {
			t.Fatal("no subscribe ack")
		}
		time.Sleep(150 * time.Millisecond)

		p, err := dial(ctx, spare.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer p.close()
		if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
			t.Fatal(err)
		}
		n := 3
		for i := 0; i < n; i++ {
			if id, _ := p.publish(1, topic, encodePayload(i, fmt.Sprintf("s%d", i)), "1h"); !p.waitAck(id, 3*time.Second) {
				t.Fatalf("no publish ack for message %d", i)
			}
		}
		deadline := time.After(5 * time.Second)
		for notified := 0; notified < n; {
			select {
			case m := <-s.ctrl:
				if m.FlowControl == utp.NOTIFY {
					notified++
				}
			case <-deadline:
				t.Fatalf("subscriber was notified of %d of %d messages", notified, n)
			}
		}
		time.Sleep(500 * time.Millisecond) // replication is asynchronous

		home.stop()
		s.close()
		time.Sleep(4 * time.Second) // failure detection and rehash
		resumed := opts
		resumed.resume = true
		r, err := dial(ctx, spare.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer r.close()
		if _, err := r.connectWith(resumed); err != nil {
			t.Fatal(err)
		}
		if _, _, err := collectUnique(r, n, 10*time.Second, 3*time.Second); err != nil {
			t.Errorf("session resumed on %s after %s died: %v", spare.name, home.name, err)
		}
	})
}

// TestClusterDrainOnSIGTERM shuts a node down with SIGTERM while the others
// publish on its topics: it leaves the cluster first, so every publish is
// acknowledged, and quickly, with no failure detection to wait for; a
// subscription it held for another node's client moves without the client
// subscribing again; and a client of it resumes its session elsewhere with
// its pending messages.
func TestClusterDrainOnSIGTERM(t *testing.T) {
	for _, role := range []string{"follower", "leader"} {
		t.Run("drain "+role, func(t *testing.T) {
			c := startCluster(t, names...)
			leader, err := c.waitLeader(c.nodes, 10*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			victim := leader
			if role == "follower" {
				victim = followerOf(leader)
			}
			draining := c.node(victim)
			var live []*clusterNode
			for _, n := range c.nodes {
				if n != draining {
					live = append(live, n)
				}
			}
			ctx := context.Background()
			contract := uint32(0x0c360000)
			cid := newClientID(contract)
			pubTopic := topicOwnedBy(victim, contract, "groups.drain.pub", names...)
			subTopic := topicOwnedBy(victim, contract, "groups.drain.sub", names...)
			sessTopic := "groups.drain.session"
			connect := func(node *clusterNode, opts connectOpts) *client {
				t.Helper()
				cl, err := dial(ctx, node.tcpAddr)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := cl.connectWith(opts); err != nil {
					t.Fatal(err)
				}
				return cl
			}

			// A client of another node, subscribed to a topic of the node.
			s := connect(live[1], connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"})
			defer s.close()
			if sid, _ := s.subscribe(0, subTopic); !s.waitAck(sid, 3*time.Second) {
				t.Fatal("no subscribe ack")
			}
			// A client of the node, with reliable messages pending.
			sessOpts := connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "session@e2e.test"}
			held, err := dial(ctx, draining.tcpAddr)
			if err != nil {
				t.Fatal(err)
			}
			defer held.close()
			held.holdNotify.Store(true)
			if _, err := held.connectWith(sessOpts); err != nil {
				t.Fatal(err)
			}
			if sid, _ := held.subscribe(1, sessTopic); !held.waitAck(sid, 3*time.Second) {
				t.Fatal("no subscribe ack")
			}
			time.Sleep(150 * time.Millisecond) // let the forwarded subscriptions settle
			p := connect(live[0], connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"})
			defer p.close()
			pending := 3
			for i := 0; i < pending; i++ {
				if id, _ := p.publish(1, sessTopic, encodePayload(i, fmt.Sprintf("p%d", i)), "1h"); !p.waitAck(id, 3*time.Second) {
					t.Fatalf("no publish ack for pending message %d", i)
				}
			}
			deadline := time.After(5 * time.Second)
			for notified := 0; notified < pending; {
				select {
				case m := <-held.ctrl:
					if m.FlowControl == utp.NOTIFY {
						notified++
					}
				case <-deadline:
					t.Fatalf("client of %s was notified of %d of %d messages", victim, notified, pending)
				}
			}

			stopped := make(chan struct{})
			go func() {
				draining.shutdown()
				close(stopped)
			}()
			// Publish through the drain and after it.
			n := 40
			var slowest time.Duration
			unacked := 0
			for i := 0; i < n; i++ {
				start := time.Now()
				if id, _ := p.publish(1, pubTopic, encodePayload(i, fmt.Sprintf("d%d", i)), "1h"); !p.waitAck(id, 5*time.Second) {
					unacked++
				} else if d := time.Since(start); d > slowest {
					slowest = d
				}
				time.Sleep(50 * time.Millisecond)
			}
			<-stopped
			t.Logf("slowest publish acknowledged in %v", slowest.Round(time.Millisecond))
			if unacked > 0 {
				t.Errorf("%d of %d publishes on %s not acknowledged while %s shut down", unacked, n, pubTopic, victim)
			}
			if slowest > time.Second {
				t.Errorf("a publish on %s took %v while %s shut down: waited for failure detection", pubTopic, slowest.Round(time.Millisecond), victim)
			}

			// The subscription held on the node moved.
			if !publishReaches(t, s, live[0], cid, subTopic) {
				t.Errorf("publish on %s did not reach the subscriber on %s after %s left", subTopic, live[1].name, victim)
			}
			// Every acknowledged publish is stored.
			r := connect(live[1], connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "reader@e2e.test"})
			defer r.close()
			r.relay(pubTopic, "1h")
			if _, _, err := collectUnique(r, n, 10*time.Second, 3*time.Second); err != nil {
				t.Errorf("relay of %s after %s left: %v", pubTopic, victim, err)
			}
			// The node's client resumes elsewhere with its pending messages.
			resumed := sessOpts
			resumed.resume = true
			rs := connect(live[0], resumed)
			defer rs.close()
			if _, _, err := collectUnique(rs, pending, 10*time.Second, 3*time.Second); err != nil {
				t.Errorf("session of a client of %s, resumed on %s: %v", victim, live[0].name, err)
			}
			c.assertAlive(t, live, "after a node drained")
		})
	}
}

var ringVersionIs = regexp.MustCompile(`cluster: ring version (\d+)`)

// ringVersionSeen returns the ring version a node last logged, or "".
func (c *cluster) ringVersionSeen(n *clusterNode) string {
	version := ""
	for _, line := range strings.Split(n.logs.String(), "\n") {
		if m := ringVersionIs.FindStringSubmatch(line); m != nil {
			version = m[1]
		}
	}
	return version
}

// waitRingVersion waits until every node routes by ring version v.
func (c *cluster) waitRingVersion(t *testing.T, v string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		seen := map[string]string{}
		all := true
		for _, n := range c.nodes {
			seen[n.name] = c.ringVersionSeen(n)
			all = all && seen[n.name] == v
		}
		if all {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("nodes did not all route by ring version %s: %v", v, seen)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestClusterRingVersionSwitch runs a cluster with a node that supports only
// ring version 1, then upgrades it: the leader switches the cluster to
// version 2 once every node supports it, the topics' owners change, and a
// subscription moves to its topic's new owner without the client subscribing
// again.
func TestClusterRingVersionSwitch(t *testing.T) {
	old := "three"
	c := startClusterWith(t, clusterOpts{env: map[string][]string{old: {"UNITDB_RING_VERSIONS=1"}}}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	c.waitRingVersion(t, "1", 10*time.Second)

	contract := uint32(0x0c370000)
	cid := newClientID(contract)
	v1 := rh.NewRing(20, rh.FNV32a)
	v1.Add(names...)
	v2 := rh.NewRing(clusterHashReplicas, nil)
	v2.Add(names...)
	// A topic whose owner changes with the switch, and a subscriber on the
	// third node, which is not the one upgraded.
	var topic, ownerV1, ownerV2, third string
	for i := 0; topic == ""; i++ {
		t0 := fmt.Sprintf("groups.ringswitch.t%d", i)
		key := fmt.Sprintf("%d/%s", contract, t0)
		o1, o2 := v1.Get(key), v2.Get(key)
		if o1 == o2 {
			continue
		}
		for _, n := range names {
			if n != o1 && n != o2 && n != old {
				topic, ownerV1, ownerV2, third = t0, o1, o2, n
			}
		}
	}

	s, err := dial(context.Background(), c.node(third).tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if _, err := s.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	if sid, _ := s.subscribe(0, topic); !s.waitAck(sid, 3*time.Second) {
		t.Fatal("no subscribe ack")
	}
	time.Sleep(150 * time.Millisecond)
	if !publishReaches(t, s, c.node(ownerV1), cid, topic) {
		t.Fatalf("on ring version 1, publish on %s did not reach the subscriber on %s", topic, third)
	}

	// Upgrade the old node.
	up := c.node(old)
	up.stop()
	up.env = nil
	if err := up.start(); err != nil {
		t.Fatalf("restart %s: %v", old, err)
	}
	c.waitRingVersion(t, "2", 15*time.Second)
	time.Sleep(time.Second) // the rebalance moves the subscription

	// The owner in version 2 does not forward the publish: it reaches the
	// subscriber only if the subscription moved there.
	if !publishReaches(t, s, c.node(ownerV2), cid, topic) {
		t.Errorf("on ring version 2, publish on %s at its owner %s (was %s) did not reach the subscriber on %s", topic, ownerV2, ownerV1, third)
	}
	// New subscriptions, on version 2.
	for _, own := range names {
		for _, sub := range c.nodes {
			r := route{sub: sub, pub: c.node(own), username: "subscriber@e2e.test", pubUsername: "publisher@e2e.test", contract: contract,
				topic: topicOwnedBy(own, contract, "groups.ringswitch.new."+sub.name, names...)}
			if !deliversRoute(t, r) {
				t.Errorf("on ring version 2: owner %s, sub on %s: not delivered", own, sub.name)
			}
		}
	}
	c.assertAlive(t, c.nodes, "through a ring switch")
}

// TestClusterRingSwitchMovesHistory stores messages on a topic whose owner
// after a switch of the ring from version 1 to 2 held none of them before,
// switches, and checks a relay from every node then returns every message
// exactly once: the topic's first holder before the switch handed them to its
// new holders.
func TestClusterRingSwitchMovesHistory(t *testing.T) {
	old := "three"
	c := startClusterWith(t, clusterOpts{env: map[string][]string{old: {"UNITDB_RING_VERSIONS=1"}}}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	c.waitRingVersion(t, "1", 10*time.Second)

	ctx := context.Background()
	contract := uint32(0x0c380000)
	cid := newClientID(contract)
	v1 := rh.NewRing(20, rh.FNV32a)
	v1.Add(names...)
	v2 := rh.NewRing(clusterHashReplicas, nil)
	v2.Add(names...)
	var topic string
	for i := 0; topic == ""; i++ {
		t0 := fmt.Sprintf("groups.ringmove.t%d", i)
		key := fmt.Sprintf("%d/%s", contract, t0)
		was := v1.GetN(key, 2)
		if owner := v2.Get(key); owner != was[0] && owner != was[1] {
			topic = t0
		}
	}

	p, err := dial(ctx, c.nodes[0].tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	n := 20
	for i := 0; i < n; i++ {
		if id, _ := p.publish(1, topic, encodePayload(i, fmt.Sprintf("h%d", i)), "1h"); !p.waitAck(id, 3*time.Second) {
			t.Fatalf("no publish ack for message %d", i)
		}
	}
	p.close()
	time.Sleep(500 * time.Millisecond) // replication is asynchronous

	up := c.node(old)
	up.stop()
	up.env = nil
	if err := up.start(); err != nil {
		t.Fatalf("restart %s: %v", old, err)
	}
	c.waitRingVersion(t, "2", 15*time.Second)
	// Every node moved what it had to.
	deadline := time.Now().Add(10 * time.Second)
	for _, node := range c.nodes {
		for !strings.Contains(node.logs.String(), "cluster: moved history for ring version 2") {
			if time.Now().After(deadline) {
				t.Fatalf("%s did not move history for ring version 2", node.name)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	for _, rel := range c.nodes {
		r, err := dial(ctx, rel.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := r.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "reader@e2e.test"}); err != nil {
			t.Fatal(err)
		}
		r.relay(topic, "1h")
		got, dups, err := collectUnique(r, n, 10*time.Second, 3*time.Second)
		for {
			more, ok := r.waitPub(500 * time.Millisecond)
			if !ok {
				break
			}
			dups += len(more.Messages)
		}
		r.close()
		if err != nil {
			t.Errorf("relay on %s after the switch: %v (%d of %d)", rel.name, err, len(got), n)
		}
		if dups > 0 {
			t.Errorf("relay on %s after the switch: %d duplicate(s)", rel.name, dups)
		}
	}
}

// OldCluster answers as a node of unitdb v0.3.0 does: Ping, with a bool answer,
// and none of the calls added since.
type OldCluster struct{}

// OldPing is v0.3.0's ping.
type OldPing struct {
	Leader    string
	Term      int
	Signature string
	Nodes     []string
}

func (OldCluster) Ping(ping *OldPing, unused *bool) error { return nil }

// TestClusterRefusesOldPeer starts a node whose cluster has a node of
// v0.3.0 up: the node refuses to start, rather than run in a cluster with
// it, and says why.
func TestClusterRefusesOldPeer(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	srv := rpc.NewServer()
	if err := srv.RegisterName("Cluster", OldCluster{}); err != nil {
		t.Fatal(err)
	}
	go srv.Accept(l)

	conf, _ := json.Marshal(map[string]interface{}{
		"self": "",
		"nodes": []map[string]string{
			{"name": "one", "addr": fmt.Sprintf("127.0.0.1:%d", freePort(t))},
			{"name": "old", "addr": l.Addr().String()},
		},
	})
	s := startServerWith(t, serverOpts{cluster: string(conf), args: []string{"-cluster_self", "one"}, expectExit: true})
	select {
	case <-s.exited:
	case <-time.After(10 * time.Second):
		t.Fatal("the node started next to a v0.3.0 node")
	}
	if logs := s.logs.String(); !strings.Contains(logs, "runs a version from before replication") {
		t.Errorf("the node exited without saying why:\n%s", logs)
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

// TestClusterRestartedOwnerKeepsSubscriptions restarts a topic's owner
// before the others fail it over, and publishes on it as soon as it takes
// clients, as a reconnecting client does: the subscriptions it held for
// other nodes' clients must be back by then.
func TestClusterRestartedOwnerKeepsSubscriptions(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	restarted := c.node("one")
	contract := uint32(0x0c3a0000)
	cid := newClientID(contract)

	// Subscribers on the other nodes, to topics the restarted node owns.
	type sub struct {
		c     *client
		topic string
	}
	var subs []sub
	for _, n := range c.nodes {
		if n == restarted {
			continue
		}
		topic := topicOwnedBy(restarted.name, contract, "groups.restartowner."+n.name, names...)
		s, err := dial(context.Background(), n.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer s.close()
		if _, err := s.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}); err != nil {
			t.Fatal(err)
		}
		if sid, _ := s.subscribe(0, topic); !s.waitAck(sid, 3*time.Second) {
			t.Fatal("no subscribe ack")
		}
		subs = append(subs, sub{s, topic})
	}
	time.Sleep(200 * time.Millisecond) // let the forwarded subscriptions settle

	// Down for less than failure detection takes, so it stays in the ring.
	restarted.stop()
	time.Sleep(500 * time.Millisecond)
	if err := restarted.start(); err != nil {
		t.Fatalf("restart %s: %v\nlogs:\n%s", restarted.name, err, restarted.logs.String())
	}
	for _, s := range subs {
		if !publishReaches(t, s.c, restarted, cid, s.topic) {
			t.Errorf("publish on %s right after it restarted did not reach its subscriber of %s", restarted.name, s.topic)
		}
	}
	c.assertAlive(t, c.nodes, "after the restart")
}
