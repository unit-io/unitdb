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

// Security stage 3 (unitdb_internal's release 3): TLS required between nodes,
// encrypt_at_rest on by default, and the store's own records kept under $sys
// topics, with a store written by v0.6.0 moved there when it opens.

import (
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/types"
)

// TestReleaseThreeRequireTLS checks that a cluster with TLS required on every
// node delivers, and that its nodes take no plain cluster connection.
func TestReleaseThreeRequireTLS(t *testing.T) {
	p := newTestPKI(t)
	tlsConf := map[string]map[string]interface{}{}
	for _, n := range names {
		tlsConf[n] = p.tlsConf(t, n, true)
	}
	c := startClusterWith(t, clusterOpts{tls: tlsConf}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	c.deliversEach(t, 0x3e1ea5e1, "groups.requiretls", "with TLS required")
	for _, n := range c.nodes {
		if conn, err := net.DialTimeout("tcp", n.addr, time.Second); err == nil {
			conn.Close()
			t.Errorf("node %s takes plain cluster connections with TLS required", n.name)
		}
	}
	c.assertAlive(t, c.nodes, "with TLS required")
}

// noneComes reports whether c is sent no message on topic within d.
func noneComes(c *client, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for {
		p, ok := c.waitPub(time.Until(deadline))
		if !ok {
			return true
		}
		for _, m := range p.Messages {
			if !strings.HasPrefix(m.Topic, "unitdb/") {
				return false
			}
		}
	}
}

// TestSysTopicsOutOfReach checks that no client request addresses the
// store's own records: a $sys topic is refused, even to a trusted service,
// and a wildcard relay or subscription does not reach below $sys.
func TestSysTopicsOutOfReach(t *testing.T) {
	s := startServer(t)
	contract := uint32(0x3e5e5000)
	service := serviceClientID(contract)
	topic := "groups.sys.reach"

	// A subscription, stored under $sys.sub.<topic> in the contract's
	// namespace.
	sub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: service})
	if err != nil {
		t.Fatal(err)
	}
	if sid, _ := sub.subscribe(0, topic); !sub.waitAck(sid, 3*time.Second) {
		t.Fatal("no subscribe ack")
	}

	c, err := connectTo(t, s.tcpAddr, connectOpts{clientID: service})
	if err != nil {
		t.Fatal(err)
	}
	for _, sys := range []string{"$sys.sub." + topic, "$sys.replica." + topic, "$sys.index.topics", "$sys.seen.seen", "$sys.security.state", "$sys..."} {
		c.relay(sys, "1h")
		if st := errorStatus(t, c); st != types.ErrForbidden.Status {
			t.Errorf("relay of %s: status %d, want %d", sys, st, types.ErrForbidden.Status)
		}
		c.subscribe(0, sys)
		if st := errorStatus(t, c); st != types.ErrForbidden.Status {
			t.Errorf("subscribe to %s: status %d, want %d", sys, st, types.ErrForbidden.Status)
		}
		c.publish(0, sys, []byte("x"), "1m")
		if st := errorStatus(t, c); st != types.ErrForbidden.Status {
			t.Errorf("publish to %s: status %d, want %d", sys, st, types.ErrForbidden.Status)
		}
	}
	// Wildcards match from the topic's first part: none reaches $sys.
	for _, wild := range []string{"...", "*.sub." + topic, "*.*." + topic, "*.sub..."} {
		r, err := connectTo(t, s.tcpAddr, connectOpts{clientID: service})
		if err != nil {
			t.Fatal(err)
		}
		r.relay(wild, "1h")
		if !noneComes(r, time.Second) {
			t.Errorf("a relay of %s returned the store's own records", wild)
		}
	}
	// The subscription still works.
	if !publishReaches(t, sub, standalone(s), service, topic) {
		t.Error("the subscription was not delivered to")
	}
	assertHealthy(t, s, "requests for $sys topics")
}

// The v0.6.0 server, for the upgrade tests.
const previousRelease = "v0.6.0"

// ownedThenBy returns a topic of contract owned by first among all the
// nodes, and by then once first is gone.
func ownedThenBy(t *testing.T, first, then string, contract uint32, prefix string) string {
	t.Helper()
	var rest []string
	for _, n := range names {
		if n != first {
			rest = append(rest, n)
		}
	}
	for i := 0; i < 10000; i++ {
		topic := fmt.Sprintf("%s.%d", prefix, i)
		if topicOwner(contract, topic, names...) == first && topicOwner(contract, topic, rest...) == then {
			return topic
		}
	}
	t.Fatal("no such topic")
	return ""
}

// upgrade restarts n with bin, on the same store, and waits for the cluster
// to settle.
func (c *cluster) upgrade(t *testing.T, n *clusterNode, bin string, live []*clusterNode) {
	t.Helper()
	n.shutdown()
	n.switchBinary(bin)
	if err := n.start(); err != nil {
		t.Fatalf("start %s with %s: %v\n%s", n.name, bin, err, n.logs.String())
	}
	time.Sleep(3 * time.Second)
	if _, err := c.waitLeader(live, 10*time.Second); err != nil {
		t.Fatalf("after %s was upgraded: %v", n.name, err)
	}
}

// TestRollingUpgradeFromV060 upgrades a v0.6.0 cluster node by node, as
// docs/rolling-deploys.md describes, one node while it is down: every node
// moves what v0.6.0 stored (replicas, the topic index, hints, the security
// state) under $sys topics as it starts, and the cluster delivers, relays
// replicated messages, hands off hints and refuses what was revoked at each
// step.
func TestRollingUpgradeFromV060(t *testing.T) {
	old := oldServerBinary(t, previousRelease)
	bin := serverBinary(t)
	c := startClusterWith(t, clusterOpts{replicas: 3, logLevel: "Info", bin: map[string]string{"one": old, "two": old, "three": old}}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitCapabilities()
	one, two, three := c.node("one"), c.node("two"), c.node("three")
	contract := uint32(0x3e06a000)
	cid := newClientID(contract)

	// Replicated on every node, owned by one, and by three once one is gone.
	before := ownedThenBy(t, "one", "three", contract, "groups.upgrade.before")
	storeOn(t, two, cid, []string{before}, "stored by v0.6.0")
	// A client id revoked in v0.6.0.
	admin, err := connectTo(t, one.tcpAddr, connectOpts{clientID: primaryClientID(contract)})
	if err != nil {
		t.Fatal(err)
	}
	victim, victimUuid := secondaryUuid(t, admin)
	revoke(t, admin, types.RevokeRequest{Uuid: victimUuid})
	time.Sleep(500 * time.Millisecond) // replication and revocations are sent asynchronously
	for _, n := range c.nodes {
		if connects(t, n.server, victim) {
			t.Fatalf("control: the revoked id connects to %s on v0.6.0", n.name)
		}
	}

	// Three goes down; what is stored meanwhile is kept for it as hints, by
	// v0.6.0.
	three.stop()
	hinted := ownedThenBy(t, "one", "three", contract, "groups.upgrade.hinted")
	storeOn(t, one, cid, []string{hinted}, "hinted by v0.6.0")

	// One, then two, upgraded while three is down.
	c.upgrade(t, one, bin, []*clusterNode{one, two})
	if !strings.Contains(one.logs.String(), "moved the hints an older version kept for three") {
		t.Errorf("one did not move the hints v0.6.0 kept for three:\n%s", one.logs.String())
	}
	if !delivers(t, one, two, "mixed@e2e.test", contract, topicOwnedBy("two", contract, "groups.upgrade.mixed", "one", "two")) {
		t.Error("v0.6.0 and v0.7.0 nodes do not deliver to each other")
	}
	if connects(t, one.server, victim) {
		t.Error("the id revoked in v0.6.0 connects to one after its upgrade")
	}
	c.upgrade(t, two, bin, []*clusterNode{one, two})

	// Three comes back as v0.7.0, on the store v0.6.0 left, and is handed
	// the hints the others moved.
	c.upgrade(t, three, bin, c.nodes)
	if !strings.Contains(three.logs.String(), "moved the topic index and replicas of an older version") {
		t.Errorf("three did not move the replicas v0.6.0 stored:\n%s", three.logs.String())
	}
	eventually(t, 10*time.Second, "one hands its hints off to three", func() bool {
		return strings.Contains(one.logs.String(), "handed off to three")
	})
	c.deliversEach(t, contract+1, "groups.upgrade.after", "after the upgrade")
	for _, n := range c.nodes {
		if connects(t, n.server, victim) {
			t.Errorf("the id revoked in v0.6.0 connects to %s after the upgrade", n.name)
		}
		if !relayFinds(t, n, cid, before, "stored by v0.6.0") {
			t.Errorf("relay on %s after the upgrade: the message v0.6.0 stored is not found", n.name)
		}
	}

	// One goes: three owns the topics, and relays what it stores as a
	// replica, moved from v0.6.0's namespace and handed off as hints.
	one.stop()
	time.Sleep(4 * time.Second)
	if _, err := c.waitLeader([]*clusterNode{two, three}, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	for topic, body := range map[string]string{before: "stored by v0.6.0", hinted: "hinted by v0.6.0"} {
		if !relayFinds(t, three, cid, topic, body) {
			t.Errorf("relay on three, which owns %s now: %q is not found", topic, body)
		}
	}
	c.assertAlive(t, []*clusterNode{two, three}, "after the upgrade")
}

// TestClusterMixedV060 runs a cluster of a v0.6.0 node and two v0.7.0
// nodes, as a rolling deploy does: they deliver to each other, replicate to
// each other, and share revocations.
func TestClusterMixedV060(t *testing.T) {
	old := oldServerBinary(t, previousRelease)
	c := startClusterWith(t, clusterOpts{bin: map[string]string{"one": old}}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitCapabilities()
	contract := uint32(0x3e06b000)
	c.deliversEach(t, contract, "groups.mixed", "a v0.6.0 node among v0.7.0 ones")

	// Revoked on a v0.7.0 node, refused on the v0.6.0 one; and the other
	// way round.
	cid := newClientID(contract)
	for _, at := range []string{"two", "one"} {
		admin, err := connectTo(t, c.node(at).tcpAddr, connectOpts{clientID: primaryClientID(contract)})
		if err != nil {
			t.Fatal(err)
		}
		victim, uuid := secondaryUuid(t, admin)
		revoke(t, admin, types.RevokeRequest{Uuid: uuid})
		for _, n := range c.nodes {
			eventually(t, 5*time.Second, "an id revoked on "+at+" is refused on "+n.name, func() bool {
				return !connects(t, n.server, victim)
			})
		}
	}

	// Replicas both ways: a message owned by each node is relayed by the
	// others once its owner is gone.
	for _, own := range []string{"one", "two"} {
		topic := topicOwnedBy(own, contract, "groups.mixed.replicated."+own, names...)
		storeOn(t, c.node("three"), cid, []string{topic}, "replicated from "+own)
	}
	time.Sleep(500 * time.Millisecond)
	// A replica on a v0.7.0 node is under $sys: a wildcard relay does not
	// reach it.
	for _, n := range c.nodes[1:] {
		r, err := connectTo(t, n.tcpAddr, connectOpts{clientID: serviceClientID(contract)})
		if err != nil {
			t.Fatal(err)
		}
		r.relay("*.replica.groups.mixed...", "1h")
		if !noneComes(r, time.Second) {
			t.Errorf("a wildcard relay on %s reached the replicas", n.name)
		}
	}
	for _, own := range []string{"one", "two"} {
		dead := c.node(own)
		dead.stop()
		var live []*clusterNode
		for _, n := range c.nodes {
			if n != dead {
				live = append(live, n)
			}
		}
		time.Sleep(4 * time.Second)
		if _, err := c.waitLeader(live, 10*time.Second); err != nil {
			t.Fatal(err)
		}
		topic := topicOwnedBy(own, contract, "groups.mixed.replicated."+own, names...)
		for _, n := range live {
			if !relayFinds(t, n, cid, topic, "replicated from "+own) {
				t.Errorf("relay on %s with %s (the owner) down: not found", n.name, own)
			}
		}
		if err := dead.start(); err != nil {
			t.Fatal(err)
		}
		time.Sleep(3 * time.Second)
		if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	c.assertAlive(t, c.nodes, "in a mixed cluster")
}
