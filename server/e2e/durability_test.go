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

// Durability tests: a node crashes right after it acknowledged data, with no
// pause for replication, and the data must survive on the other nodes. The
// other cluster tests wait for replication before a failure, so they don't
// cover the window between the acknowledgement and the replica's copy.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/utp"
)

// durabilityCount is the number of messages a durability test acknowledges
// before the crash.
const durabilityCount = 20

// crashAfterAck starts a cluster with opts and returns it with a node to
// crash and the surviving nodes.
func crashAfterAck(t *testing.T, opts clusterOpts) (*cluster, *clusterNode, []*clusterNode) {
	t.Helper()
	c := startClusterWith(t, opts, names...)
	leader, err := c.waitLeader(c.nodes, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	victim := c.node(followerOf(leader))
	var live []*clusterNode
	for _, n := range c.nodes {
		if n != victim {
			live = append(live, n)
		}
	}
	return c, victim, live
}

// failOver waits until the survivors have failed the crashed node over.
func failOver(t *testing.T, c *cluster, live []*clusterNode) {
	t.Helper()
	// Allow for failure detection (node_fail_after * heartbeat) and rehash.
	time.Sleep(4 * time.Second)
	if _, err := c.waitLeader(live, 10*time.Second); err != nil {
		t.Fatalf("survivors: %v", err)
	}
}

// testStoredMessagesSurviveCrash publishes one message on each of
// durabilityCount topics that the victim owns, crashes the victim as soon as
// the last publish is acknowledged, and relays every topic from a survivor.
func testStoredMessagesSurviveCrash(t *testing.T, mode uint8) {
	c, victim, live := crashAfterAck(t, clusterOpts{})
	contract := uint32(0x0d010000) + uint32(mode)
	cid := newClientID(contract)

	var topics []string
	for i := 0; i < durabilityCount; i++ {
		topics = append(topics, topicOwnedBy(victim.name, contract, fmt.Sprintf("groups.durable.m%d.%d", mode, i), names...))
	}

	p, err := dial(context.Background(), victim.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	for i, topic := range topics {
		id, _ := p.publish(mode, topic, encodePayload(i, fmt.Sprintf("durable-%d", i)), "1h")
		if !p.waitAck(id, 3*time.Second) {
			t.Fatalf("no publish ack for %s", topic)
		}
	}
	// Crash as soon as everything is acknowledged.
	victim.stop()
	p.close()
	failOver(t, c, live)

	lost := 0
	for i, topic := range topics {
		if !relayFinds(t, live[0], cid, topic, fmt.Sprintf("durable-%d", i)) {
			lost++
		}
	}
	if lost > 0 {
		t.Errorf("%d of %d acknowledged messages lost when their owner %s crashed", lost, len(topics), victim.name)
	}
	c.assertAlive(t, live, "after the crash")
}

// withReplicationDelays runs f as it is, and with asynchronous replication
// delayed, so that the crash always falls in the window.
func withReplicationDelays(t *testing.T, f func(t *testing.T)) {
	t.Run("natural", f)
	t.Run("delayed", func(t *testing.T) {
		t.Setenv("UNITDB_REPLICATION_DELAY", "3s")
		f(t)
	})
}

// A reliable publish waits for a replica: TestClusterReliablePublishSurvivesCrash.
func TestClusterExpressPublishSurvivesCrash(t *testing.T) {
	withReplicationDelays(t, func(t *testing.T) { testStoredMessagesSurviveCrash(t, 0) })
}

func TestClusterSessionSurvivesCrash(t *testing.T) {
	withReplicationDelays(t, testSessionSurvivesCrash)
}

func testSessionSurvivesCrash(t *testing.T) {
	c, victim, live := crashAfterAck(t, clusterOpts{})
	ctx := context.Background()
	contract := uint32(0x0d020000)
	cid := newClientID(contract)
	// A topic a survivor owns, so that only the session's log is at risk.
	topic := topicOwnedBy(live[0].name, contract, "groups.durable.session", names...)
	opts := connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "subscriber@e2e.test"}

	// A subscriber of the session on the victim, notified of reliable
	// messages it does not receive yet.
	s, err := dial(ctx, victim.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	s.holdNotify.Store(true)
	if _, err := s.connectWith(opts); err != nil {
		t.Fatal(err)
	}
	if sid, _ := s.subscribe(1, topic); !s.waitAck(sid, 3*time.Second) {
		t.Fatal("no subscribe ack")
	}
	time.Sleep(150 * time.Millisecond) // let the forwarded subscription settle

	p, err := dial(ctx, live[1].tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < durabilityCount; i++ {
		if id, _ := p.publish(1, topic, encodePayload(i, fmt.Sprintf("s%d", i)), "1h"); !p.waitAck(id, 3*time.Second) {
			t.Fatalf("no publish ack for message %d", i)
		}
	}
	deadline := time.After(5 * time.Second)
	for notified := 0; notified < durabilityCount; {
		select {
		case m := <-s.ctrl:
			if m.FlowControl == utp.NOTIFY {
				notified++
			}
		case <-deadline:
			t.Fatalf("subscriber on %s was notified of %d of %d messages", victim.name, notified, durabilityCount)
		}
	}
	// Crash as soon as the session was notified of everything.
	victim.stop()
	s.close()
	failOver(t, c, live)

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
	got, _, err := collectUnique(r, durabilityCount, 10*time.Second, 3*time.Second)
	if err != nil {
		t.Errorf("session resumed on %s after %s crashed: %v (%d of %d messages)", live[0].name, victim.name, err, len(got), durabilityCount)
	}
	c.assertAlive(t, live, "while resuming the session")
}

// TestClusterAsyncReplication checks that async_replication restores
// acknowledging express publishes before a replica stored them: with
// replication delayed, a crash right after the acknowledgements loses them.
func TestClusterAsyncReplication(t *testing.T) {
	t.Setenv("UNITDB_REPLICATION_DELAY", "3s")
	c, victim, live := crashAfterAck(t, clusterOpts{asyncReplication: true})
	contract := uint32(0x0d040000)
	cid := newClientID(contract)
	var topics []string
	for i := 0; i < durabilityCount; i++ {
		topics = append(topics, topicOwnedBy(victim.name, contract, fmt.Sprintf("groups.async.%d", i), names...))
	}
	p, err := dial(context.Background(), victim.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess(), username: "publisher@e2e.test"}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	for i, topic := range topics {
		if id, _ := p.publish(0, topic, encodePayload(i, fmt.Sprintf("async-%d", i)), "1h"); !p.waitAck(id, 3*time.Second) {
			t.Fatalf("no publish ack for %s", topic)
		}
	}
	// Acknowledged without waiting for the delayed replicas.
	if d := time.Since(start); d > time.Second {
		t.Errorf("%d express publishes took %v to be acknowledged: they waited for a replica", len(topics), d)
	}
	victim.stop()
	p.close()
	failOver(t, c, live)
	// One lost message shows it; each relay that finds nothing takes seconds.
	lost := 0
	for i, topic := range topics {
		if !relayFinds(t, live[0], cid, topic, fmt.Sprintf("async-%d", i)) {
			lost++
			break
		}
	}
	if lost == 0 {
		t.Errorf("no message was lost: async_replication did not stop publishes waiting for a replica")
	}
	c.assertAlive(t, live, "after the crash")
}
