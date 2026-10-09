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
