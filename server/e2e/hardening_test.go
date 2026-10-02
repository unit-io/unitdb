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

// Hardening tests: the insecure flag is the server's to allow, not the
// client's; trusted services are known by their client ids, and vouch for
// the connections they open for users; reserved topics are out of reach;
// sessions are their owners'.

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/utp"
)

// connectTo dials addr and connects with o. The client is closed on cleanup.
func connectTo(t *testing.T, addr string, o connectOpts) (*client, error) {
	t.Helper()
	c, err := dial(context.Background(), addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.close)
	if o.sessKey == 0 {
		o.sessKey = nextSess()
	}
	_, err = c.connectWith(o)
	return c, err
}

// waitTopic waits for a publish on topic, and returns its first message.
func (c *client) waitTopic(topic string, d time.Duration) (*utp.PublishMessage, bool) {
	deadline := time.Now().Add(d)
	for {
		p, ok := c.waitPub(time.Until(deadline))
		if !ok {
			return nil, false
		}
		for _, m := range p.Messages {
			if m.Topic == topic {
				return m, true
			}
		}
	}
}

// request sends the special request unitdb/name with payload on c and
// returns the answer.
func request(t *testing.T, c *client, name string, payload interface{}) []byte {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.publish(0, "unitdb/"+name, b, ""); err != nil {
		t.Fatal(err)
	}
	m, ok := c.waitTopic("unitdb/"+name, 3*time.Second)
	if !ok {
		t.Fatalf("no answer to unitdb/%s", name)
	}
	return m.Payload
}

// answerStatus returns the status of a special request's answer: an object,
// or the first of an array of them.
func answerStatus(t *testing.T, answer []byte) int {
	t.Helper()
	var one struct {
		Status int `json:"status"`
	}
	if err := json.Unmarshal(answer, &one); err == nil {
		return one.Status
	}
	var many []struct {
		Status int `json:"status"`
	}
	if err := json.Unmarshal(answer, &many); err != nil || len(many) == 0 {
		t.Fatalf("answer %s: %v", answer, err)
	}
	return many[0].Status
}

// vouch sends unitdb/service on c with a service's client id, and returns
// the answer's status.
func vouch(t *testing.T, c *client, serviceID string) int {
	t.Helper()
	return answerStatus(t, request(t, c, "service", map[string]string{"client_id": serviceID}))
}

// keygen requests a read/write key for topic on c, and returns the answer's
// status.
func keygen(t *testing.T, c *client, topic string) int {
	t.Helper()
	return answerStatus(t, request(t, c, "keygen", []types.KeyGenRequest{{Topic: topic, Type: "rw"}}))
}

// deliveredUnkeyed subscribes on sub and publishes on pub, both already
// connected, on topic without a key, and reports whether it was delivered.
func deliveredUnkeyed(t *testing.T, sub, pub *client, topic string) bool {
	t.Helper()
	sid, _ := sub.subscribe(0, topic)
	sub.waitAck(sid, 3*time.Second)
	time.Sleep(150 * time.Millisecond) // let a forwarded subscription settle
	body := fmt.Sprintf("unkeyed-%d", time.Now().UnixNano())
	pub.publish(0, topic, encodePayload(0, body), "1m")
	deadline := time.Now().Add(2 * time.Second)
	for {
		msg, ok := sub.waitPub(time.Until(deadline))
		if !ok {
			return false
		}
		for _, m := range msg.Messages {
			if _, b, ok := decodePayload(m.Payload); ok && m.Topic == topic && string(b) == body {
				return true
			}
		}
	}
}

// errorStatus waits for an error notice on c and returns its status.
func errorStatus(t *testing.T, c *client) int {
	t.Helper()
	m, ok := c.waitTopic("unitdb/error/", 3*time.Second)
	if !ok {
		t.Fatal("no error notice")
	}
	var e types.Error
	if err := json.Unmarshal(m.Payload, &e); err != nil {
		t.Fatalf("error notice %s: %v", m.Payload, err)
	}
	return e.Status
}

// TestInsecureFlagRefused checks that a server refuses a client's insecure
// flag unless allow_insecure is set, and serves the refused connection
// nothing.
func TestInsecureFlagRefused(t *testing.T) {
	s := startServer(t)
	cid := newClientID(0x1a5ec001)
	c, err := dial(context.Background(), s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	ack, err := c.connectWith(connectOpts{clientID: cid, insecure: true, sessKey: nextSess()})
	if err == nil {
		t.Fatal("a client with the insecure flag connected to a server without allow_insecure")
	}
	if ack == nil || ack.ReturnCode != types.ErrUnauthorized.ReturnCode {
		t.Fatalf("insecure connect: %v, want return code %d", err, types.ErrUnauthorized.ReturnCode)
	}
	// The refused connection is not served: a request closes it.
	c.subscribe(0, "groups.refused.x")
	select {
	case <-c.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("a refused connection was kept open after a request")
	}

	// The same client id connects without the flag, and needs topic keys.
	sub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: cid})
	if err != nil {
		t.Fatal(err)
	}
	pub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: cid})
	if err != nil {
		t.Fatal(err)
	}
	if deliveredUnkeyed(t, sub, pub, "groups.refused.y") {
		t.Fatal("a client without topic keys was delivered to")
	}
	assertHealthy(t, s, "an insecure client refused")
}

func TestInsecureFlagAllowedStandalone(t *testing.T) {
	s := startServerWith(t, serverOpts{allowInsecure: true})
	cid := newClientID(0x1a5ec002)
	sub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: cid, insecure: true})
	if err != nil {
		t.Fatalf("an insecure client was refused with allow_insecure: %v", err)
	}
	pub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: cid, insecure: true})
	if err != nil {
		t.Fatal(err)
	}
	if !deliveredUnkeyed(t, sub, pub, "groups.insecure.x") {
		t.Fatal("insecure clients needed topic keys with allow_insecure")
	}
	// Reserved topics stay out of reach.
	sub.subscribe(0, "$sys.x")
	if st := errorStatus(t, sub); st != types.ErrForbidden.Status {
		t.Errorf("subscribe to a reserved topic: status %d, want %d", st, types.ErrForbidden.Status)
	}
	// The flag gives no keys: they would outlast insecure mode.
	if st := keygen(t, sub, "groups.insecure.key"); st != types.ErrKeyGenForbidden.Status {
		t.Errorf("keygen by an insecure client: status %d, want %d", st, types.ErrKeyGenForbidden.Status)
	}
}

// TestClusterRefusesAllowInsecure checks that a cluster node refuses to
// start with allow_insecure, and says why.
func TestClusterRefusesAllowInsecure(t *testing.T) {
	c := startClusterWith(t, clusterOpts{allowInsecure: true}, "one", "two")
	for _, n := range c.nodes {
		select {
		case <-n.exited:
		case <-time.After(15 * time.Second):
			t.Fatalf("node %s started with allow_insecure", n.name)
		}
		if logs := n.logs.String(); !strings.Contains(logs, "allow_insecure is set") {
			t.Errorf("node %s exited without saying why:\n%s", n.name, logs)
		}
	}
}

func TestServiceIDs(t *testing.T) {
	s := startServer(t)
	contract := uint32(0x5e41ce01)
	service := serviceClientID(contract)

	t.Run("a service needs no topic keys", func(t *testing.T) {
		sub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: service, insecure: true})
		if err != nil {
			t.Fatalf("a service id with the insecure flag was refused: %v", err)
		}
		pub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: service})
		if err != nil {
			t.Fatalf("a service id without the flag was refused: %v", err)
		}
		if !deliveredUnkeyed(t, sub, pub, "groups.service.flag") {
			t.Error("a service's connections needed topic keys")
		}
		// A primary id of the contract that is not a service's needs keys.
		other, err := connectTo(t, s.tcpAddr, connectOpts{clientID: primaryClientID(contract)})
		if err != nil {
			t.Fatal(err)
		}
		if deliveredUnkeyed(t, other, pub, "groups.service.primary") {
			t.Error("a primary id that is not a service's subscribed without a key")
		}
	})

	t.Run("reserved topics stay out of reach", func(t *testing.T) {
		c, err := connectTo(t, s.tcpAddr, connectOpts{clientID: service})
		if err != nil {
			t.Fatal(err)
		}
		c.subscribe(0, "$sys.users")
		if st := errorStatus(t, c); st != types.ErrForbidden.Status {
			t.Errorf("subscribe to a reserved topic: status %d, want %d", st, types.ErrForbidden.Status)
		}
		c.publish(0, "$sys.users", []byte("x"), "1m")
		if st := errorStatus(t, c); st != types.ErrForbidden.Status {
			t.Errorf("publish to a reserved topic: status %d, want %d", st, types.ErrForbidden.Status)
		}
		c.relay("$sys.users", "1h")
		if st := errorStatus(t, c); st != types.ErrForbidden.Status {
			t.Errorf("relay of a reserved topic: status %d, want %d", st, types.ErrForbidden.Status)
		}
		if st := keygen(t, c, "$sys.users"); st != types.ErrForbidden.Status {
			t.Errorf("keygen for a reserved topic: status %d, want %d", st, types.ErrForbidden.Status)
		}
	})

	t.Run("a service vouches for a user's connection", func(t *testing.T) {
		user, err := connectTo(t, s.tcpAddr, connectOpts{clientID: newClientID(contract)})
		if err != nil {
			t.Fatal(err)
		}
		pub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: service})
		if err != nil {
			t.Fatal(err)
		}
		if deliveredUnkeyed(t, user, pub, "groups.service.before") {
			t.Fatal("a connection nothing vouched for subscribed without a key")
		}
		if st := keygen(t, user, "groups.service.key"); st != types.ErrKeyGenForbidden.Status {
			t.Fatalf("keygen before the vouch: status %d, want %d", st, types.ErrKeyGenForbidden.Status)
		}
		if st := vouch(t, user, service); st != 200 {
			t.Fatalf("vouching with a service id: status %d", st)
		}
		if !deliveredUnkeyed(t, user, pub, "groups.service.after") {
			t.Error("a connection a service vouched for still needed topic keys")
		}
		if st := keygen(t, user, "groups.service.key"); st != 200 {
			t.Errorf("keygen on a connection a service vouched for: status %d", st)
		}
	})

	t.Run("only a service of the contract vouches", func(t *testing.T) {
		for name, id := range map[string]string{
			"a primary id":                  primaryClientID(contract),
			"a secondary id":                newClientID(contract),
			"a service of another contract": serviceClientID(contract + 1),
			"garbage":                       "not-a-client-id",
		} {
			user, err := connectTo(t, s.tcpAddr, connectOpts{clientID: newClientID(contract)})
			if err != nil {
				t.Fatal(err)
			}
			if st := vouch(t, user, id); st != types.ErrForbidden.Status {
				t.Errorf("%s vouched: status %d, want %d", name, st, types.ErrForbidden.Status)
			}
		}
	})
	assertHealthy(t, s, "service ids")
}

// TestServiceIDsCluster checks that a service's connections, and the ones a
// service vouched for, need no topic keys on the node owning the topic
// either, and that a node takes the forwarded trust only from nodes that
// advertise the service capability: node one runs as an older node.
func TestServiceIDsCluster(t *testing.T) {
	older := []string{"UNITDB_CLUSTER_CAPS=replicate,deliver,sessions,resync"}
	c := startClusterWith(t, clusterOpts{env: map[string][]string{"one": older}}, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitCapabilities()
	contract := uint32(0x5e41ce02)
	service := serviceClientID(contract)
	two := c.node("two")

	for _, tc := range []struct {
		from      *clusterNode
		delivered bool
	}{
		{c.node("three"), true}, // three advertises the capability: two takes its trust
		{c.node("one"), false},  // one does not: two checks keys
	} {
		topic := topicOwnedBy("two", contract, "groups.service.cluster."+tc.from.name, names...)
		pub, err := connectTo(t, two.tcpAddr, connectOpts{clientID: service})
		if err != nil {
			t.Fatal(err)
		}

		// The service's own connection.
		sub, err := connectTo(t, tc.from.tcpAddr, connectOpts{clientID: service})
		if err != nil {
			t.Fatal(err)
		}
		if got := deliveredUnkeyed(t, sub, pub, topic+".own"); got != tc.delivered {
			t.Errorf("a service on %s, topic owned by two: delivered %t, want %t", tc.from.name, got, tc.delivered)
		}

		// A user's connection the service vouched for.
		user, err := connectTo(t, tc.from.tcpAddr, connectOpts{clientID: newClientID(contract)})
		if err != nil {
			t.Fatal(err)
		}
		if st := vouch(t, user, service); st != 200 {
			t.Fatalf("vouching on %s: status %d", tc.from.name, st)
		}
		if got := deliveredUnkeyed(t, user, pub, topic+".vouched"); got != tc.delivered {
			t.Errorf("vouched on %s, topic owned by two: delivered %t, want %t", tc.from.name, got, tc.delivered)
		}

		// Clients with topic keys are delivered to either way: the older
		// node serves them as before.
		if !delivers(t, tc.from, two, "subscriber@e2e.test", contract, topic+".keyed") {
			t.Errorf("keyed clients on %s, topic owned by two: not delivered", tc.from.name)
		}
	}
	c.assertAlive(t, c.nodes, "with service ids")
}

// TestClusterRefusesInsecureClients checks that every node of a cluster
// refuses a client's insecure flag.
func TestClusterRefusesInsecureClients(t *testing.T) {
	c := startCluster(t, names...)
	if _, err := c.waitLeader(c.nodes, 10*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, n := range c.nodes {
		if _, err := connectTo(t, n.tcpAddr, connectOpts{clientID: newClientID(0x1a5ec003), insecure: true}); err == nil {
			t.Errorf("node %s took a client's insecure flag", n.name)
		}
	}
}

// TestSessionsBoundToOwner checks that a client of the same contract that
// sends another client's session key does not resume its session, while
// the owner does.
func TestSessionsBoundToOwner(t *testing.T) {
	s := startServer(t)
	contract := uint32(0x5e55c001)
	owner, intruder := newClientID(contract), primaryClientID(contract)
	sessKey := nextSess()
	topic := "groups.session.owner"

	sub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: owner, autoKey: true, sessKey: sessKey})
	if err != nil {
		t.Fatal(err)
	}
	sub.holdNotify.Store(true) // keep the message in the session
	if sid, _ := sub.subscribe(1, topic); !sub.waitAck(sid, 3*time.Second) {
		t.Fatal("no subscribe ack")
	}
	pub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: owner, autoKey: true})
	if err != nil {
		t.Fatal(err)
	}
	pub.publish(1, topic, encodePayload(0, "for the owner"), "1m")
	isNotify := func(c *client) bool {
		deadline := time.After(time.Second)
		for {
			select {
			case m := <-c.ctrl:
				if m.FlowControl == utp.NOTIFY {
					return true
				}
			case <-deadline:
				return false
			}
		}
	}
	if !isNotify(sub) {
		t.Fatal("the subscriber was not notified")
	}
	sub.close()
	time.Sleep(200 * time.Millisecond)

	other, err := connectTo(t, s.tcpAddr, connectOpts{clientID: intruder, autoKey: true, sessKey: sessKey, resume: true})
	if err != nil {
		t.Fatal(err)
	}
	other.holdNotify.Store(true)
	if isNotify(other) {
		t.Fatal("another client of the contract resumed the session with its session key")
	}
	again, err := connectTo(t, s.tcpAddr, connectOpts{clientID: owner, autoKey: true, sessKey: sessKey, resume: true})
	if err != nil {
		t.Fatal(err)
	}
	if !isNotify(again) {
		t.Fatal("the owner did not resume its session")
	}
}
