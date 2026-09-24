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

// Security tests are defensive: the server must reject requests it should not
// allow, keep tenants (contracts) apart, and survive malformed input without
// crashing or hanging. Credentials are minted only through the same APIs the
// server's own keygen uses, with the test deployment's key.

import (
	"context"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/utp"
)

var healthContract uint32 = 0x4ea17000

// assertHealthy checks the server is alive and still serves a full pub/sub
// round trip on a fresh connection and contract.
func assertHealthy(t *testing.T, s *server, after string) {
	t.Helper()
	if !s.alive() {
		t.Fatalf("server crashed after %s\nlogs:\n%s", after, s.logs.String())
	}
	ctx := context.Background()
	cid := newClientID(atomic.AddUint32(&healthContract, 1))
	topic := "groups.health.x.message"
	sub, err := dial(ctx, s.tcpAddr)
	if err != nil {
		t.Fatalf("after %s: cannot connect: %v", after, err)
	}
	defer sub.close()
	if _, err := sub.connect(cid, true, nextSess()); err != nil {
		t.Fatalf("after %s: connect: %v\nlogs:\n%s", after, err, s.logs.String())
	}
	sid, _ := sub.subscribe(0, topic)
	if !sub.waitAck(sid, 5*time.Second) {
		t.Fatalf("after %s: no subscribe ack", after)
	}
	pub, err := dial(ctx, s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.close()
	if _, err := pub.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}
	pub.publish(0, topic, encodePayload(0, "ok"), "1m")
	if _, ok := sub.waitPub(5 * time.Second); !ok {
		t.Fatalf("after %s: server no longer delivers messages\nlogs:\n%s", after, s.logs.String())
	}
}

// TestSecurityMalformedInput sends malformed and hostile frames, each to its
// own server instance, and checks the server stays up and keeps serving.
func TestSecurityMalformedInput(t *testing.T) {
	ctx := context.Background()
	rnd := rand.New(rand.NewSource(1))
	garbage := make([]byte, 4096)
	rnd.Read(garbage)

	cases := []struct {
		name string
		send func(c *client) error
	}{
		{"random bytes", func(c *client) error { return c.writeRaw(garbage) }},
		{"truncated frame", func(c *client) error { return c.writeRaw([]byte{0x05, 0x08}) }},
		{"zero message type", func(c *client) error { return c.writeFrame(0, utp.NONE, []byte{1, 2, 3}) }},
		{"unknown message type", func(c *client) error { return c.writeFrame(99, utp.NONE, []byte{1, 2, 3}) }},
		{"body shorter than declared", func(c *client) error {
			// Header claims 1000 bytes, only 3 follow, then the client hangs up.
			return c.writeRaw(append(frameHeader(utp.PUBLISH, utp.NONE, 1000), 1, 2, 3))
		}},
		{"huge declared length", func(c *client) error {
			// Claims 64MB and sends nothing; the server must not wedge or die.
			return c.writeRaw(frameHeader(utp.PUBLISH, utp.NONE, 64<<20))
		}},
		{"negative declared length", func(c *client) error {
			return c.writeRaw(frameHeader(utp.PUBLISH, utp.NONE, -1))
		}},
		{"garbage protobuf body", func(c *client) error { return c.writeFrame(utp.CONNECT, utp.NONE, garbage[:512]) }},
		{"publish before connect", func(c *client) error {
			_, err := c.publish(0, "groups.x.y", []byte("x"), "")
			return err
		}},
		{"subscribe before connect", func(c *client) error {
			_, err := c.subscribe(0, "groups.x.y")
			return err
		}},
		{"relay before connect", func(c *client) error {
			_, err := c.relay("groups.x.y", "1h")
			return err
		}},
		{"batch publish before connect", func(c *client) error {
			_, err := c.publish(2, "groups.x.y", []byte("x"), "")
			return err
		}},
		{"empty topic after connect", func(c *client) error {
			if _, err := c.connect(newClientID(0x5ec00001), true, nextSess()); err != nil {
				return err
			}
			_, err := c.subscribe(0, "")
			return err
		}},
		{"key separator only topic", func(c *client) error {
			if _, err := c.connect(newClientID(0x5ec00002), true, nextSess()); err != nil {
				return err
			}
			_, err := c.subscribe(0, "/")
			return err
		}},
		{"unknown RECEIVE id", func(c *client) error {
			if _, err := c.connect(newClientID(0x5ec00003), true, nextSess()); err != nil {
				return err
			}
			return c.sendControl(4242, utp.PUBLISH, utp.RECEIVE, nil)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Each case gets its own server, so one crash can't hide the rest.
			s := startServer(t)
			c, err := dial(ctx, s.tcpAddr)
			if err != nil {
				t.Fatalf("server not accepting connections: %v\nlogs:\n%s", err, s.logs.String())
			}
			tc.send(c) // write errors are fine: the server may drop us
			time.Sleep(300 * time.Millisecond)
			c.close()
			assertHealthy(t, s, tc.name)
		})
	}
}

// frameHeader builds just a frame header claiming length bytes of body.
func frameHeader(mt utp.MessageType, fc utp.FlowControl, length int32) []byte {
	h := headerBytes(mt, fc, length)
	return append(encodeVarint(len(h)), h...)
}

// TestSecurityContractIsolation checks a subscriber on one contract never sees
// messages published on the same topic under another contract.
func TestSecurityContractIsolation(t *testing.T) {
	s := startServer(t)
	ctx := context.Background()
	topic := "groups.shared.name.message"

	other, err := dial(ctx, s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer other.close()
	if _, err := other.connect(newClientID(0x0a0a0a0a), true, nextSess()); err != nil {
		t.Fatal(err)
	}
	sid, _ := other.subscribe(0, topic)
	if !other.waitAck(sid, 5*time.Second) {
		t.Fatal("no subscribe ack")
	}

	pub, err := dial(ctx, s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.close()
	if _, err := pub.connect(newClientID(0x0b0b0b0b), true, nextSess()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		pub.publish(0, topic, encodePayload(i, "secret"), "1m")
	}
	if msg, ok := other.waitPub(2 * time.Second); ok {
		t.Fatalf("contract isolation broken: other contract received %q", msg.Messages[0].Payload)
	}

	// Relay across contracts must not leak stored messages either.
	rid, _ := other.relay(topic, "1h")
	other.waitAck(rid, 5*time.Second)
	if msg, ok := other.waitPub(2 * time.Second); ok {
		t.Fatalf("contract isolation broken via relay: got %q", msg.Messages[0].Payload)
	}
}

// TestSecurityKeyEnforcement connects in secure mode (keys required) and checks
// that topics need a valid key with the right permission.
func TestSecurityKeyEnforcement(t *testing.T) {
	s := startServer(t)
	ctx := context.Background()
	contract := uint32(0x6e7c0de0)
	cid := newClientID(contract)
	topic := "groups.keyed.x.message"

	readKey := topicKey(contract, topic, security.AllowRead)
	writeKey := topicKey(contract, topic, security.AllowWrite)
	otherContractKey := topicKey(contract+1, topic, security.AllowReadWrite)

	// deliveredTo subscribes with subTopic in secure mode, publishes with
	// pubTopic in secure mode, and reports whether the message arrived.
	deliveredTo := func(subTopic, pubTopic string) bool {
		sub, err := dial(ctx, s.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.close()
		if _, err := sub.connect(cid, false, nextSess()); err != nil {
			t.Fatalf("secure connect: %v", err)
		}
		sub.subscribe(0, subTopic)
		time.Sleep(200 * time.Millisecond)

		pub, err := dial(ctx, s.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer pub.close()
		if _, err := pub.connect(cid, false, nextSess()); err != nil {
			t.Fatalf("secure connect: %v", err)
		}
		pub.publish(0, pubTopic, encodePayload(0, "k"), "1m")
		// Only the published payload on the target topic counts as delivery.
		// The server reports refused requests as publishes on "unitdb/error/",
		// which must not be mistaken for acceptance.
		deadline := time.Now().Add(1500 * time.Millisecond)
		for {
			msg, ok := sub.waitPub(time.Until(deadline))
			if !ok {
				return false
			}
			for _, m := range msg.Messages {
				if m.Topic != topic && !strings.HasSuffix(m.Topic, "/"+topic) {
					continue
				}
				if _, body, ok := decodePayload(m.Payload); ok && string(body) == "k" {
					return true
				}
			}
		}
	}

	rwKey := topicKey(contract, topic, security.AllowReadWrite)
	if !deliveredTo(keyed(rwKey, topic), keyed(rwKey, topic)) {
		t.Fatalf("control: a valid read/write key should deliver\nlogs:\n%s", s.logs.String())
	}

	t.Run("subscribe without key", func(t *testing.T) {
		if deliveredTo(topic, keyed(rwKey, topic)) {
			t.Error("subscribe without a key received a message in secure mode")
		}
	})
	t.Run("publish without key", func(t *testing.T) {
		if deliveredTo(keyed(rwKey, topic), topic) {
			t.Error("publish without a key was delivered in secure mode")
		}
	})
	t.Run("subscribe with write-only key", func(t *testing.T) {
		if deliveredTo(keyed(writeKey, topic), keyed(rwKey, topic)) {
			t.Error("a write-only key was allowed to subscribe")
		}
	})
	t.Run("publish with read-only key", func(t *testing.T) {
		if deliveredTo(keyed(rwKey, topic), keyed(readKey, topic)) {
			t.Error("a read-only key was allowed to publish")
		}
	})
	t.Run("key for another contract", func(t *testing.T) {
		if deliveredTo(keyed(otherContractKey, topic), keyed(rwKey, topic)) {
			t.Error("a key minted for another contract was accepted")
		}
	})
	t.Run("key for another topic", func(t *testing.T) {
		wrong := topicKey(contract, "groups.other.x.message", security.AllowReadWrite)
		if deliveredTo(keyed(wrong, topic), keyed(rwKey, topic)) {
			t.Error("a key minted for another topic was accepted")
		}
	})
	t.Run("corrupted key", func(t *testing.T) {
		bad := []byte(rwKey)
		bad[len(bad)/2] ^= 0x01
		if deliveredTo(keyed(string(bad), topic), keyed(rwKey, topic)) {
			t.Error("a corrupted key was accepted")
		}
	})
	assertHealthy(t, s, "key enforcement")
}

// TestSecurityConnectionFlood opens many connections that never CONNECT and
// checks the server still serves a real client.
func TestSecurityConnectionFlood(t *testing.T) {
	s := startServer(t)
	ctx := context.Background()
	var idle []*client
	for i := 0; i < 200; i++ {
		c, err := dial(ctx, s.tcpAddr)
		if err != nil {
			break
		}
		idle = append(idle, c)
	}
	defer func() {
		for _, c := range idle {
			c.close()
		}
	}()
	t.Logf("opened %d idle connections", len(idle))
	assertHealthy(t, s, "a flood of idle connections")
}
