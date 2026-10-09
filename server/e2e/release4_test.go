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

// Release 4 of the security design: v1 client ids and topic keys, and
// unsigned keys, are refused, and accept_unsigned_keys is gone. mintid -from
// is the only place a v1 id is still read, to seal it again as v2.

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/internal/v1test"
)

// refusedAtConnect connects with clientID and returns the CONNECT return
// code, and whether the server sent a new client id.
func refusedAtConnect(t *testing.T, s *server, clientID string) (uint8, bool) {
	t.Helper()
	c, err := dial(context.Background(), s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	ack, err := c.connectWith(connectOpts{clientID: clientID, sessKey: nextSess()})
	if err == nil || ack == nil {
		t.Fatalf("connect with %q: accepted (%v)", clientID, err)
	}
	return ack.ReturnCode, renewed(c, 500*time.Millisecond) != ""
}

// keyRefusal publishes and subscribes on topic with key from a client of
// cid, and returns the error notices' messages: none if the key is taken.
func keyRefusal(t *testing.T, s *server, cid, key, topic string) []string {
	t.Helper()
	c, err := connectTo(t, s.tcpAddr, connectOpts{clientID: cid})
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	var refusals []string
	for _, send := range []func(){
		func() { c.subscribe(0, keyed(key, topic)) },
		func() { c.publish(0, keyed(key, topic), encodePayload(0, "refused"), "1m") },
	} {
		send()
		if m, ok := c.waitTopic("unitdb/error/", time.Second); ok {
			refusals = append(refusals, string(m.Payload))
		}
	}
	return refusals
}

// TestReleaseFourV1Refused checks that v1 client ids are refused at CONNECT
// with return code 2 and no new id, and when a service's v1 id vouches for a
// connection; and that v1 signed keys and unsigned keys are refused, saying
// why.
func TestReleaseFourV1Refused(t *testing.T) {
	s := startServer(t)
	contract := uint32(0x4e1ea5e1)
	topic := "groups.v1.refused"

	for name, id := range map[string]string{
		"a v1 id":           v1ClientID(contract),
		"a v1 primary id":   v1test.ClientID(mustMint(t, contract, false)[:12], []byte(testKey)),
		"a v1 service's id": v1test.ClientID(mustMint(t, contract, true)[:12], []byte(testKey)),
	} {
		code, sent := refusedAtConnect(t, s, id)
		if code != types.ErrInvalidClientID.ReturnCode {
			t.Errorf("%s: return code %d, want %d", name, code, types.ErrInvalidClientID.ReturnCode)
		}
		if sent {
			t.Errorf("%s was sent a new id", name)
		}
	}
	// Garbage of another length is still sent a new id.
	if code, sent := refusedAtConnect(t, s, "not-a-client-id"); code != types.ErrInvalidClientID.ReturnCode || !sent {
		t.Errorf("an invalid id: return code %d, sent a new id %t", code, sent)
	}

	cid := newClientID(contract)
	if !keyedDelivers(t, s, cid, topicKey(contract, topic, security.AllowReadWrite), topic) {
		t.Fatal("control: a v2 id with a v2 key is not delivered to")
	}
	for name, key := range map[string]string{
		"v1 signed key":  v1SignedTopicKey(contract, topic, security.AllowReadWrite),
		"unsigned key":   unsignedTopicKey(contract, topic, security.AllowReadWrite),
		"v1 key for ...": v1SignedTopicKey(contract, "...", security.AllowReadWrite),
	} {
		if keyedDelivers(t, s, cid, key, topic) {
			t.Errorf("a %s opened its topic", name)
		}
		refusals := keyRefusal(t, s, cid, key, topic)
		if len(refusals) != 2 {
			t.Errorf("a %s: %d refusals of a subscribe and a publish", name, len(refusals))
		}
		for _, r := range refusals {
			if !strings.Contains(r, "no longer accepted") || !strings.Contains(r, fmt.Sprint(types.ErrV1Key.Status)) {
				t.Errorf("a %s was refused with %s, which does not say why", name, r)
			}
		}
	}

	// unitdb/service: a service's v1 id vouches for nothing; its v2 id does.
	c, err := connectTo(t, s.tcpAddr, connectOpts{clientID: cid})
	if err != nil {
		t.Fatal(err)
	}
	v1Service := v1test.ClientID(mustMint(t, contract, true)[:12], []byte(testKey))
	if status := vouch(t, c, v1Service); status != types.ErrForbidden.Status {
		t.Errorf("a service's v1 id vouched: status %d", status)
	}
	if status := vouch(t, c, serviceClientID(contract)); status != 200 {
		t.Errorf("control: a service's v2 id did not vouch: status %d", status)
	}
	assertHealthy(t, s, "v1 ids and keys refused")
}

// mustMint returns a primary id of contract, a service's if service.
func mustMint(t *testing.T, contract uint32, service bool) uid.ID {
	t.Helper()
	id, err := uid.MintClientID(contract, service)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// TestReleaseFourMintID checks that mintid seals a v1 id again as a v2 one,
// the same id, which connects, keeps its sessions' owner and, for a primary
// id, generates keys; and that it mints no v1 ids any more.
func TestReleaseFourMintID(t *testing.T) {
	s := startServer(t)
	env := []string{"UNITDB_ENCRYPTION_KEY=" + testKey}
	contract := uint32(0x4e1ea5e2)

	// A secondary id, and a primary one.
	for name, id := range map[string]uid.ID{"secondary": secondaryID(contract)[:12], "primary": mustMint(t, contract, false)[:12]} {
		old := v1test.ClientID(id, []byte(testKey))
		again := mintid(t, env, "-from", old)
		got, claims, err := openClientID(again)
		if err != nil || !bytes.Equal(got, id) || claims.KeyID != 0 || got.Uuid() != 0 {
			t.Fatalf("%s: the v1 id sealed again opens to %x %+v (%v), want %x", name, []byte(got), claims, err, []byte(id))
		}
		c, err := connectTo(t, s.tcpAddr, connectOpts{clientID: again})
		if err != nil {
			t.Fatalf("%s: the v1 id sealed again was refused: %v", name, err)
		}
		if name == "primary" {
			if key, status := keygenAnswer(t, c, "groups.minted", ""); status != 200 || len(key) != security.KeyLenV2 {
				t.Errorf("the primary id sealed again from v1 can't generate keys: %q, status %d", key, status)
			}
		}
		c.close()
		if code, _ := refusedAtConnect(t, s, old); code != types.ErrInvalidClientID.ReturnCode {
			t.Errorf("%s: the v1 id itself: return code %d", name, code)
		}
	}

	// A service's v1 id stays a service's.
	service := mintid(t, env, "-from", v1test.ClientID(mustMint(t, contract, true)[:12], []byte(testKey)))
	sub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: service})
	if err != nil {
		t.Fatal(err)
	}
	pub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: service})
	if err != nil {
		t.Fatal(err)
	}
	if !deliveredUnkeyed(t, sub, pub, "groups.minted.service") {
		t.Error("a service's id sealed again from v1 needs topic keys")
	}

	// -v1 is gone.
	cmd := exec.Command(mintidBuild.bin, "-v1")
	cmd.Env = append(os.Environ(), env...)
	if out, err := cmd.CombinedOutput(); err == nil {
		t.Errorf("mintid -v1 minted:\n%s", out)
	}
}

// TestReleaseFourAcceptUnsignedKeys checks that a config that still sets
// accept_unsigned_keys to true stops the server, saying why, and that false
// is ignored with a warning: unsigned keys are refused either way.
func TestReleaseFourAcceptUnsignedKeys(t *testing.T) {
	s := startServerWith(t, serverOpts{extra: `"accept_unsigned_keys": true,`, expectExit: true})
	select {
	case <-s.exited:
	case <-time.After(10 * time.Second):
		t.Fatal("the server started with accept_unsigned_keys")
	}
	if logs := s.logs.String(); !strings.Contains(logs, "accept_unsigned_keys is set, but unsigned topic keys are refused") {
		t.Errorf("the server exited without saying why:\n%s", logs)
	}

	off := startServerWith(t, serverOpts{extra: `"accept_unsigned_keys": false,`, logLevel: "Warn"})
	if logs := off.logs.String(); !strings.Contains(logs, "accept_unsigned_keys is no longer read") {
		t.Errorf("no warning for accept_unsigned_keys false:\n%s", logs)
	}
	contract := uint32(0x4e1ea5e3)
	topic := "groups.unsigned.off"
	if keyedDelivers(t, off, newClientID(contract), unsignedTopicKey(contract, topic, security.AllowReadWrite), topic) {
		t.Error("an unsigned key opened its topic")
	}
}

// publishKeyed publishes body on topic with key from a client of cid on s,
// kept for an hour, and waits for it to be acknowledged.
func publishKeyed(t *testing.T, s *server, cid, key, topic, body string) {
	t.Helper()
	p, err := connectTo(t, s.tcpAddr, connectOpts{clientID: cid})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer p.close()
	if id, _ := p.publish(0, keyed(key, topic), encodePayload(0, body), "1h"); !p.waitAck(id, 3*time.Second) {
		t.Fatalf("no publish ack for %s", topic)
	}
}

// relayKeyedFinds relays topic with key from a client of cid on s, and
// reports whether a message with body comes back.
func relayKeyedFinds(t *testing.T, s *server, cid, key, topic, body string) bool {
	t.Helper()
	r, err := connectTo(t, s.tcpAddr, connectOpts{clientID: cid})
	if err != nil {
		return false
	}
	defer r.close()
	r.relay(keyed(key, topic), "1h")
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

// TestReleaseFourUpgradeFromV060 upgrades a v0.6.0 server whose clients
// used v1 ids, v1 signed keys and unsigned keys (accept_unsigned_keys), as
// docs/rolling-deploys.md says: on v0.6.0 the clients are renewed to v2 ids
// (the renewal push) and get v2 keys from keygen; then v0.7.0 refuses to
// start while the config sets accept_unsigned_keys, and once it is removed
// opens the v0.6.0 store, moves it to the $sys layout, and serves what
// v0.6.0 stored, under v2 ids and keys only.
func TestReleaseFourUpgradeFromV060(t *testing.T) {
	old := oldServerBinary(t, previousRelease)
	s := startServerWith(t, serverOpts{bin: old, logLevel: "Info", extra: `"accept_unsigned_keys": true,`})
	contract := uint32(0x4e1ea5e4)
	v1 := v1ClientID(contract)
	signedTopic, unsignedTopic := "groups.upgrade.v1signed", "groups.upgrade.unsigned"

	// v1-era data: stored with a v1 id, a v1 signed key and an unsigned one.
	publishKeyed(t, s, v1, v1SignedTopicKey(contract, signedTopic, security.AllowReadWrite), signedTopic, "stored with a v1 signed key")
	publishKeyed(t, s, v1, unsignedTopicKey(contract, unsignedTopic, security.AllowReadWrite), unsignedTopic, "stored with an unsigned key")
	// The renewal push: v0.6.0 sends a client of a v1 id the same id as v2.
	c, err := connectTo(t, s.tcpAddr, connectOpts{clientID: v1})
	if err != nil {
		t.Fatal(err)
	}
	pushed := renewed(c, 3*time.Second)
	c.close()
	if len(pushed) != uid.EncodedLenV2 {
		t.Fatalf("v0.6.0 renewed the v1 id as %q, want a v2 id", pushed)
	}
	// A client id revoked on v0.6.0.
	admin, err := connectTo(t, s.tcpAddr, connectOpts{clientID: primaryClientID(contract)})
	if err != nil {
		t.Fatal(err)
	}
	victim, victimUuid := secondaryUuid(t, admin)
	revoke(t, admin, types.RevokeRequest{Uuid: victimUuid})
	admin.close()

	// v0.7.0 on the same store and config: refused while the config sets
	// accept_unsigned_keys.
	s.shutdown()
	s.switchBinary(serverBinary(t))
	s.noWait = true
	if err := s.start(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.exited:
	case <-time.After(10 * time.Second):
		t.Fatal("v0.7.0 started with accept_unsigned_keys set")
	}
	if !strings.Contains(s.logs.String(), "accept_unsigned_keys is set") {
		t.Errorf("v0.7.0 exited without saying why:\n%s", s.logs.String())
	}
	conf, err := os.ReadFile(s.confPath)
	if err != nil {
		t.Fatal(err)
	}
	conf = bytes.Replace(conf, []byte(`"accept_unsigned_keys": true,`), nil, 1)
	if err := os.WriteFile(s.confPath, conf, 0644); err != nil {
		t.Fatal(err)
	}
	s.noWait = false
	if err := s.start(); err != nil {
		t.Fatalf("v0.7.0 on the v0.6.0 store: %v\n%s", err, s.logs.String())
	}
	if !strings.Contains(s.logs.String(), "moved the topic index and replicas of an older version") {
		t.Errorf("v0.7.0 did not move the v0.6.0 store:\n%s", s.logs.String())
	}

	// The v1 id and keys are refused; the id pushed to the client, and v2
	// keys, serve what v0.6.0 stored.
	if code, sent := refusedAtConnect(t, s, v1); code != types.ErrInvalidClientID.ReturnCode || sent {
		t.Errorf("the v1 id after the upgrade: return code %d, sent a new id %t", code, sent)
	}
	for topic, body := range map[string]string{signedTopic: "stored with a v1 signed key", unsignedTopic: "stored with an unsigned key"} {
		if !relayKeyedFinds(t, s, pushed, topicKey(contract, topic, security.AllowReadWrite), topic, body) {
			t.Errorf("relay %s with the pushed id and a v2 key: %q is not found", topic, body)
		}
	}
	if relayKeyedFinds(t, s, pushed, v1SignedTopicKey(contract, signedTopic, security.AllowReadWrite), signedTopic, "stored with a v1 signed key") {
		t.Error("a v1 signed key relays after the upgrade")
	}
	if relayKeyedFinds(t, s, pushed, unsignedTopicKey(contract, unsignedTopic, security.AllowReadWrite), unsignedTopic, "stored with an unsigned key") {
		t.Error("an unsigned key relays after the upgrade")
	}
	if connects(t, s, victim) {
		t.Error("the id revoked on v0.6.0 connects after the upgrade")
	}
	// mintid -from seals the v1 id as v0.6.0's push did: the same id.
	fromV1 := mintid(t, []string{"UNITDB_ENCRYPTION_KEY=" + testKey}, "-from", v1)
	a, _, errA := openClientID(fromV1)
	b, _, errB := openClientID(pushed)
	if errA != nil || errB != nil || !bytes.Equal(a, b) {
		t.Errorf("mintid -from %x (%v), the push %x (%v): want the same id", []byte(a), errA, []byte(b), errB)
	}
	assertHealthy(t, s, "after the upgrade from v0.6.0")
}
