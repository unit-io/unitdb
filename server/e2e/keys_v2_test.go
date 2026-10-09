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

// v2 client ids and topic keys: the only ones issued and taken, with key ids
// and expiries, from a keyring that rotates; in a cluster too, whatever the
// other nodes say they read. v1 ones are refused (release4_test.go).

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/config"
	"github.com/unit-io/unitdb/server/internal/keys"
	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/internal/v1test"
)

// keyedDelivers connects a subscriber and a publisher with cid to s, and
// reports whether a message published with key on topic reaches the
// subscriber, subscribed with key too: not if cid does not connect.
func keyedDelivers(t *testing.T, s *server, cid, key, topic string) bool {
	t.Helper()
	sub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: cid})
	if err != nil {
		return false
	}
	defer sub.close()
	pub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: cid})
	if err != nil {
		return false
	}
	defer pub.close()
	sid, _ := sub.subscribe(0, keyed(key, topic))
	sub.waitAck(sid, 3*time.Second)
	body := fmt.Sprintf("keyed-%d", time.Now().UnixNano())
	pub.publish(0, keyed(key, topic), encodePayload(0, body), "1m")
	deadline := time.Now().Add(2 * time.Second)
	for {
		msg, ok := sub.waitPub(time.Until(deadline))
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

// renewed returns the client id the server sent on unitdb/clientid/ within
// d, or "".
func renewed(c *client, d time.Duration) string {
	m, ok := c.waitTopic("unitdb/clientid/", d)
	if !ok {
		return ""
	}
	return string(m.Payload)
}

// keygenAnswer requests a read/write key for topic lasting ttl ("" for the
// server's default), and returns the key and the answer's status.
func keygenAnswer(t *testing.T, c *client, topic, ttl string) (string, int) {
	t.Helper()
	answer := request(t, c, "keygen", []types.KeyGenRequest{{Topic: topic, Type: "rw", Ttl: ttl}})
	var resp []types.KeyGenResponse
	if err := json.Unmarshal(answer, &resp); err == nil && len(resp) == 1 {
		return resp[0].Key, resp[0].Status
	}
	return "", answerStatus(t, answer)
}

// secondaryRequest asks the primary client c for a secondary client id.
func secondaryRequest(t *testing.T, c *client) string {
	t.Helper()
	var resp types.ClientIdResponse
	answer := request(t, c, "clientid", nil)
	if err := json.Unmarshal(answer, &resp); err != nil || resp.Status != 200 {
		t.Fatalf("client id request: %s (%v)", answer, err)
	}
	return resp.ClientId
}

// TestV2Issued checks that the server issues v2 client ids and topic keys,
// and does not renew a fresh one.
func TestV2Issued(t *testing.T) {
	s := startServer(t)

	// The id a client without one is assigned.
	c, err := dial(context.Background(), s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.close()
	if _, err := c.connectWith(connectOpts{sessKey: nextSess()}); err == nil {
		t.Fatal("a client without an id connected")
	}
	assigned := renewed(c, 3*time.Second)
	if len(assigned) != uid.EncodedLenV2 {
		t.Fatalf("assigned id %q, want a v2 one", assigned)
	}
	primary, err := connectTo(t, s.tcpAddr, connectOpts{clientID: assigned})
	if err != nil {
		t.Fatalf("the assigned id: %v", err)
	}
	contract, _ := contractOf(assigned)
	secondary := secondaryRequest(t, primary)
	id, claims, err := openClientID(secondary)
	if len(secondary) != uid.EncodedLenV2 || err != nil || claims.IssuedAt == 0 || id.Contract() != contract || id.Uuid() == 0 {
		t.Fatalf("secondary id %q: %x %+v %v", secondary, []byte(id), claims, err)
	}
	if other := secondaryRequest(t, primary); other == secondary {
		t.Fatal("two secondary ids are the same")
	}

	topic := "groups.v2.issued"
	key, status := keygenAnswer(t, primary, topic, "")
	if status != 200 || len(key) != security.KeyLenV2 {
		t.Fatalf("keygen: %q, status %d, want a v2 key", key, status)
	}
	if !keyedDelivers(t, s, secondary, key, topic) {
		t.Fatal("a v2 key does not open its topic")
	}
	if keyedDelivers(t, s, secondary, key, topic+".other") {
		t.Error("a v2 key opens another topic")
	}

	// A fresh v2 id is not renewed.
	fresh, err := connectTo(t, s.tcpAddr, connectOpts{clientID: secondary})
	if err != nil {
		t.Fatal(err)
	}
	if again := renewed(fresh, 500*time.Millisecond); again != "" {
		t.Errorf("a fresh v2 id was renewed: %q", again)
	}
	assertHealthy(t, s, "v2 ids and keys")
}

// TestV2Expiry checks that a v2 client id is renewed past 80% of its
// lifetime, with the same id, and is refused once expired, and that topic
// keys expire with their ttl.
func TestV2Expiry(t *testing.T) {
	s := startServerWith(t, serverOpts{extra: `"client_id_ttl": "10s", "topic_key_ttl": "1h",`})
	contract := uint32(0x2e1ea5e1)
	id := secondaryID(contract)
	now := uint32(time.Now().Unix())
	seal := func(issuedAt, expiresAt uint32) string {
		text, err := testKeys.SealClientIDAt(id, issuedAt, expiresAt)
		if err != nil {
			t.Fatal(err)
		}
		return text
	}

	c, err := connectTo(t, s.tcpAddr, connectOpts{clientID: seal(now, now+10)})
	if err != nil {
		t.Fatalf("a v2 id did not connect: %v", err)
	}
	if again := renewed(c, 500*time.Millisecond); again != "" {
		t.Errorf("a new v2 id was renewed at once: %q", again)
	}
	c2, err := connectTo(t, s.tcpAddr, connectOpts{clientID: seal(now-17, now+3)})
	if err != nil {
		t.Fatalf("a v2 id near the end of its lifetime did not connect: %v", err)
	}
	next := renewed(c2, 2*time.Second)
	got, claims, err := openClientID(next)
	if err != nil || !bytes.Equal(got, id) || claims.ExpiresAt < now+9 || claims.ExpiresAt > now+12 {
		t.Fatalf("renewed id %x %+v (%v), want the same id for client_id_ttl, 10s", []byte(got), claims, err)
	}
	topic := "groups.v2.expiry"
	if !keyedDelivers(t, s, next, topicKey(contract, topic, security.AllowReadWrite), topic) {
		t.Error("the renewed id does not key its contract's topics")
	}

	expired, err := dial(context.Background(), s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer expired.close()
	ack, err := expired.connectWith(connectOpts{clientID: seal(now-20, now-10), sessKey: nextSess()})
	if err == nil || ack == nil || ack.ReturnCode != types.ErrInvalidClientID.ReturnCode {
		t.Fatalf("an expired v2 id: %v, want return code %d", err, types.ErrInvalidClientID.ReturnCode)
	}
	if again := renewed(expired, 500*time.Millisecond); again != "" {
		t.Error("an expired id was sent a new one")
	}

	// Topic keys: topic_key_ttl, and a keygen request's own ttl.
	primary, err := connectTo(t, s.tcpAddr, connectOpts{clientID: primaryClientID(contract)})
	if err != nil {
		t.Fatal(err)
	}
	long, _ := keygenAnswer(t, primary, topic, "")
	if k, err := testKeys.DecodeTopicKeyV2(contract, long, topic); err != nil || k.ExpiresAt < now+3500 || k.ExpiresAt > now+3700 {
		t.Errorf("a key with topic_key_ttl 1h: %+v %v", k, err)
	}
	short, status := keygenAnswer(t, primary, topic, "2s")
	if status != 200 {
		t.Fatalf("keygen with a ttl: status %d", status)
	}
	if !keyedDelivers(t, s, next, short, topic) {
		t.Fatal("a key with a ttl does not open its topic")
	}
	time.Sleep(3 * time.Second)
	if keyedDelivers(t, s, next, short, topic) {
		t.Error("an expired key opens its topic")
	}
	if _, status := keygenAnswer(t, primary, topic, "soon"); status != types.ErrBadRequest.Status {
		t.Errorf("keygen with a ttl that isn't a duration: status %d", status)
	}
}

var mintidBuild struct {
	once sync.Once
	bin  string
	err  error
}

// mintid runs server/cmd/mintid with env and args, and returns the client id
// it printed.
func mintid(t *testing.T, env []string, args ...string) string {
	t.Helper()
	mintidBuild.once.Do(func() {
		dir, err := os.MkdirTemp("", "unitdb-e2e-mintid")
		if err != nil {
			mintidBuild.err = err
			return
		}
		bin := filepath.Join(dir, "mintid")
		cmd := exec.Command("go", "build", "-o", bin, "./cmd/mintid")
		cmd.Dir = serverSourceDir(t)
		if out, err := cmd.CombinedOutput(); err != nil {
			mintidBuild.err = fmt.Errorf("build mintid: %v\n%s", err, out)
			return
		}
		mintidBuild.bin = bin
	})
	if mintidBuild.err != nil {
		t.Fatal(mintidBuild.err)
	}
	cmd := exec.Command(mintidBuild.bin, args...)
	cmd.Env = append(os.Environ(), env...)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("mintid %v: %v", args, err)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if v, ok := strings.CutPrefix(line, "client id: "); ok {
			return v
		}
	}
	t.Fatalf("mintid printed no id:\n%s", out)
	return ""
}

// TestKeyRotation rotates a server's key: a new issue key, the old one kept
// to read with, and then removed. Ids and keys of the old key work until it
// is removed; the server issues with the new one, and sends a client of an
// old key's id the same id sealed with the new key. A v1 id of the old key,
// refused by the server, is sealed again by mintid -from while the key is in
// the keyring.
func TestKeyRotation(t *testing.T) {
	keyA := []byte("rotation-test-key-a-0123456789ab")
	keyB := []byte("rotation-test-key-b-0123456789ab")
	entry := func(id int, key []byte, use string) string {
		return fmt.Sprintf(`{"id": %d, "key": %q, "use": %q}`, id, base64.StdEncoding.EncodeToString(key), use)
	}
	ring := func(entries ...string) []string {
		return []string{config.KeyringEnv + "=[" + strings.Join(entries, ",") + "]"}
	}
	setOf := func(ks ...config.Key) *keys.Set {
		s, err := keys.New(&config.Keyring{Keys: ks})
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	ringA := ring(entry(0, keyA, "issue"))
	ringBA := ring(entry(1, keyB, "issue"), entry(0, keyA, "read"))
	ringB := ring(entry(1, keyB, "issue"))
	setA := setOf(config.Key{ID: 0, Key: keyA, Use: config.KeyIssue})
	setB := setOf(config.Key{ID: 1, Key: keyB, Use: config.KeyIssue})

	s := startServerWith(t, serverOpts{env: ringA})
	contract := uint32(0x2e1ea5e7)
	topic := "groups.rotation"
	idV2A, _ := setA.SealClientID(secondaryID(contract), 0)
	idV1A := v1test.ClientID(secondaryID(contract), keyA)
	keyV2A, _ := setA.TopicKey(contract, topic, security.AllowReadWrite, 0)
	// mintid reads the keyring as the server does.
	service := mintid(t, ringA, "-contract", fmt.Sprint(contract), "-service")
	if len(service) != uid.EncodedLenV2 {
		t.Fatalf("mintid printed %q, want a v2 id", service)
	}
	checkA := func(when string, want bool) {
		t.Helper()
		if got := keyedDelivers(t, s, idV2A, keyV2A, topic); got != want {
			t.Errorf("%s: the old key's id and key: delivered %t, want %t", when, got, want)
		}
		if keyedDelivers(t, s, idV1A, keyV2A, topic) {
			t.Errorf("%s: the old key's v1 id connected", when)
		}
	}
	checkA("before", true)
	sub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: service})
	if err != nil {
		t.Fatalf("mintid's service id: %v", err)
	}
	pub, err := connectTo(t, s.tcpAddr, connectOpts{clientID: service})
	if err != nil {
		t.Fatal(err)
	}
	if !deliveredUnkeyed(t, sub, pub, topic+".service") {
		t.Error("mintid's service id needs topic keys")
	}
	sub.close()
	pub.close()

	// Key B issues, key A reads.
	s.env = ringBA
	if err := s.restart(); err != nil {
		t.Fatal(err)
	}
	checkA("during", true)
	c, err := connectTo(t, s.tcpAddr, connectOpts{clientID: idV2A})
	if err != nil {
		t.Fatal(err)
	}
	moved := renewed(c, 3*time.Second)
	if _, claims, err := setB.OpenClientID([]byte(moved)); err != nil || claims.KeyID != 1 {
		t.Fatalf("an id of the old key was renewed as %q, %+v %v, want it sealed with key 1", moved, claims, err)
	}
	primary, err := connectTo(t, s.tcpAddr, connectOpts{clientID: mintid(t, ringBA, "-contract", fmt.Sprint(contract))})
	if err != nil {
		t.Fatal(err)
	}
	keyB1, _ := keygenAnswer(t, primary, topic, "")
	if k, err := setB.DecodeTopicKeyV2(contract, keyB1, topic); err != nil || k.KeyID != 1 {
		t.Fatalf("keygen during the rotation: %+v %v, want a key of key 1", k, err)
	}
	fromV1 := mintid(t, ringBA, "-from", idV1A)
	if got, claims, err := setB.OpenClientID([]byte(fromV1)); err != nil || claims.KeyID != 1 || got.Contract() != contract {
		t.Fatalf("mintid -from a v1 id of the old key: %+v %v", claims, err)
	}

	// Key A removed.
	s.env = ringB
	if err := s.restart(); err != nil {
		t.Fatal(err)
	}
	checkA("after", false)
	if _, err := connectTo(t, s.tcpAddr, connectOpts{clientID: idV2A}); err == nil {
		t.Error("an id of the removed key connected")
	}
	for name, id := range map[string]string{"renewed": moved, "sealed again by mintid": fromV1} {
		if !keyedDelivers(t, s, id, keyB1, topic) {
			t.Errorf("the id %s with the new key, with a key of the new key: not delivered", name)
		}
	}
}

func indexOf(list []string, s string) int {
	for i, v := range list {
		if v == s {
			return i
		}
	}
	return -1
}
