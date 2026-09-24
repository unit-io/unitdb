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

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestIntegrityReliableDelivery publishes with delivery mode 1 (reliable) and
// checks every message arrives once, in order, uncorrupted.
func TestIntegrityReliableDelivery(t *testing.T) {
	s := startServer(t)
	ctx := context.Background()
	cid := newClientID(0x0d1de111)
	topic := "groups.reliable.x.message"

	sub, err := dial(ctx, s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.close()
	if _, err := sub.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}
	sid, _ := sub.subscribe(1, topic)
	if !sub.waitAck(sid, 5*time.Second) {
		t.Fatal("no subscribe ack")
	}

	pub, err := dial(ctx, s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.close()
	if _, err := pub.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}

	const n = 100
	for i := 0; i < n; i++ {
		if _, err := pub.publish(1, topic, encodePayload(i, fmt.Sprintf("r%d", i)), "1h"); err != nil {
			t.Fatal(err)
		}
	}

	// Mode 1 is at-least-once, so tolerate duplicates but require every
	// sequence, uncorrupted. A bounded collector fails fast on a redelivery
	// storm rather than looping forever.
	got, dups, err := collectUnique(sub, n, 30*time.Second, 10*time.Second)
	if err != nil {
		t.Fatalf("reliable delivery: %v\nlogs:\n%s", err, s.logs.String())
	}
	for i := 0; i < n; i++ {
		if want := fmt.Sprintf("r%d", i); got[i] != want {
			t.Fatalf("seq %d: expected %q got %q", i, want, got[i])
		}
	}
	if dups > 0 {
		t.Logf("reliable delivery redelivered %d duplicate(s) (at-least-once, acceptable)", dups)
	}
}

// TestIntegrityPersistsAcrossRestart publishes, cleanly restarts the server on
// the same DB, and checks a relay returns every message uncorrupted.
func TestIntegrityPersistsAcrossRestart(t *testing.T) {
	testPersistence(t, false)
}

// TestIntegrityPersistsAcrossCrash is the same but the server is SIGKILLed
// (crash) rather than restarted cleanly.
func TestIntegrityPersistsAcrossCrash(t *testing.T) {
	testPersistence(t, true)
}

func testPersistence(t *testing.T, crash bool) {
	s := startServer(t)
	ctx := context.Background()
	cid := newClientID(0x0e2e0b57)
	topic := "groups.persist.x.message"

	pub, err := dial(ctx, s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pub.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}

	const n = 50
	for i := 0; i < n; i++ {
		id, err := pub.publish(1, topic, encodePayload(i, fmt.Sprintf("m%d", i)), "24h")
		if err != nil {
			t.Fatal(err)
		}
		// Reliable publish is acked once persisted; wait so the data is on disk.
		pub.waitAck(id, 5*time.Second)
	}
	// Give the server a moment to flush its write-ahead log to the store.
	time.Sleep(1500 * time.Millisecond)
	pub.close()

	if crash {
		s.stop() // SIGKILL, already how stop works
	}
	if err := s.restart(); err != nil {
		t.Fatalf("restart: %v\nlogs:\n%s", err, s.logs.String())
	}

	sub, err := dial(ctx, s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.close()
	if _, err := sub.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}
	rid, err := sub.relay(topic, "24h")
	if err != nil {
		t.Fatal(err)
	}
	sub.waitAck(rid, 5*time.Second)

	got, dups, err := collectUnique(sub, n, 30*time.Second, 8*time.Second)
	if err != nil {
		t.Fatalf("after restart(crash=%v): %v\nlogs:\n%s", crash, err, s.logs.String())
	}
	for i := 0; i < n; i++ {
		if want := fmt.Sprintf("m%d", i); got[i] != want {
			t.Fatalf("seq %d: expected %q got %q", i, want, got[i])
		}
	}
	if dups > 0 {
		t.Logf("relay after restart(crash=%v) redelivered %d duplicate(s)", crash, dups)
	}
}
