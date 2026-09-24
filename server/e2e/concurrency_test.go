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
	"sync"
	"testing"
	"time"
)

// TestConcurrentManyClients runs many publisher/subscriber pairs at once, each
// pair on its own topic and all on one contract, and checks every subscriber
// receives exactly its own publisher's messages, uncorrupted and unduplicated.
func TestConcurrentManyClients(t *testing.T) {
	s := startServer(t)
	ctx := context.Background()
	contract := uint32(0x0c04ce77)
	cid := newClientID(contract)

	const pairs = 12
	const perPair = 40

	var wg sync.WaitGroup
	errs := make(chan error, pairs)
	for p := 0; p < pairs; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			topic := fmt.Sprintf("groups.c.%d.message", p)

			sub, err := dial(ctx, s.tcpAddr)
			if err != nil {
				errs <- err
				return
			}
			defer sub.close()
			if _, err := sub.connect(cid, true, nextSess()); err != nil {
				errs <- fmt.Errorf("pair %d sub connect: %w", p, err)
				return
			}
			sid, _ := sub.subscribe(0, topic)
			if !sub.waitAck(sid, 5*time.Second) {
				errs <- fmt.Errorf("pair %d no subscribe ack", p)
				return
			}

			// Collect deliveries in the background.
			got := make(map[int][]byte)
			done := make(chan struct{})
			go func() {
				defer close(done)
				for len(got) < perPair {
					msg, ok := sub.waitPub(5 * time.Second)
					if !ok {
						return
					}
					for _, m := range msg.Messages {
						seq, payload, ok := decodePayload(m.Payload)
						if !ok {
							errs <- fmt.Errorf("pair %d corrupt payload", p)
							return
						}
						got[seq] = payload
					}
				}
			}()

			pub, err := dial(ctx, s.tcpAddr)
			if err != nil {
				errs <- err
				return
			}
			defer pub.close()
			if _, err := pub.connect(cid, true, nextSess()); err != nil {
				errs <- fmt.Errorf("pair %d pub connect: %w", p, err)
				return
			}
			for i := 0; i < perPair; i++ {
				if _, err := pub.publish(0, topic, encodePayload(i, fmt.Sprintf("p%d-%d", p, i)), "1m"); err != nil {
					errs <- fmt.Errorf("pair %d publish %d: %w", p, i, err)
					return
				}
			}

			select {
			case <-done:
			case <-time.After(10 * time.Second):
			}
			if len(got) != perPair {
				errs <- fmt.Errorf("pair %d: expected %d messages, got %d", p, perPair, len(got))
				return
			}
			for i := 0; i < perPair; i++ {
				if want := fmt.Sprintf("p%d-%d", p, i); string(got[i]) != want {
					errs <- fmt.Errorf("pair %d seq %d: expected %q got %q", p, i, want, got[i])
					return
				}
			}
		}(p)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if !s.alive() {
		t.Fatalf("server died under concurrent load\nlogs:\n%s", s.logs.String())
	}
}

// TestConcurrentFanout has many subscribers on one topic and one publisher, and
// checks every subscriber receives every message exactly once.
func TestConcurrentFanout(t *testing.T) {
	s := startServer(t)
	ctx := context.Background()
	contract := uint32(0x0fa0a007)
	cid := newClientID(contract)
	topic := "groups.fanout.all.message"

	const subs = 10
	const msgs = 30

	subClients := make([]*client, subs)
	for i := range subClients {
		c, err := dial(ctx, s.tcpAddr)
		if err != nil {
			t.Fatal(err)
		}
		defer c.close()
		if _, err := c.connect(cid, true, nextSess()); err != nil {
			t.Fatalf("sub %d connect: %v", i, err)
		}
		sid, _ := c.subscribe(0, topic)
		if !c.waitAck(sid, 5*time.Second) {
			t.Fatalf("sub %d no ack", i)
		}
		subClients[i] = c
	}

	pub, err := dial(ctx, s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer pub.close()
	if _, err := pub.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < msgs; i++ {
		if _, err := pub.publish(0, topic, encodePayload(i, "x"), "1m"); err != nil {
			t.Fatal(err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, subs)
	for i, c := range subClients {
		wg.Add(1)
		go func(i int, c *client) {
			defer wg.Done()
			seen := make(map[int]bool)
			for len(seen) < msgs {
				msg, ok := c.waitPub(10 * time.Second)
				if !ok {
					errs <- fmt.Errorf("sub %d: got %d of %d messages", i, len(seen), msgs)
					return
				}
				for _, m := range msg.Messages {
					seq, _, ok := decodePayload(m.Payload)
					if !ok {
						errs <- fmt.Errorf("sub %d corrupt payload", i)
						return
					}
					if seen[seq] {
						errs <- fmt.Errorf("sub %d duplicate seq %d", i, seq)
						return
					}
					seen[seq] = true
				}
			}
		}(i, c)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if !s.alive() {
		t.Fatalf("server died\nlogs:\n%s", s.logs.String())
	}
}
