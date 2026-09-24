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

// Performance tests measure end-to-end throughput and delivery latency through
// a real server process and log the results. They also check every message
// arrived, so a fast-but-lossy server fails. Numbers under -race are much
// slower and only useful for comparing runs with each other. Skipped with
// -short.

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"
)

type latencies []time.Duration

func (l latencies) pct(p float64) time.Duration {
	if len(l) == 0 {
		return 0
	}
	s := append(latencies(nil), l...)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(float64(len(s)-1)*p)]
}

// stamp encodes the send time so the receiver can compute delivery latency.
func stamp(seq int) []byte {
	return encodePayload(seq, strconv.FormatInt(time.Now().UnixNano(), 10))
}

// receiveAll collects n distinct sequences from c, returning per-message
// latency. It fails on corruption, duplicates beyond at-least-once, or stall.
func receiveAll(t *testing.T, c *client, n int, overall time.Duration) latencies {
	t.Helper()
	seen := make(map[int]bool, n)
	lat := make(latencies, 0, n)
	deadline := time.Now().Add(overall)
	for len(seen) < n {
		msg, ok := c.waitPub(time.Until(deadline))
		if !ok {
			t.Fatalf("received %d of %d messages before deadline", len(seen), n)
		}
		now := time.Now()
		for _, m := range msg.Messages {
			seq, body, ok := decodePayload(m.Payload)
			if !ok {
				t.Fatalf("corrupt payload")
			}
			if seen[seq] {
				continue
			}
			seen[seq] = true
			if sent, err := strconv.ParseInt(string(body), 10, 64); err == nil {
				lat = append(lat, now.Sub(time.Unix(0, sent)))
			}
		}
	}
	return lat
}

func perfPair(t *testing.T, s *server, contract uint32, topic string, mode uint8) (pub, sub *client) {
	t.Helper()
	ctx := context.Background()
	cid := newClientID(contract)
	var err error
	if sub, err = dial(ctx, s.tcpAddr); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(sub.close)
	if _, err := sub.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}
	sid, _ := sub.subscribe(mode, topic)
	if !sub.waitAck(sid, 5*time.Second) {
		t.Fatal("no subscribe ack")
	}
	if pub, err = dial(ctx, s.tcpAddr); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pub.close)
	if _, err := pub.connect(cid, true, nextSess()); err != nil {
		t.Fatal(err)
	}
	return pub, sub
}

func report(t *testing.T, name string, n int, elapsed time.Duration, lat latencies) {
	t.Helper()
	t.Logf("%s: %d msgs in %v = %.0f msg/s; latency p50=%v p99=%v max=%v%s",
		name, n, elapsed.Round(time.Millisecond), float64(n)/elapsed.Seconds(),
		lat.pct(0.50).Round(time.Microsecond), lat.pct(0.99).Round(time.Microsecond),
		lat.pct(1).Round(time.Microsecond), raceNote())
}

func raceNote() string {
	if raceEnabled {
		return " (race build)"
	}
	return ""
}

func TestPerfExpressThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("performance test")
	}
	s := startServer(t)
	const n = 5000
	pub, sub := perfPair(t, s, 0x9e7f0001, "groups.perf.express.message", 0)
	start := time.Now()
	go func() {
		for i := 0; i < n; i++ {
			pub.publish(0, "groups.perf.express.message", stamp(i), "1m")
		}
	}()
	lat := receiveAll(t, sub, n, 60*time.Second)
	report(t, "express (mode 0)", n, time.Since(start), lat)
}

func TestPerfReliableThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("performance test")
	}
	s := startServer(t)
	const n = 2000
	pub, sub := perfPair(t, s, 0x9e7f0002, "groups.perf.reliable.message", 1)
	start := time.Now()
	go func() {
		for i := 0; i < n; i++ {
			pub.publish(1, "groups.perf.reliable.message", stamp(i), "1h")
		}
	}()
	lat := receiveAll(t, sub, n, 90*time.Second)
	report(t, "reliable (mode 1)", n, time.Since(start), lat)
}

func TestPerfConcurrentThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("performance test")
	}
	s := startServer(t)
	const pairs = 8
	const n = 1000
	type pair struct{ pub, sub *client }
	ps := make([]pair, pairs)
	for i := range ps {
		ps[i].pub, ps[i].sub = perfPair(t, s, 0x9e7f0100, fmt.Sprintf("groups.perf.c%d.message", i), 0)
	}
	start := time.Now()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var all latencies
	for i, p := range ps {
		topic := fmt.Sprintf("groups.perf.c%d.message", i)
		go func(p pair) {
			for j := 0; j < n; j++ {
				p.pub.publish(0, topic, stamp(j), "1m")
			}
		}(p)
		wg.Add(1)
		go func(p pair) {
			defer wg.Done()
			lat := receiveAll(t, p.sub, n, 90*time.Second)
			mu.Lock()
			all = append(all, lat...)
			mu.Unlock()
		}(p)
	}
	wg.Wait()
	report(t, fmt.Sprintf("concurrent (%d pairs)", pairs), pairs*n, time.Since(start), all)
}

func TestPerfFanout(t *testing.T) {
	if testing.Short() {
		t.Skip("performance test")
	}
	s := startServer(t)
	const subs = 20
	const n = 500
	topic := "groups.perf.fanout.message"
	pub, first := perfPair(t, s, 0x9e7f0200, topic, 0)
	subsList := []*client{first}
	for i := 1; i < subs; i++ {
		_, sub := perfPair(t, s, 0x9e7f0200, topic, 0)
		subsList = append(subsList, sub)
	}
	start := time.Now()
	go func() {
		for i := 0; i < n; i++ {
			pub.publish(0, topic, stamp(i), "1m")
		}
	}()
	var wg sync.WaitGroup
	var mu sync.Mutex
	var all latencies
	for _, sub := range subsList {
		wg.Add(1)
		go func(sub *client) {
			defer wg.Done()
			lat := receiveAll(t, sub, n, 90*time.Second)
			mu.Lock()
			all = append(all, lat...)
			mu.Unlock()
		}(sub)
	}
	wg.Wait()
	report(t, fmt.Sprintf("fanout (1 pub -> %d subs)", len(subsList)), n*len(subsList), time.Since(start), all)
}

func TestPerfRelay(t *testing.T) {
	if testing.Short() {
		t.Skip("performance test")
	}
	s := startServer(t)
	const n = 1000
	topic := "groups.perf.relay.message"
	pub, _ := perfPair(t, s, 0x9e7f0300, topic, 0)
	for i := 0; i < n; i++ {
		id, _ := pub.publish(1, topic, encodePayload(i, "r"), "1h")
		pub.waitAck(id, 5*time.Second)
	}
	time.Sleep(1500 * time.Millisecond)

	ctx := context.Background()
	reader, err := dial(ctx, s.tcpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.close()
	if _, err := reader.connect(newClientID(0x9e7f0300), true, nextSess()); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	reader.relay(topic, "1h")
	got, dups, err := collectUnique(reader, n, 60*time.Second, 5*time.Second)
	if err != nil {
		t.Fatalf("relay: %v", err)
	}
	elapsed := time.Since(start)
	t.Logf("relay: %d stored msgs retrieved in %v = %.0f msg/s (%d duplicates)%s",
		len(got), elapsed.Round(time.Millisecond), float64(len(got))/elapsed.Seconds(), dups, raceNote())
}
