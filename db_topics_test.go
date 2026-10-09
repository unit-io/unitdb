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

package unitdb

import (
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"
)

func openTopicsDB(t *testing.T) *DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "topics"), WithDefaultOptions(), WithMutable())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustHash(t *testing.T, db *DB, topic string, contract uint32) uint64 {
	t.Helper()
	h, err := db.TopicHash([]byte(topic), contract)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func sameHashes(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	sort.Slice(a, func(i, j int) bool { return a[i] < a[j] })
	sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestMatchTopics(t *testing.T) {
	db := openTopicsDB(t)
	for _, topic := range []string{"teams.alpha.ch1", "teams.alpha.ch2", "teams.beta.ch1", "teams.alpha", "other.x"} {
		if err := db.Put([]byte(topic), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
	// Wildcard topics written to are not topics that exist for reading.
	db.Put([]byte("teams.*.ch1"), []byte("broadcast"))
	db.Put([]byte("teams..."), []byte("broadcast"))
	db.PutEntry(NewEntry([]byte("teams.alpha.ch1"), []byte("c7")).WithContract(7))
	h := func(s ...string) []uint64 {
		var out []uint64
		for _, x := range s {
			out = append(out, mustHash(t, db, x, 0))
		}
		return out
	}
	for _, tc := range []struct {
		pattern string
		want    []uint64
	}{
		{"teams.alpha.ch1", h("teams.alpha.ch1")},
		{"teams.*.ch1", h("teams.alpha.ch1", "teams.beta.ch1")},
		{"teams.alpha...", h("teams.alpha", "teams.alpha.ch1", "teams.alpha.ch2")},
		{"teams.*", h("teams.alpha")},
		{"...", h("teams.alpha.ch1", "teams.alpha.ch2", "teams.beta.ch1", "teams.alpha", "other.x")},
		{"teams.gamma.*", nil},
	} {
		got, err := db.MatchTopics([]byte(tc.pattern), 0)
		if err != nil {
			t.Fatalf("%s: %v", tc.pattern, err)
		}
		if !sameHashes(got, tc.want) {
			t.Errorf("MatchTopics(%q) = %v, want %v", tc.pattern, got, tc.want)
		}
	}
	got, _ := db.MatchTopics([]byte("teams..."), 7)
	if !sameHashes(got, []uint64{mustHash(t, db, "teams.alpha.ch1", 7)}) {
		t.Errorf("contract 7 = %v", got)
	}
	for _, bad := range []string{"", "a..b", "a.b*", "a?x"} {
		if _, err := db.MatchTopics([]byte(bad), 0); err == nil {
			t.Errorf("MatchTopics(%q) accepted", bad)
		}
	}
}

func TestReadTopicAndGetEntries(t *testing.T) {
	db := openTopicsDB(t)
	db.Put([]byte("teams.alpha.ch1"), []byte("a1"))
	db.Put([]byte("teams.alpha.ch1"), []byte("a2"))
	db.Put([]byte("teams.*.ch1"), []byte("all"))
	hA := mustHash(t, db, "teams.alpha.ch1", 0)
	hW := mustHash(t, db, "teams.*.ch1", 0)

	items, err := db.GetEntries(NewQuery([]byte("teams.alpha.ch1")))
	if err != nil || len(items) != 3 {
		t.Fatalf("GetEntries = %d items, %v", len(items), err)
	}
	if string(items[0].Payload) != "all" || items[0].TopicHash != hW || items[1].TopicHash != hA {
		t.Fatalf("topics of items = %x %x", items[0].TopicHash, items[1].TopicHash)
	}
	own, err := db.ReadTopic(hA, ReadOptions{})
	if err != nil || len(own) != 2 || string(own[0].Payload) != "a2" || string(own[1].Payload) != "a1" {
		t.Fatalf("ReadTopic = %+v %v", own, err)
	}
	if one, _ := db.ReadTopic(hA, ReadOptions{Limit: 1}); len(one) != 1 {
		t.Fatalf("limit 1 = %d", len(one))
	}
	if none, _ := db.ReadTopic(hA, ReadOptions{Since: time.Now().Add(time.Hour)}); len(none) != 0 {
		t.Fatalf("future since = %d", len(none))
	}
	// Deleting by the id ReadTopic returns removes the entry.
	if err := db.DeleteEntry(NewEntry([]byte("teams.alpha.ch1"), nil).WithID(own[0].ID)); err != nil {
		t.Fatal(err)
	}
	if left, _ := db.ReadTopic(hA, ReadOptions{}); len(left) != 1 || string(left[0].Payload) != "a1" {
		t.Fatalf("after delete = %+v", left)
	}
	if missing, err := db.ReadTopic(12345, ReadOptions{}); err != nil || missing != nil {
		t.Fatalf("unknown topic = %v %v", missing, err)
	}
}

func TestOnWrite(t *testing.T) {
	db := openTopicsDB(t)
	var mu sync.Mutex
	var events []WriteEvent
	remove := db.OnWrite(func(ev WriteEvent) {
		mu.Lock()
		events = append(events, ev)
		mu.Unlock()
	})
	db.Put([]byte("a.b?ttl=1h"), []byte("one"))
	err := db.Batch(func(b *Batch, _ <-chan struct{}) error {
		b.Put([]byte("a.c"), []byte("two"))
		return b.Put([]byte("a.*"), []byte("three"))
	})
	if err != nil {
		t.Fatal(err)
	}
	items, _ := db.GetEntries(NewQuery([]byte("a.b")).WithLimit(1))
	db.DeleteEntry(NewEntry([]byte("a.b"), nil).WithID(items[0].ID))
	// A failed batch fires nothing.
	db.Batch(func(b *Batch, _ <-chan struct{}) error {
		b.Put([]byte("a.d"), []byte("never"))
		return errBadPattern
	})
	remove()
	db.Put([]byte("a.e"), []byte("after remove"))

	if len(events) != 4 {
		t.Fatalf("events = %d: %+v", len(events), events)
	}
	if e := events[0]; e.Op != OpPut || string(e.Topic) != "a.b" || string(e.Payload) != "one" || e.ExpiresAt == 0 || len(e.ID) != 16 || e.TopicHash != mustHash(t, db, "a.b", 0) {
		t.Fatalf("put event = %+v", e)
	}
	if string(events[1].Topic) != "a.c" || !events[2].Wildcard() || events[1].Wildcard() {
		t.Fatalf("batch events = %+v %+v", events[1], events[2])
	}
	if e := events[3]; e.Op != OpDelete || string(e.Topic) != "a.b" || e.Payload != nil || string(e.ID) != string(items[0].ID) {
		t.Fatalf("delete event = %+v", e)
	}
}

func TestOnWriteMayWrite(t *testing.T) {
	db := openTopicsDB(t)
	n := 0
	db.OnWrite(func(ev WriteEvent) {
		n++
		if string(ev.Topic) == "src" {
			if err := db.Put([]byte("copy"), ev.Payload); err != nil {
				t.Error(err)
			}
		}
	})
	db.Put([]byte("src"), []byte("x"))
	if n != 2 {
		t.Fatalf("hook calls = %d, want 2", n)
	}
	if items, _ := db.Get(NewQuery([]byte("copy"))); len(items) != 1 {
		t.Fatalf("copy = %d", len(items))
	}
}

func TestDeleteEntryEventHash(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "x"), WithDefaultOptions(), WithMutable())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Put([]byte("a.b"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	h, _ := db.TopicHash([]byte("a.b"), 0)
	items, _ := db.ReadTopic(h, ReadOptions{})
	var got uint64
	remove := db.OnWrite(func(ev WriteEvent) { got = ev.TopicHash })
	defer remove()
	if err := db.DeleteEntry(NewEntry([]byte("a.b"), nil).WithID(items[0].ID)); err != nil {
		t.Fatal(err)
	}
	if got != h {
		t.Fatalf("delete event hash %x, want the put's %x", got, h)
	}
}
