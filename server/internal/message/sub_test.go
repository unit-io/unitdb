package message

import (
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/utp"
)

func TestStatsIncrementDecrement(t *testing.T) {
	s := NewStats()
	if first := s.Increment("teams.alpha", "key1", []byte("id1")); !first {
		t.Fatal("first increment must report first")
	}
	if first := s.Increment("teams.alpha", "key1", []byte("id2")); first {
		t.Fatal("second increment must not report first")
	}
	if !s.Exist("key1") || s.Exist("key2") {
		t.Fatal("unexpected Exist result")
	}

	if last, _ := s.Decrement("teams.alpha", "key1"); last {
		t.Fatal("decrement with remaining subscribers must not report last")
	}
	last, id := s.Decrement("teams.alpha", "key1")
	if !last {
		t.Fatal("final decrement must report last")
	}
	// The id of the first subscription is kept for the store delete.
	if string(id) != "id1" {
		t.Fatalf("id = %q, want %q", id, "id1")
	}
	if s.Exist("key1") {
		t.Fatal("stat must be removed after the last decrement")
	}

	if last, id := s.Decrement("teams.alpha", "missing"); last || id != nil {
		t.Fatal("decrement of unknown key must be a no-op")
	}
}

func TestStatsAll(t *testing.T) {
	s := NewStats()
	s.Increment("a", "k1", []byte("1"))
	s.Increment("b", "k2", []byte("2"))
	s.Increment("b", "k2", []byte("3"))

	all := s.All()
	if len(all) != 2 {
		t.Fatalf("len(All) = %d, want 2", len(all))
	}
	counts := map[string]int{}
	for _, st := range all {
		counts[st.Topic] = st.Counter
	}
	if counts["a"] != 1 || counts["b"] != 2 {
		t.Fatalf("unexpected counters %v", counts)
	}
}

func TestMessageIds(t *testing.T) {
	ids := NewMessageIds()
	a := ids.NextID(utp.PUBLISH)
	b := ids.NextID(utp.SUBSCRIBE)
	if a == b {
		t.Fatal("NextID must return distinct ids")
	}
	if ids.GetType(a) != utp.PUBLISH || ids.GetType(b) != utp.SUBSCRIBE {
		t.Fatal("GetType returned the wrong message type")
	}
	ids.FreeID(a)
	if ids.GetType(a) != 0 {
		t.Fatal("freed id must not have a type")
	}
}

func TestMessageSize(t *testing.T) {
	m := &Message{Payload: []byte("hello")}
	if m.Size() != 5 {
		t.Fatalf("Size = %d, want 5", m.Size())
	}
}

func TestMessageIdsSkipsResumedID(t *testing.T) {
	ids := NewMessageIds()
	ids.ResumeID(1)
	done := make(chan MID, 1)
	go func() { done <- ids.NextID(utp.PUBLISH) }()
	select {
	case id := <-done:
		if id == 1 {
			t.Fatal("NextID returned a resumed id")
		}
	case <-time.After(time.Second):
		t.Fatal("NextID deadlocked")
	}
}
