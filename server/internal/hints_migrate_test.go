package internal

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/store"
)

// fakeLegacyHints holds hints as a v0.6.0 node kept them, in memory.
type fakeLegacyHints struct {
	ids, raw [][]byte
	// failDelete fails the deletes, as a crash after the copies would.
	failDelete bool
}

func (f *fakeLegacyHints) get(string) ([][]byte, [][]byte, error) {
	return append([][]byte(nil), f.ids...), append([][]byte(nil), f.raw...), nil
}

func (f *fakeLegacyHints) delete(_ string, id []byte) error {
	if f.failDelete {
		return errors.New("crash")
	}
	for i := range f.ids {
		if bytes.Equal(f.ids[i], id) {
			f.ids = append(f.ids[:i], f.ids[i+1:]...)
			f.raw = append(f.raw[:i], f.raw[i+1:]...)
			return nil
		}
	}
	return nil
}

// TestMoveLegacyHints moves the hints a v0.6.0 node kept for a node: each is
// stored again under a new id, which it holds; an expired one is dropped; an
// unreadable one is left; and a move stopped after its copies are stored
// stores none twice when it runs again.
func TestMoveLegacyHints(t *testing.T) {
	openTestStore(t)
	node := fmt.Sprintf("legacy-hint-node-%d", time.Now().UnixNano())
	f := &fakeLegacyHints{}
	add := func(h hintRecord) {
		id, err := store.Hint.NewID()
		if err != nil {
			t.Fatal(err)
		}
		h.ID = id
		var buf bytes.Buffer
		if err := gob.NewEncoder(&buf).Encode(h); err != nil {
			t.Fatal(err)
		}
		f.ids, f.raw = append(f.ids, id), append(f.raw, buf.Bytes())
	}
	now := time.Now().Unix()
	add(hintRecord{Entry: ReplicaEntry{ID: "legacy/1", Contract: 1, Topic: "groups.legacy", Payload: []byte("m1"), Ttl: "1h", ExpiresAt: now + 3600}})
	add(hintRecord{Entry: ReplicaEntry{ID: "legacy/2", Contract: 1, Topic: "groups.legacy", Payload: []byte("m2")}})
	add(hintRecord{Entry: ReplicaEntry{ID: "legacy/3", Contract: 1, Topic: "groups.legacy", Payload: []byte("gone"), ExpiresAt: now - 1}})
	add(hintRecord{Op: &store.LogOp{Block: 7, Key: 7}})
	unreadableID, _ := store.Hint.NewID()
	f.ids, f.raw = append(f.ids, unreadableID), append(f.raw, []byte("not a hint"))

	legacyHints, deleteLegacyHint = f.get, f.delete
	t.Cleanup(func() { legacyHints, deleteLegacyHint = store.Hint.Legacy, store.Hint.DeleteLegacy })

	// Stopped after the copies are stored: the old hints stay.
	f.failDelete = true
	if _, err := moveLegacyHintsOf(node); err == nil {
		t.Fatal("the move was not stopped")
	}
	f.failDelete = false
	if _, err := moveLegacyHintsOf(node); err != nil {
		t.Fatal(err)
	}
	if len(f.ids) != 1 || !bytes.Equal(f.ids[0], unreadableID) {
		t.Errorf("%d old hints left, want the unreadable one", len(f.ids))
	}
	raw, err := store.Hint.Get(node)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, b := range raw {
		var h hintRecord
		if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&h); err != nil {
			t.Fatal(err)
		}
		for _, old := range f.ids {
			if bytes.Equal(old, h.ID) {
				t.Error("a hint holds its old id")
			}
		}
		if h.Op != nil {
			got["op"]++
		} else {
			got[h.Entry.ID]++
		}
		// It is deleted by the id it holds.
		if err := store.Hint.Delete(node, h.ID); err != nil {
			t.Fatal(err)
		}
	}
	want := map[string]int{"legacy/1": 1, "legacy/2": 1, "op": 1}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("hints moved %v, want %v", got, want)
	}
	if left, _ := store.Hint.Get(node); len(left) != 0 {
		t.Errorf("%d hints left after deleting each by the id it holds", len(left))
	}
}
