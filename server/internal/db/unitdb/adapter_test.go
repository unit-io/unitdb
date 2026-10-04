package adapter

import (
	"testing"

	"github.com/unit-io/unitdb/memdb"
)

// TestMessageRewrittenSurvivesReopen checks that a key deleted and put again,
// as a session's log key is, reads its new value after the store reopens.
// memdb's recovery applied the delete after the later put, so the key read
// as missing.
func TestMessageRewrittenSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	open := func() *adapter {
		a := &adapter{}
		if err := a.Open(dir, `{}`, false); err != nil {
			t.Fatal(err)
		}
		return a
	}
	a := open()
	const key = 42
	if err := a.PutMessage(key, []byte("first")); err != nil {
		t.Fatal(err)
	}
	if err := a.DeleteMessage(key); err != nil {
		t.Fatal(err)
	}
	if err := a.PutMessage(key, []byte("second")); err != nil {
		t.Fatal(err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}

	a = open()
	defer a.Close()
	got, err := a.GetMessage(key)
	if err != nil || string(got) != "second" {
		t.Fatalf("after reopening: %q, %v; want %q", got, err, "second")
	}
	if keys := a.Keys(); len(keys) != 1 || keys[0] != key {
		t.Fatalf("keys after reopening: %v, want [%d]", keys, key)
	}
}

// TestDeleteManyVersions deletes a key put in more blocks than
// deleteVersions deletes: it returned nil, and the key read on.
func TestDeleteManyVersions(t *testing.T) {
	a := &adapter{}
	if err := a.Open(t.TempDir(), `{}`, false); err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	const key = 7
	// A batch puts in a block of its own: a version each.
	for i := 0; i < maxKeyVersions+1; i++ {
		if err := a.mem.Batch(func(b *memdb.Batch, _ <-chan struct{}) error {
			return b.Put(key, []byte("v"))
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.DeleteMessage(key); err == nil {
		t.Fatal("deleted a key of more versions than it deletes, and reported no error")
	}
	// The rest go on the next delete.
	if err := a.DeleteMessage(key); err != nil {
		t.Fatal(err)
	}
	if got, err := a.GetMessage(key); err == nil {
		t.Fatalf("the key reads %q after its deletes", got)
	}
	// A key with no version is deleted.
	if err := a.DeleteMessage(key); err != nil {
		t.Fatal(err)
	}
}

// TestDeleteClosed deletes from a closed store: the call dereferenced the
// closed store; before that, the error of the get was taken for the key not
// found, and the delete reported done.
func TestDeleteClosed(t *testing.T) {
	a := &adapter{}
	if err := a.Open(t.TempDir(), `{}`, false); err != nil {
		t.Fatal(err)
	}
	if err := a.PutMessage(1, []byte("v")); err != nil {
		t.Fatal(err)
	}
	a.Close()
	if err := a.DeleteMessage(1); err == nil {
		t.Fatal("deleted from a closed store, and reported no error")
	}
	// The other calls fail too, rather than panic.
	if err := a.PutMessage(1, []byte("v")); err == nil {
		t.Error("PutMessage on a closed store: no error")
	}
	if _, err := a.Get(0, "unit.closed", ""); err == nil {
		t.Error("Get on a closed store: no error")
	}
	if err := a.Put(0, "unit.closed", []byte("v"), ""); err == nil {
		t.Error("Put on a closed store: no error")
	}
	if err := a.Flush(); err == nil {
		t.Error("Flush on a closed store: no error")
	}
	if n := a.Count(); n != 0 {
		t.Errorf("Count on a closed store: %d", n)
	}
}
