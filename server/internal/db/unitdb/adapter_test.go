package adapter

import "testing"

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
