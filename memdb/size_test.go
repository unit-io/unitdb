package memdb

import "testing"

// TestSizeCountsAKeyOnce checks that a key put again is one record, not
// two: Size counted every put, so a store whose records are rewritten (a
// security state, a session) looked ever bigger, and a copy of it smaller.
func TestSizeCountsAKeyOnce(t *testing.T) {
	db, _ := openTestDB(t)
	for i := 0; i < 3; i++ {
		if _, err := db.Put(42, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Put(43, []byte("other")); err != nil {
		t.Fatal(err)
	}
	if size := db.Size(); size != 2 {
		t.Errorf("Size after putting one key 3 times and another once: %d, want 2", size)
	}
	if got, err := db.Get(42); err != nil || len(got) != 1 || got[0] != 2 {
		t.Errorf("Get(42): %v, %v; want the last put", got, err)
	}
	if err := db.Delete(42); err != nil {
		t.Fatal(err)
	}
	if size := db.Size(); size != 1 {
		t.Errorf("Size after deleting it: %d, want 1", size)
	}
}
