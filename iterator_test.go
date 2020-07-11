package unitdb

import (
	"fmt"
	"testing"
	"time"
)

func TestIteratorEmpty(t *testing.T) {
	db, err := open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Items(NewQuery(nil))
	if err == nil {
		t.Fatal(err)
	}
}

func TestIterator(t *testing.T) {
	db, err := open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}

	var i uint16
	var n uint16 = 255
	items := map[uint16]bool{}

	entry := NewEntry([]byte("unit5.test?ttl=1m")).WithContract(contract)
	for i = 0; i < n; i++ {
		items[i] = false
		val := []byte(fmt.Sprintf("msg.%2d", i))
		entry.WithPayload(val)
		if err := db.PutEntry(entry); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(10 * time.Millisecond)
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)
	it, err := db.Items(NewQuery([]byte("unit5.test?last=1s")).WithContract(contract))
	if err != nil {
		t.Fatal(err)
	}
	i = 0
	for it.First(); it.Valid(); it.Next() {
		err := it.Error()
		if err != nil {
			t.Fatal(err)
		}
		i++
	}

	if i != n {
		t.Fatalf("expected %d records; got %d", n, i)
	}
}
