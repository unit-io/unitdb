package tracedb

import (
	"testing"
	"time"
)

func TestIteratorEmpty(t *testing.T) {
	db, err := open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Items(&Query{})
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

	items := map[byte]bool{}
	var i byte
	// var vals, itvals [][]byte

	entry := &Entry{Topic: []byte("unit12.test?ttl=1h"), Contract: contract}
	for i = 0; i < 255; i++ {
		items[i] = false
		val := []byte("msg.")
		val = append(val, i)
		// vals = append(vals, val)
		entry.SetPayload(val)
		if err := db.PutEntry(entry); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(10 * time.Millisecond)
	syncHandle := syncHandle{DB: db, internal: internal{}}
	if err := syncHandle.Sync(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)
	it, err := db.Items(&Query{Topic: []byte("unit12.test?last=255"), Contract: contract})
	if err != nil {
		t.Fatal(err)
	}
	i = 0
	for it.First(); it.Valid(); it.Next() {
		err := it.Error()
		if err != nil {
			t.Fatal(err)
		}
		// vals = append(vals, it.Item().Value())
		i++
	}
	// if !reflect.DeepEqual(vals, itvals) {
	// 	t.Fatalf("expected %v; got %v", vals, itvals)
	// }

	if i != 255 {
		t.Fatalf("expected 255 records; got %d", i)
	}
}
