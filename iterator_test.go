package tracedb

import (
	"testing"
	"time"
)

func TestIteratorEmpty(t *testing.T) {
	time.Sleep(1 * time.Second)
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
	time.Sleep(1 * time.Second)
	db, err := open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit8.test")

	items := map[byte]bool{}
	var i byte
	// var vals, itvals [][]byte

	for i = 0; i < 255; i++ {
		items[i] = false
		topic := append(topic, []byte("?ttl=1h")...)
		val := []byte("msg.")
		val = append(val, i)
		// vals = append(vals, val)
		if err := db.PutEntry(&Entry{Topic: topic, Payload: val, Contract: contract}); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(1 * time.Second)
	topic = append(topic, []byte("?last=255")...)
	it, err := db.Items(&Query{Topic: topic, Contract: contract})
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
