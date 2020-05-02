package unitdb

import (
	"os"
	"reflect"
	"testing"
	"time"
)

func open(path string, opts *Options) (*DB, error) {
	os.Remove(path + indexPostfix)
	os.Remove(path + dataPostfix)
	os.Remove(path + logPostfix)
	os.Remove(path + lockPostfix)
	os.Remove(path + windowPostfix)
	os.Remove(path + filterPostfix)
	return Open(path, opts)
}

func TestSimple(t *testing.T) {
	db, err := open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var i byte
	var n uint8 = 255

	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit1.test")

	if db.count != 0 {
		t.Fatal()
	}

	if data, err := db.Get(&Query{Topic: topic, Contract: contract}); data != nil || err != nil {
		t.Fatal()
	}

	id := db.NewID()

	if err := db.DeleteEntry(&Entry{ID: id, Topic: topic, Contract: contract}); err != nil {
		t.Fatal(err)
	}

	if db.count != 0 {
		t.Fatal()
	}

	entry := &Entry{Topic: topic, Contract: contract}
	for i = 0; i < n; i++ {
		val := []byte("msg.")
		val = append(val, i)
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
	if count := db.Count(); count != int64(n) {
		varz, err := db.Varz()
		if err != nil {
			t.Fatal(err)
		}
		t.Fatalf("expected %d records; got %d \n%+v", n, count, varz)
	}

	verifyMsgsAndClose := func() {
		if count := db.Count(); count != int64(n) {
			t.Fatalf("expected %d records; got %d", n, count)
		}
		qtopic := topic
		qtopic = append(qtopic, []byte("?last=1h")...)
		var v, vals [][]byte
		v, err = db.Get(&Query{Topic: qtopic, Contract: contract})
		if err != nil {
			t.Fatal(err)
		}
		for i = 0; i < n; i++ {
			val := []byte("msg.")
			val = append(val, n-i-1)
			vals = append(vals, val)
		}

		if !reflect.DeepEqual(vals, v) {
			t.Fatalf("expected %v; got %v", vals, v)
		}

		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	verifyMsgsAndClose()
	db, err = Open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}

}

func TestBatch(t *testing.T) {
	db, err := open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit2.test")

	if db.count != 0 {
		t.Fatal()
	}

	var i byte
	var n uint8 = 255

	verifyMsgsAndClose := func() {
		if count := db.Count(); count != int64(n) {
			t.Fatalf("expected %d records; got %d", n, count)
		}
		qtopic := topic
		qtopic = append(qtopic, []byte("?last=1h")...)
		var v, vals [][]byte
		v, err = db.Get(&Query{Topic: qtopic, Contract: contract})
		if err != nil {
			t.Fatal(err)
		}
		for i = 0; i < n; i++ {
			val := []byte("msg.")
			val = append(val, n-i-1)
			vals = append(vals, val)
		}
		if !reflect.DeepEqual(vals, v) {
			t.Fatalf("expected %v; got %v", vals, v)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	// var wg sync.WaitGroup
	err = db.Batch(func(b *Batch, completed <-chan struct{}) error {
		// wg.Add(1)
		for i = 0; i < n; i++ {
			topic := append(topic, []byte("?ttl=1h")...)
			val := []byte("msg.")
			val = append(val, i)
			if err := b.PutEntry(&Entry{Topic: topic, Payload: val, Contract: contract}); err != nil {
				t.Fatal(err)
			}
		}
		err := b.Write()
		return err
	})

	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	syncHandle := syncHandle{DB: db, internal: internal{}}
	if err := syncHandle.Sync(); err != nil {
		t.Fatal(err)
	}
	verifyMsgsAndClose()
}

func TestBatchGroup(t *testing.T) {
	db, err := open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit3.test")

	var i byte
	var n uint8 = 255

	// var wg sync.WaitGroup
	batch := func(b *Batch, completed <-chan struct{}) error {
		// wg.Add(1)
		for i = 0; i < n; i++ {
			topic := append(topic, []byte("?ttl=1h")...)
			val := []byte("msg.")
			val = append(val, i)
			if err := db.PutEntry(&Entry{Topic: topic, Payload: val, Contract: contract}); err != nil {
				t.Fatal(err)
			}
		}
		err := b.Write()
		return err
	}

	g := db.NewBatchGroup()
	g.Add(batch)
	g.Add(batch)
	g.Add(batch)

	if err := g.Run(); err != nil {
		t.Fatal(err)
	}

	// wg.Wait()
	time.Sleep(10 * time.Millisecond)
	syncHandle := syncHandle{DB: db, internal: internal{}}
	if err := syncHandle.Sync(); err != nil {
		t.Fatal(err)
	}
	// count -> 255+256+256
	qtopic := topic
	qtopic = append(qtopic, []byte("?last=1h")...)
	for i = 0; i < n; i++ {
		_, err = db.Get(&Query{Topic: qtopic, Contract: contract})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestExpiry(t *testing.T) {
	db, err := open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit4.test")

	var i byte
	var n uint8 = 255

	// var wg sync.WaitGroup
	err = db.Batch(func(b *Batch, completed <-chan struct{}) error {
		// wg.Add(1)
		for i = 0; i < n; i++ {
			topic := append(topic, []byte("?ttl=1h")...)
			val := []byte("msg.")
			val = append(val, i)
			if err := db.PutEntry(&Entry{Topic: topic, Payload: val, Contract: contract}); err != nil {
				t.Fatal(err)
			}
		}
		err := b.Write()
		return err
	})

	if err != nil {
		t.Fatal(err)
	}
	// wg.Wait()
	time.Sleep(10 * time.Millisecond)
	db.ExpireOldEntries()

	if data, err := db.Get(&Query{Topic: topic, Contract: contract}); len(data) != 0 || err != nil {
		t.Fatal()
	}
}
