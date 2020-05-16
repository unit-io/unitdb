package unitdb

import (
	"fmt"
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
	opts := &Options{BufferSize: 1 << 8, MemdbSize: 1 << 8, LogSize: 1 << 8, MinimumFreeBlocksSize: 1 << 4}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var i uint16
	var n uint16 = 1000

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

	if db.count != 0 {
		t.Fatal()
	}
	var ids [][]byte

	entry := &Entry{Topic: topic}
	entry.SetContract(contract)
	entry.SetTTL([]byte("1m"))
	for i = 0; i < n; i++ {
		messageId := db.NewID()
		entry.SetID(messageId)
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := db.SetEntry(entry, val); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, messageId)
	}

	verifyMsgsAndClose := func() {
		if count := db.Count(); count != int64(n) {
			if err := db.recoverLog(); err != nil {
				t.Fatal(err)
			}
		}
		qtopic := topic
		qtopic = append(qtopic, []byte("?last=1h")...)
		var v, vals [][]byte
		v, err = db.Get(&Query{Topic: qtopic, Contract: contract})
		if err != nil {
			t.Fatal(err)
		}
		for i = 0; i < n; i++ {
			val := []byte(fmt.Sprintf("msg.%2d", n-i-1))
			vals = append(vals, val)
		}

		if !reflect.DeepEqual(vals, v) {
			t.Fatalf("expected %v; got %v", vals, v)
		}
		if size, err := db.FileSize(); err != nil || size == 0 {
			t.Fatal(err)
		}
		if _, err = db.Varz(); err != nil {
			t.Fatal(err)
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
	_, err = db.Get(&Query{Topic: topic, Contract: contract, Limit: int(n)})
	if err != nil {
		t.Fatal(err)
	}

	for i = 0; i < n; i++ {
		messageId := db.NewID()
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := db.Put(topic, val); err != nil {
			t.Fatal(err)
		}
		if err := db.PutEntry(&Entry{ID: messageId, Topic: topic, Payload: val}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, messageId)
	}
	db.tinyCommit()
	dbsync := syncHandle{DB: db, internal: internal{}}
	if err := dbsync.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		db.Delete(id, topic)
	}
}

func TestBatch(t *testing.T) {
	opts := &Options{BufferSize: 1 << 8, MemdbSize: 1 << 8, LogSize: 1 << 8, MinimumFreeBlocksSize: 1 << 4}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit2.test")

	// if db.count != 0 {
	// 	t.Fatal()
	// }

	var i uint16
	var n uint16 = 255

	verifyMsgsAndClose := func() {
		if count := db.Count(); count != int64(n) {
			if err := db.recoverLog(); err != nil {
				t.Fatal(err)
			}
		}
		qtopic := topic
		qtopic = append(qtopic, []byte("?last=1h")...)
		var v, vals [][]byte
		v, err = db.Get(&Query{Topic: qtopic, Contract: contract})
		if err != nil {
			t.Fatal(err)
		}
		for i = 0; i < n; i++ {
			val := []byte(fmt.Sprintf("msg.%2d", n-i-1))
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
		var ids [][]byte
		for i = 0; i < n; i++ {
			messageId := db.NewID()
			topic := append(topic, []byte("?ttl=1h")...)
			val := []byte(fmt.Sprintf("msg.%2d", i))
			if err := b.PutEntry(&Entry{ID: messageId, Topic: topic, Payload: val, Contract: contract}); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, messageId)
		}
		for _, id := range ids {
			if err := b.Delete(id, topic); err != nil {
				t.Fatal(err)
			}
		}
		err := b.Write()
		return err
	})

	if err != nil {
		t.Fatal(err)
	}
	dbsync := syncHandle{DB: db, internal: internal{}}
	if err := dbsync.Sync(); err != nil {
		t.Fatal(err)
	}
	verifyMsgsAndClose()
}

func TestBatchGroup(t *testing.T) {
	opts := &Options{BufferSize: 1 << 8, MemdbSize: 1 << 8, LogSize: 1 << 8, MinimumFreeBlocksSize: 1 << 4}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit3.test")

	var i uint16
	var n uint16 = 255

	// var wg sync.WaitGroup
	batch := func(b *Batch, completed <-chan struct{}) error {
		// wg.Add(1)
		for i = 0; i < n; i++ {
			topic := append(topic, []byte("?ttl=1h")...)
			val := []byte(fmt.Sprintf("msg.%2d", i))
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
	opts := &Options{BackgroundKeyExpiry: true}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit4.test")

	var i uint16
	var n uint16 = 255

	// var wg sync.WaitGroup
	err = db.Batch(func(b *Batch, completed <-chan struct{}) error {
		expiresAt := uint32(time.Now().Add(-1 * time.Hour).Unix())

		for i = 0; i < n; i++ {
			val := []byte(fmt.Sprintf("msg.%2d", i))
			entry := &Entry{Topic: topic, Payload: val, Contract: contract, ExpiresAt: expiresAt}
			if err := db.PutEntry(entry); err != nil {
				t.Fatal(err)
			}
		}
		err := b.Write()
		return err
	})

	if err != nil {
		t.Fatal(err)
	}

	if data, err := db.Get(&Query{Topic: topic, Contract: contract, Limit: int(n)}); len(data) != 0 || err != nil {
		t.Fatal()
	}
	db.ExpireOldEntries()
}

func TestAbort(t *testing.T) {
	opts := &Options{BufferSize: 1 << 8, MemdbSize: 1 << 8, LogSize: 1 << 8, MinimumFreeBlocksSize: 1 << 4}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var i uint16
	var n uint16 = 1000

	topic := []byte("unit1.test")

	for i = 0; i < n; i++ {
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := db.Put(topic, val); err != nil {
			t.Fatal(err)
		}
	}
	db.tinyCommit()
	dbsync := syncHandle{DB: db, internal: internal{}}
	dbabort := syncHandle{DB: db, internal: dbsync.internal}
	dbabort.startSync()
	if err := dbsync.Sync(); err != nil {
		t.Fatal(err)
	}
	dbabort.abort()
}

func TestLeasing(t *testing.T) {
	opts := &Options{BufferSize: 1 << 8, MemdbSize: 1 << 8, LogSize: 1 << 8, MinimumFreeBlocksSize: 1 << 4}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var i uint16
	var n uint16 = 1000

	topic := []byte("unit1.test")
	var ids [][]byte
	for i = 0; i < n; i++ {
		messageId := db.NewID()
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := db.PutEntry(&Entry{ID: messageId, Topic: topic, Payload: val}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, messageId)
	}
	db.tinyCommit()
	dbsync := syncHandle{DB: db, internal: internal{}}
	if err := dbsync.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		db.Delete(id, topic)
	}
	for i = 0; i < n; i++ {
		messageId := db.NewID()
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := db.Put(topic, val); err != nil {
			t.Fatal(err)
		}
		if err := db.PutEntry(&Entry{ID: messageId, Topic: topic, Payload: val}); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, messageId)
	}
	db.tinyCommit()
	if err := dbsync.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		db.Delete(id, topic)
	}
}
