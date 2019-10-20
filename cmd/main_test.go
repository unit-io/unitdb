package main

import (
	"log"
	"testing"
	"time"

	"github.com/saffat-in/tracedb"
	m "github.com/saffat-in/tracedb/message"
)

func BenchmarkBatch(bm *testing.B) {
	// Opening a database.
	testdb, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer testdb.Close()
	g := testdb.NewBatchGroup()
	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		for i := 0; i < bm.N; i++ {
			b.Put([]byte("dev18.b1?ttl=2m"), []byte("bar"))
		}
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})
	g.Run()
}

func TestBatchupdate(t *testing.T) {
	start := time.Now()
	// Opening a database.
	testdb, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer testdb.Close()
	defer log.Printf("test.batch %d", time.Since(start).Nanoseconds())
	err = testdb.Batch(func(b *tracedb.Batch) error {
		opts := tracedb.DefaultBatchOptions
		opts.Encryption = true
		b.SetOptions(opts)
		b.Put([]byte("ttl.ttl1?ttl=3m"), []byte("bar"))
		b.Put([]byte("ttl.ttl2?ttl=3m"), []byte("bar"))
		b.Put([]byte("ttl.ttl3?ttl=3m"), []byte("bar"))
		err = b.Write()
		return err
	})
	err = testdb.Batch(func(b *tracedb.Batch) error {
		t, _ := time.Now().MarshalText()
		b.Put([]byte("ttl.ttl3?ttl=3m"), t)
		err := b.Write()

		return err
	})
	if err != nil {
		log.Print(err)
	}
}

func TestBatchGroup(t *testing.T) {
	start := time.Now()
	// Opening a database.
	testdb, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer testdb.Close()
	defer log.Printf("test.batch %d", time.Since(start).Nanoseconds())
	g := testdb.NewBatchGroup()
	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b1?ttl=2m"), []byte("bar"))
		b.Put([]byte("dev18.c1?ttl=1m"), []byte("bar"))
		b.Put([]byte("dev18.b1?ttl=3m"), []byte("bar2"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b.b11"), []byte("bar"))
		b.Put([]byte("dev18.b.b11"), []byte("bar2"))
		b.Put([]byte("dev18.b.b1"), []byte("bar3"))
		b.Put([]byte("dev18.c.c11"), []byte("bar"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b.b111"), []byte("bar"))
		b.Put([]byte("dev18.b.b111"), []byte("bar2"))
		b.Put([]byte("dev18.b.b11"), []byte("bar3"))
		b.Put([]byte("dev18.c.c111"), []byte("bar"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	err = g.Run()

	if err != nil {
		log.Fatal(err)
		return
	}
}

func TestPut(t *testing.T) {
	start := time.Now()
	// Opening a database.
	testdb, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer testdb.Close()
	defer log.Printf("test.Put %d", time.Since(start).Nanoseconds())
	id := m.GenID(&m.Entry{
		Topic:   []byte("ttl.ttl1"),
		Payload: []byte("bar"),
	})
	if id == nil {
		log.Printf("Key is empty.")
	}
	testdb.PutEntry(&m.Entry{
		ID:      m.ID(id),
		Topic:   []byte("ttl.ttl1?ttl=3m"),
		Payload: []byte("bar"),
	})
}

func TestIterator(t *testing.T) {
	// Opening a database.
	testdb, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer testdb.Close()
	id := m.GenID(&m.Entry{
		Topic:   []byte("ttl.ttl1"),
		Payload: []byte("bar"),
	})
	if id == nil {
		log.Printf("Key is empty.")
	}
	err = testdb.PutEntry(&m.Entry{
		ID:      m.ID(id),
		Topic:   []byte("ttl.ttl1?ttl=3m"),
		Payload: []byte("bar"),
	})
	if err != nil {
		log.Fatal(err)
		return
	}
	start := time.Now()
	defer log.Printf("test.Iterator %d", time.Since(start).Nanoseconds())
	it, err := testdb.Items(&tracedb.Query{Topic: []byte("ttl.ttl1?last=3m")})
	if err != nil {
		log.Fatal(err)
		return
	}
	// for i := 0; i < 10000; i++ {
	for it.First(); it.Valid(); it.Next() {
		err := it.Error()
		if err != nil {
			log.Fatal(err)
			return
		}
		log.Printf("%s %s", it.Item().Key(), it.Item().Value())
	}
	// }
}
