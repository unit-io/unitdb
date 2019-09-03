package main

import (
	"log"
	"time"

	"github.com/frontnet/tracedb"
)

func printGet(key string, testdb *tracedb.DB) {
	// Reading from a database.
	val, err := testdb.Get([]byte(key))
	if err != nil {
		log.Fatal(err)
		return
	}
	log.Printf("%s %s", key, val)
}

var batchC chan struct{}

func main() {
	// Opening a database.
	testdb, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer testdb.Close()

	// Writing to a database.
	err = testdb.Put([]byte("foo"), []byte("bar"))
	if err != nil {
		log.Fatal(err)
		return
	}

	// printGet("foo", testdb)
	// testdb.PutWithTTL([]byte("b4"), []byte("bar"), "1m")
	batchC := make(chan struct{}, 1)
	batchC <- struct{}{}
	go func() {
		testdb.Update(func(b *tracedb.Batch) error {
			//b.Put([]byte("foo"), []byte("bar"))
			b.PutWithTTL([]byte("ayaz"), []byte("bar"), time.Second*30)
			b.Put([]byte("riz"), []byte("bar"))
			b.Put([]byte("b3"), []byte("bar"))
			b.Delete([]byte("foo"))
			//b.Delete([]byte("b3"))
			b.Write()
			return err
		})
	}()

	go func() {
		testdb.Update(func(b *tracedb.Batch) error {
			b.Put([]byte("foo"), []byte("bar"))
			// b.PutWithTTL([]byte("ayaz"), []byte("bar"), time.Second*30)
			// b.Put([]byte("riz"), []byte("bar"))
			b.Put([]byte("b4"), []byte("bar"))
			//b.Delete([]byte("foo"))
			b.Delete([]byte("b3"))
			b.Write()
			return err
		})
	}()

	go func() {
		testdb.Update(func(b *tracedb.Batch) error {
			b.Delete([]byte("b4"))
			b.Write()
			return err
		})
	}()

	if err != nil {
		log.Fatal(err)
		return
	}

	<-batchC
	// Iterating over key/value pairs.
	it := testdb.Items()
	for it.First(); it.Valid(); it.Next() {
		if it.Error() != nil {
			if err != tracedb.ErrIterationDone {
				log.Fatal(err)
				return
			}
			break
		}
		log.Printf("%s %s", it.Item().Key(), it.Item().Value())
	}
}
