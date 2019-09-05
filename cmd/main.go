package main

import (
	"log"
	"time"

	"github.com/frontnet/tracedb"
)

func abs(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

func batch1(testdb *tracedb.DB) {
	b, err := testdb.Batch()
	defer b.Abort()
	b.Put([]byte("foo"), []byte("bar1"))
	b.PutWithTTL([]byte("ayaz"), []byte("bar1"), time.Second*30)
	b.Put([]byte("riz"), []byte("bar1"))
	b.Put([]byte("b3"), []byte("bar1"))
	b.Delete([]byte("foo"))
	b.Put([]byte("foo"), []byte("bar2"))
	b.Put([]byte("ayaz"), []byte("bar2"))
	b.Put([]byte("b4"), []byte("bar1"))
	b.Delete([]byte("foo"))
	b.Delete([]byte("b3"))
	b.Delete([]byte("b4"))
	b.Write()
	b.Commit()
	if err != nil {
		log.Fatal(err)
		return
	}
}

func main() {
	berr := make(chan error, 2)
	// Opening a database.
	testdb, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer testdb.Close()

	batch1(testdb)

	// // Reading from a database.
	// val, err := testdb.Get([]byte("foo"))
	// if err != nil {
	// 	log.Fatal(err)
	// 	return
	// }
	// log.Printf("%s", val)

	go func() {
		berr <- testdb.Update(func(b *tracedb.Batch) error {
			b.PutWithTTL([]byte("ayaz"), []byte("bar"), time.Second*30)
			b.Put([]byte("riz"), []byte("bar"))
			b.Put([]byte("b3"), []byte("bar"))
			b.Delete([]byte("foo"))
			b.Write()
			return err
		})
	}()

	go func() {
		berr <- testdb.Update(func(b *tracedb.Batch) error {
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
		berr <- testdb.Update(func(b *tracedb.Batch) error {
			b.Delete([]byte("b4"))
			b.Write()
			return err
		})
	}()

	for i := 0; i < cap(berr); i++ {
		if err := <-berr; err != nil {
			log.Fatal(err)
			return
		}
	}

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
