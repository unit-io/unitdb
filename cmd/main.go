package main

import (
	"log"
	"sync"
	"time"

	"github.com/frontnet/tracedb"
)

func abs(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

func batch1(testdb *tracedb.DB) {
	b, err := testdb.Batch(tracedb.DefaultBatchOptions)
	defer b.Abort()
	b.Put([]byte("foo"), []byte("bar1"))
	b.PutWithTTL([]byte("ayaz"), []byte("bar1"), time.Second*30)
	b.Put([]byte("riz"), []byte("bar1"))
	b.Put([]byte("b1"), []byte("bar1"))
	b.Delete([]byte("foo"))
	b.Put([]byte("foo"), []byte("bar2"))
	b.Put([]byte("ayaz"), []byte("bar2"))
	b.Put([]byte("b2"), []byte("bar1"))
	b.Delete([]byte("foo"))
	b.Delete([]byte("b1"))
	b.Delete([]byte("b2"))
	err = b.Write()
	if err != nil {
		log.Fatal(err)
		return
	}
	b.Commit()
}

func main() {
	var wg sync.WaitGroup
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

	wg.Add(3)
	go func() {
		defer wg.Done()
		berr <- testdb.Update(&tracedb.BatchOptions{Order: 1}, func(b *tracedb.Batch) error {
			//b.PutWithTTL([]byte("ayaz"), []byte("bar"), time.Second*30)
			//b.Put([]byte("riz"), []byte("bar"))
			b.Put([]byte("b3"), []byte("bar"))
			//b.Delete([]byte("foo"))
			err := b.Write()
			if err != nil {
				log.Printf("Error update1: %s", err)
			}
			return err
		})
	}()

	go func() {
		defer wg.Done()
		berr <- testdb.Update(&tracedb.BatchOptions{Order: 2}, func(b *tracedb.Batch) error {
			b.Put([]byte("foo"), []byte("bar"))
			// b.PutWithTTL([]byte("ayaz"), []byte("bar"), time.Second*30)
			// b.Put([]byte("riz"), []byte("bar"))
			b.Put([]byte("b4"), []byte("bar"))
			//b.Delete([]byte("foo"))
			//b.Delete([]byte("b3"))
			err := b.Write()
			if err != nil {
				log.Printf("Error update2: %s", err)
			}
			return err
		})
	}()

	go func() {
		defer wg.Done()
		berr <- testdb.Update(&tracedb.BatchOptions{Order: 3}, func(b *tracedb.Batch) error {
			b.Put([]byte("b5"), []byte("bar"))
			//b.Delete([]byte("b3"))
			err := b.Write()
			if err != nil {
				log.Printf("Error update3: %s", err)
			}
			return err
		})
	}()

	for i := 0; i < cap(berr); i++ {
		if err := <-berr; err != nil {
			log.Fatal(err)
			return
		}
	}

	wg.Wait()
	testdb.Update(tracedb.DefaultBatchOptions, func(b *tracedb.Batch) error {
		b.Delete([]byte("b3"))
		err := b.Write()
		if err != nil {
			log.Printf("Error update1: %s", err)
		}
		return err
	})

	time.Sleep(time.Second * 3)
	go func() {
		testdb.Update(&tracedb.BatchOptions{Order: 1}, func(b *tracedb.Batch) error {
			b.Delete([]byte("b4"))
			err := b.Write()
			if err != nil {
				log.Printf("Error update1: %s", err)
			}
			return err
		})
	}()
	go func() {
		testdb.Update(&tracedb.BatchOptions{Order: 2}, func(b *tracedb.Batch) error {
			b.Delete([]byte("b5"))
			err := b.Write()
			if err != nil {
				log.Printf("Error update1: %s", err)
			}
			return err
		})
	}()

	time.Sleep(time.Second * 3)
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
