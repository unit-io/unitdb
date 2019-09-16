package main

import (
	"log"
	"time"

	"github.com/frontnet/tracedb"
)

func print(testdb *tracedb.DB) {
	it := testdb.Items()
	for it.First(); it.Valid(); it.Next() {
		err := it.Error()
		if err != nil {
			if err != tracedb.ErrIterationDone {
				log.Fatal(err)
				return
			}
			break
		}
		log.Printf("%s %s", it.Item().Key(), it.Item().Value())
	}
}

func main() {
	// Opening a database.
	testdb, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer testdb.Close()
	testdb.Update(func(b *tracedb.Batch) error {
		b.PutWithTTL([]byte("ttl1"), []byte("bar"), time.Minute*1)
		b.Write()
		return nil
	})
	testdb.Update(func(b *tracedb.Batch) error {
		b.PutWithTTL([]byte("ttl2"), []byte("bar"), time.Minute*2)
		b.Write()
		return nil
	})
	testdb.Update(func(b *tracedb.Batch) error {
		b.PutWithTTL([]byte("ttl3"), []byte("bar"), time.Minute*3)
		b.Write()
		return nil
	})

	func(retry int) {
		i := 0
		for _ = range time.Tick(60 * time.Second) {
			print(testdb)
			if i >= retry {
				return
			}
			i++
		}
	}(3)

	g := testdb.NewBatchGroup()
	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		//b.PutWithTTL([]byte("ttl1"), []byte("bar"), time.Second*30)
		b.Put([]byte("b1"), []byte("bar"))
		b.Put([]byte("c1"), []byte("bar"))
		b.Put([]byte("b1"), []byte("bar2"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("b11"), []byte("bar"))
		b.Put([]byte("b11"), []byte("bar2"))
		b.Put([]byte("b1"), []byte("bar3"))
		b.Put([]byte("c11"), []byte("bar"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("b111"), []byte("bar"))
		b.Put([]byte("b111"), []byte("bar2"))
		b.Put([]byte("b11"), []byte("bar3"))
		b.Put([]byte("c111"), []byte("bar"))
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

	testdb.Update(func(b *tracedb.Batch) error {
		b.Delete([]byte("b111"))
		err := b.Write()
		if err != nil {
			log.Printf("Error update1: %s", err)
		}
		return err
	})

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
