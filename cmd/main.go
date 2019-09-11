package main

import (
	"log"

	"github.com/frontnet/tracedb"
)

func main() {
	// Opening a database.
	testdb, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer testdb.Close()

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

	print := func(b *tracedb.Batch, stop <-chan struct{}) error {
		select {
		case <-stop:
			// Iterating over key/value pairs.
			it = testdb.Items()
			for it.First(); it.Valid(); it.Next() {
				if it.Error() != nil {
					if err != tracedb.ErrIterationDone {
						log.Fatal(err)
						return err
					}
					break
				}
				log.Printf("%s %s", it.Item().Key(), it.Item().Value())
			}
		}
		return nil
	}

	g.Add(print)

	g.Run()
}
