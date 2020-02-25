package main

import (
	"log"
	"time"

	"github.com/unit-io/tracedb"
)

func print(topic []byte, db *tracedb.DB) {
	it, err := db.Items(&tracedb.Query{Topic: topic})
	if err != nil {
		log.Printf("print: %v", err)
		return
	}
	for it.First(); it.Valid(); it.Next() {
		err := it.Error()
		if err != nil {
			log.Fatal(err)
			return
		}
		log.Printf("%s %s", it.Item().Topic(), it.Item().Value())
	}
}

func printWithContract(topic []byte, contract uint32, db *tracedb.DB) {
	it, err := db.Items(&tracedb.Query{Topic: topic, Contract: contract, Limit: 100})
	if err != nil {
		log.Printf("print: %v", err)
		return
	}
	for it.First(); it.Valid(); it.Next() {
		err := it.Error()
		if err != nil {
			log.Fatal(err)
			return
		}
		log.Printf("%s %s", it.Item().Topic(), it.Item().Value())
	}
}

func main() {
	// Opening a database.
	db, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	print([]byte("unit8.b1?last=1m"), db)
	print([]byte("unit8.b.b1?last=1m"), db)
	print([]byte("unit8.b.b11?last=1m"), db)
	print([]byte("unit8?last=1m"), db)
	print([]byte("unit9?last=1m"), db)

	print([]byte("unit8.c.c1?last=30m"), db)
	print([]byte("unit8.c.c11?last=30m"), db)

	msgs, err := db.Get(&tracedb.Query{Topic: []byte("unit8.b.b1?last=1h"), Limit: 250})
	for _, msg := range msgs {
		log.Printf("%s ", msg)
	}

	contract, err := db.NewContract()

	messageId := db.NewID()
	err = db.PutEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("unit1?ttl=3m"),
		Payload:  []byte("unit1.1"),
		Contract: contract,
	})

	printWithContract([]byte("unit1?last=2m"), contract, db)

	err = db.DeleteEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("unit1"),
		Contract: contract,
	})

	printWithContract([]byte("unit1?last=2m"), contract, db)

	func(retry int) {
		i := 1
		for range time.Tick(1 * time.Millisecond) {
			for j := 0; j < 50; j++ {
				t := time.Now().Add(time.Duration(j) * time.Millisecond)
				p, _ := t.MarshalText()
				messageId := db.NewID()
				db.PutEntry(&tracedb.Entry{ID: messageId, Topic: []byte("unit8.c.*?ttl=1h"), Payload: p})

				// db.DeleteEntry(&tracedb.Entry{
				// 	ID:       messageId,
				// 	Topic:    []byte("unit8.b.*"),
				// 	Contract: contract,
				// })
			}
			if err != nil {
				log.Printf("Error update1: %s", err)
			}
			if i >= retry {
				break
			}
			i++
		}
	}(1)

	msgs, err = db.Get(&tracedb.Query{Topic: []byte("unit8.c.c1?last=1h"), Limit: 250})
	for _, msg := range msgs {
		log.Printf("%s ", msg)
	}

	print([]byte("unit8.c.c1?last=30m"), db)
	print([]byte("unit8.c.c11?last=30m"), db)

	time.Sleep(100 * time.Millisecond)
	func(retry int) {
		i := 1
		for range time.Tick(10 * time.Millisecond) {
			err := db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
				opts := tracedb.DefaultBatchOptions
				opts.Topic = []byte("unit8.b.*?ttl=1h")
				opts.AllowDuplicates = true
				b.SetOptions(opts)
				t := time.Now().Add(time.Duration(i) * time.Millisecond)
				p, _ := t.MarshalText()
				for j := 0; j < 250; j++ {
					b.Put(p)
					// if j%100 == 0 {
					// 	if err := b.Write(); err != nil {
					// 		return err
					// 	}
					// }
				}
				if err := b.Write(); err != nil {
					return err
				}
				go func() {
					<-completed // it signals batch has completed and fully committed to db
					log.Printf("batch completed")
					print([]byte("unit8.b.b1?last=3m"), db)
					print([]byte("unit8.b.b11?last=3m"), db)
				}()
				return nil
			})
			if err != nil {
				log.Printf("Error update1: %s", err)
			}
			if i >= retry {
				break
			}
			i++
		}
	}(1)

	print([]byte("unit8.b.b1?last=30m"), db)
	print([]byte("unit8.b.b11?last=30m"), db)

	messageId = db.NewID()
	err = db.PutEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("unit1?ttl=3m"),
		Payload:  []byte("unit1.2"),
		Contract: contract,
	})

	// print([]byte("unit1?last=2m"), db)

	err = db.DeleteEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("unit1"),
		Contract: contract,
	})

	// print([]byte("unit1?last=2m"), db)

	err = db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		// opts := tracedb.DefaultBatchOptions
		// opts.Encryption= true
		// b.SetOptions(opts)
		b.PutEntry(tracedb.NewEntry([]byte("unit1?ttl=3m"), []byte("unit1.1")))
		b.PutEntry(tracedb.NewEntry([]byte("unit2?ttl=3m"), []byte("unit2.1")))
		b.PutEntry(tracedb.NewEntry([]byte("unit3?ttl=3m"), []byte("unit3.1")))
		err := b.Write()
		return err
	})

	err = db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.PutEntry(&tracedb.Entry{Topic: []byte("unit8.*.b11"), Payload: []byte("unit8.*.b11.1"), Contract: contract})
		b.PutEntry(&tracedb.Entry{Topic: []byte("unit8.b.*"), Payload: []byte("unit8.b.*.1"), Contract: contract})
		b.PutEntry(&tracedb.Entry{Topic: []byte("unit8..."), Payload: []byte("unit8..."), Contract: contract})
		b.PutEntry(&tracedb.Entry{Topic: []byte("*"), Payload: []byte("*.1"), Contract: contract})
		b.PutEntry(&tracedb.Entry{Topic: []byte("..."), Payload: []byte("...1"), Contract: contract})
		err := b.Write()
		go func() {
			<-completed // it signals batch has completed and fully committed to log
			printWithContract([]byte("unit8.b.b11?last=3m"), contract, db)
		}()
		return err
	})

	g := db.NewBatchGroup()
	g.Add(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b1?ttl=2m"), []byte("unit8.b1.1")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.c1?ttl=1m"), []byte("unit8.c1.1")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b1?ttl=3m"), []byte("unit8.b1.1")))
		b.Write()
		go func() {
			<-completed // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b11"), []byte("unit8.b.b11.1")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b11"), []byte("unit8.b.b11.2")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b1"), []byte("unit8.b.b1.1")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.c.c11"), []byte("unit8.c.c11.1")))
		b.Write()
		go func() {
			<-completed // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b111"), []byte("unit8.b.b111.1")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b1"), []byte("unit8.b.b1.2")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b11"), []byte("unit8.b.b11.2")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.c.c111"), []byte("unit8.c.c111.1")))
		b.Write()
		go func() {
			<-completed // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	// err = g.Run()

	if err != nil {
		log.Fatal(err)
		return
	}

	func(retry int) {
		i := 1
		for range time.Tick(1 * time.Second) {
			print([]byte("unit8.b1?last=20"), db)
			print([]byte("unit8.b.b1?last=20"), db)
			print([]byte("unit8.b.b11?last=20"), db)
			print([]byte("unit8?last=10"), db)
			print([]byte("unit9?last=10"), db)
			if i >= retry {
				return
			}
			i++
		}
	}(1)
	msgs, err = db.Get(&tracedb.Query{Topic: []byte("unit8.b.b1?last=1h"), Limit: 250})
	for _, msg := range msgs {
		log.Printf("%s ", msg)
	}
}
