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
	// fmt.Println("pagesize: ", os.Getpagesize())
	// Opening a database.
	db, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	print([]byte("unit8.b1?last=10m"), db)
	print([]byte("unit8.b.b1?last=10m"), db)
	print([]byte("unit8.b.b11?last=10m"), db)
	print([]byte("unit8?last=10m"), db)
	print([]byte("unit9?last=10m"), db)

	contract, err := db.NewContract()

	messageId := db.NewID()
	err = db.PutEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("ttl.ttl1?ttl=3m"),
		Payload:  []byte("ttl.ttl1.2"),
		Contract: contract,
	})

	print([]byte("ttl.ttl1?last=2m"), db)

	err = db.DeleteEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("ttl.ttl1"),
		Contract: contract,
	})

	print([]byte("ttl.ttl1?last=2m"), db)

	var start time.Time
	func(retry int) {
		i := 1
		for range time.Tick(100 * time.Millisecond) {
			start = time.Now()
			for j := 0; j < 50; j++ {
				t := time.Now().Add(time.Duration(j) * time.Millisecond)
				p, _ := t.MarshalText()
				messageId := db.NewID()
				db.PutEntry(&tracedb.Entry{ID: messageId, Topic: []byte("unit8.b.*?ttl=30m"), Payload: p, Contract: contract})

				db.DeleteEntry(&tracedb.Entry{
					ID:       messageId,
					Topic:    []byte("unit8.b.*"),
					Contract: contract,
				})
			}
			// log.Println("db.write ", time.Since(start).Seconds())
			if err != nil {
				log.Printf("Error update1: %s", err)
			}
			if i >= retry {
				break
			}
			i++
		}
	}(1)

	func(retry int) {
		i := 1
		for range time.Tick(100 * time.Millisecond) {
			start = time.Now()
			for j := 0; j < 10; j++ {
				t := time.Now().Add(time.Duration(j) * time.Millisecond)
				p, _ := t.MarshalText()
				db.PutEntry(&tracedb.Entry{Topic: []byte("unit8.c.*?ttl=30m"), Payload: p})
			}
			// log.Println("db.write ", time.Since(start).Seconds())
			if err != nil {
				log.Printf("Error update1: %s", err)
			}
			if i >= retry {
				break
			}
			i++
		}
	}(5)

	// print([]byte("unit8.c.c1?last=30m"), db)
	// print([]byte("unit8.c.c11?last=30m"), db)

	// func(retry int) {
	// 	i := 1
	// 	for range time.Tick(100 * time.Millisecond) {
	// 		err := db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
	// 			for j := 0; j < 100; j++ {
	// 				t := time.Now().Add(time.Duration(j) * time.Millisecond)
	// 				p, _ := t.MarshalText()
	// 				b.Put([]byte("unit8.b.*?ttl=30m"), p)
	// 			}
	// 			start = time.Now()
	// 			err := b.Write()
	// 			go func() {
	// 				<-completed // it signals batch has completed and fully committed to log
	// 				log.Printf("batch completed")
	// 				print([]byte("unit8.b.b1?last=30m"), db)
	// 				print([]byte("unit8.b.b11?last=30m"), db)
	// 			}()
	// 			return err
	// 		})
	// 		log.Println("batch.write ", time.Since(start).Seconds())
	// 		if err != nil {
	// 			log.Printf("Error update1: %s", err)
	// 		}
	// 		if i >= retry {
	// 			break
	// 		}
	// 		i++
	// 	}
	// }(1)

	func(retry int) {
		i := 1
		for range time.Tick(100 * time.Millisecond) {
			err := db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
				for j := 0; j < 250; j++ {
					t := time.Now().Add(time.Duration(j) * time.Millisecond)
					p, _ := t.MarshalText()
					b.Put([]byte("unit8.b.*?ttl=30m"), p)
					if j%100 == 0 {
						if err := b.Write(); err != nil {
							return err
						}
					}
				}
				start = time.Now()
				if err := b.Write(); err != nil {
					return err
				}
				go func() {
					<-completed // it signals batch has completed and fully committed to log
					log.Printf("batch completed")
					print([]byte("unit8.b.b1?last=30m"), db)
					print([]byte("unit8.b.b11?last=30m"), db)
				}()
				return nil
			})
			// log.Println("batch.write ", time.Since(start).Seconds())
			if err != nil {
				log.Printf("Error update1: %s", err)
			}
			if i >= retry {
				break
			}
			i++
		}
	}(1)

	// print([]byte("unit8.b.b1?last=30m"), db)
	// print([]byte("unit8.b.b11?last=30m"), db)

	messageId = db.NewID()
	err = db.PutEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("ttl.ttl1?ttl=3m"),
		Payload:  []byte("ttl.ttl1.3"),
		Contract: contract,
	})

	print([]byte("ttl.ttl1?last=2m"), db)

	err = db.DeleteEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("ttl.ttl1"),
		Contract: contract,
	})

	print([]byte("ttl.ttl1?last=2m"), db)

	err = db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		// opts := tracedb.DefaultBatchOptions
		// opts.Encryption = true
		// b.SetOptions(opts)
		b.Put([]byte("ttl.ttl1?ttl=3m"), []byte("ttl.ttl1.1"))
		b.Put([]byte("ttl.ttl2?ttl=3m"), []byte("ttl.ttl2.1"))
		b.Put([]byte("ttl.ttl3?ttl=3m"), []byte("ttl.ttl3.1"))
		err := b.Write()
		return err
	})

	err = db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		t, _ := time.Now().MarshalText()
		b.Put([]byte("ttl.ttl3?ttl=3m"), t)
		err := b.Write()

		return err
	})
	if err != nil {
		log.Print(err)
	}

	print([]byte("ttl.ttl3?last=2m"), db)

	err = db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.PutEntry(&tracedb.Entry{Topic: []byte("unit8.*.b11"), Payload: []byte("unit8.*.b11.1"), Contract: contract})
		b.PutEntry(&tracedb.Entry{Topic: []byte("unit8.b.*"), Payload: []byte("unit8.b.*.1"), Contract: contract})
		b.PutEntry(&tracedb.Entry{Topic: []byte("unit8..."), Payload: []byte("unit8..."), Contract: contract})
		b.PutEntry(&tracedb.Entry{Topic: []byte("*"), Payload: []byte("*.1"), Contract: contract})
		b.PutEntry(&tracedb.Entry{Topic: []byte("..."), Payload: []byte("...1"), Contract: contract})
		err := b.Write()
		go func() {
			<-completed // it signals batch has completed and fully committed to log
			printWithContract([]byte("unit8.b.b11?last=30m"), contract, db)
		}()
		return err
	})

	g := db.NewBatchGroup()
	g.Add(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.Put([]byte("unit8.b1?ttl=2m"), []byte("unit8.b1.1"))
		b.Put([]byte("unit8.c1?ttl=1m"), []byte("unit8.c1.1"))
		b.Put([]byte("unit8.b1?ttl=3m"), []byte("unit8.b1.1"))
		b.Write()
		go func() {
			<-completed // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.Put([]byte("unit8.b.b11"), []byte("unit8.b.b11.1"))
		b.Put([]byte("unit8.b.b11"), []byte("unit8.b.b11.2"))
		b.Put([]byte("unit8.b.b1"), []byte("unit8.b.b1.1"))
		b.Put([]byte("unit8.c.c11"), []byte("unit8.c.c11.1"))
		b.Write()
		go func() {
			<-completed // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.Put([]byte("unit8.b.b111"), []byte("unit8.b.b111.1"))
		b.Put([]byte("unit8.b.b1"), []byte("unit8.b.b1.2"))
		b.Put([]byte("unit8.b.b11"), []byte("unit8.b.b11.2"))
		b.Put([]byte("unit8.c.c111"), []byte("unit8.c.c111.1"))
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
		for range time.Tick(1000 * time.Millisecond) {
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
}
