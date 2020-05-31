package main

import (
	"fmt"
	"log"
	"time"

	"github.com/unit-io/unitdb"
)

func print(topic []byte, db *unitdb.DB) {
	it, err := db.Items(&unitdb.Query{Topic: topic})
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

func printWithContract(topic []byte, contract uint32, db *unitdb.DB) {
	it, err := db.Items(&unitdb.Query{Topic: topic, Contract: contract, Limit: 100})
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
	opts := &unitdb.Options{BufferSize: 1 << 27, MemdbSize: 1 << 32, LogSize: 1 << 30, MinimumFreeBlocksSize: 1 << 27}
	flags := &unitdb.Flags{Immutable: 1, Encryption: -1, BackgroundKeyExpiry: -1}
	db, err := unitdb.Open("example", flags, opts)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	contract, err := db.NewContract()
	if err != nil {
		log.Fatal(err)
		return
	}

	print([]byte("unit8.b1?last=1h"), db)
	print([]byte("unit8.b.b1?last=1h"), db)
	print([]byte("unit8.b.b11?last=1h"), db)
	print([]byte("unit8?last=1m"), db)
	print([]byte("unit9?last=1m"), db)

	print([]byte("unit8.c.c1?last=30m"), db)
	print([]byte("unit8.c.c11?last=30m"), db)

	if msgs, err := db.Get(&unitdb.Query{Topic: []byte("unit8.b.b1?last=1h"), Limit: 200}); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	reqs := []struct {
		wtopic []byte
		topic  []byte
		msg    []byte
	}{
		{[]byte("..."), []byte("unit8.b.b1"), []byte("...1")},
		{[]byte("unit.*.b1.b11.*.*.b11111.*"), []byte("unit"), []byte("unit.*.b1.b11.*.*.b11111.*.1")},
		{[]byte("unit.b..."), []byte("unit.b.b1.b11.b111.b1111.b11111.b111111"), []byte("unit.b...1")},
		{[]byte("unit.*.b1.*.*.*.b11111.*"), []byte("unit.b.b1?ttl=3m"), []byte("unit.*.b1.*.*.*.b11111.*.1")},
		{[]byte("unit.b.b1"), []byte("unit.b.b1?ttl=3m"), []byte("unit.b.b1.1")},
		{[]byte("unit.b.b1.b11"), []byte("unit.b.b1.b11"), []byte("unit.b.b1.b11.1")},
		{[]byte("unit.b"), []byte("unit.b"), []byte("unit.b.1")},
		{[]byte("unit8.b.b1"), []byte("unit8.b.b1"), []byte("unit8.b.b1.1")},
	}
	for i, r := range reqs {
		db.Put(r.wtopic, r.msg)
		if msgs, err := db.Get(&unitdb.Query{Topic: r.wtopic, Limit: 10}); err == nil {
			for _, msg := range msgs {
				log.Printf("req %d %s ", i, msg)
			}
		}
		if msgs, err := db.Get(&unitdb.Query{Topic: r.topic, Limit: 10}); err == nil {
			for _, msg := range msgs {
				log.Printf("req %d %s ", i, msg)
			}
		}
	}

	messageId := db.NewID()
	entry := &unitdb.Entry{
		ID:    messageId,
		Topic: []byte("unit.*.b1.*.*.*.b11111.*?ttl=3m"),
		// Contract: contract,
	}
	db.SetEntry(entry, []byte("unit.*.b1.*.*.*.b11111.*.1"))

	print([]byte("unit.*.b1.*.*.*.b11111.*?last=2m"), db)

	if err := db.DeleteEntry(&unitdb.Entry{
		ID:       messageId,
		Topic:    []byte("unit1"),
		Contract: contract,
	}); err != nil {
		log.Printf("Error update1: %s", err)
	}
	print([]byte("unit.b.b1.b11.b111.b1111.b11111.b111111?last=2m"), db)

	func(retry int) {
		i := 1
		entry := &unitdb.Entry{Topic: []byte("unit8.c.c1?ttl=1h")}
		for range time.Tick(1 * time.Millisecond) {
			for j := 0; j < 50; j++ {
				db.SetEntry(entry, []byte(fmt.Sprintf("msg.%2d", j)))
			}
			if i >= retry {
				break
			}
			i++
		}
	}(1)

	func(retry int) {
		i := 1
		entry := &unitdb.Entry{Topic: []byte("unit8.c.*?ttl=1h")}
		for range time.Tick(1 * time.Millisecond) {
			for j := 50; j < 100; j++ {
				db.SetEntry(entry, []byte(fmt.Sprintf("msg.%2d", j)))
			}
			if i >= retry {
				break
			}
			i++
		}
	}(1)

	print([]byte("unit8.c.c1?last=10s"), db)

	if msgs, err := db.Get(&unitdb.Query{Topic: []byte("unit8.c.*?last=1h"), Limit: 100}); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	print([]byte("unit8.c.c1?last=30m"), db)
	print([]byte("unit8.c.c11?last=30m"), db)

	time.Sleep(100 * time.Millisecond)
	func(retry int) {
		i := 1
		for range time.Tick(10 * time.Millisecond) {
			err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
				opts := unitdb.DefaultBatchOptions
				opts.Topic = []byte("unit8.b.*?ttl=1h")
				opts.AllowDuplicates = true
				b.SetOptions(opts)
				for j := 0; j < 50; j++ {
					b.Put([]byte(fmt.Sprintf("msg.%2d", j)))
				}
				return b.Write()
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
	if err := db.PutEntry(&unitdb.Entry{
		ID:       messageId,
		Topic:    []byte("unit1?ttl=3m"),
		Payload:  []byte("unit1.2"),
		Contract: contract,
	}); err != nil {
		log.Printf("Error %s", err)
	}

	printWithContract([]byte("unit1?last=2m"), contract, db)

	if err := db.DeleteEntry(&unitdb.Entry{
		ID:       messageId,
		Topic:    []byte("unit1"),
		Contract: contract,
	}); err != nil {
		log.Printf("Error %s", err)
	}

	printWithContract([]byte("unit1?last=2m"), contract, db)

	if err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		opts := unitdb.DefaultBatchOptions
		opts.Encryption = true
		b.SetOptions(opts)
		b.PutEntry(unitdb.NewEntry([]byte("unit1?ttl=3m"), []byte("unit1.1")))
		b.PutEntry(unitdb.NewEntry([]byte("unit2?ttl=3m"), []byte("unit2.1")))
		b.PutEntry(unitdb.NewEntry([]byte("unit3?ttl=3m"), []byte("unit3.1")))
		err := b.Write()
		return err
	}); err != nil {
		log.Printf("Error %s", err)
	}

	if err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(&unitdb.Entry{Topic: []byte("unit8.*.b11"), Payload: []byte("unit8.*.b11.1"), Contract: contract})
		b.PutEntry(&unitdb.Entry{Topic: []byte("unit8.b.*"), Payload: []byte("unit8.b.*.1"), Contract: contract})
		b.PutEntry(&unitdb.Entry{Topic: []byte("unit8"), Payload: []byte("unit8.1"), Contract: contract})
		b.PutEntry(&unitdb.Entry{Topic: []byte("*"), Payload: []byte("*.1"), Contract: contract})
		b.PutEntry(&unitdb.Entry{Topic: []byte("..."), Payload: []byte("...1"), Contract: contract})
		err := b.Write()
		go func() {
			<-completed // it signals batch has completed and fully committed to log
			printWithContract([]byte("unit8.b.b11?last=3m"), contract, db)
		}()
		return err
	}); err != nil {
		log.Printf("Error %s", err)
	}

	g := db.NewBatchGroup()
	g.Add(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b1?ttl=2m"), []byte("unit8.b1.1")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.c1?ttl=1m"), []byte("unit8.c1.1")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b1?ttl=3m"), []byte("unit8.b1.1")))
		b.Write()
		go func() {
			<-completed // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b11"), []byte("unit8.b.b11.1")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b11"), []byte("unit8.b.b11.2")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b1"), []byte("unit8.b.b1.1")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.c.c11"), []byte("unit8.c.c11.1")))
		b.Write()
		go func() {
			<-completed // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b111"), []byte("unit8.b.b111.1")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b1"), []byte("unit8.b.b1.2")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b11"), []byte("unit8.b.b11.2")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.c.c111"), []byte("unit8.c.c111.1")))
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
		for range time.Tick(3 * time.Second) {
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
	if msgs, err := db.Get(&unitdb.Query{Topic: []byte("unit8.b.b1?last=1h"), Limit: 100}); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}
}
