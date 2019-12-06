package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pkg/profile"
	"github.com/saffat-in/tracedb"
)

var (
	engine       = flag.String("e", "tracedb", "database engine name. tracedb, pogreb, goleveldb, bbolt or badger")
	numKeys      = flag.Int("n", 1000000, "number of keys")
	minKeySize   = flag.Int("mink", 16, "minimum key size")
	maxKeySize   = flag.Int("maxk", 64, "maximum key size")
	minValueSize = flag.Int("minv", 128, "minimum value size")
	maxValueSize = flag.Int("maxv", 512, "maximum value size")
	concurrency  = flag.Int("c", 271, "number of concurrent goroutines")
	dir          = flag.String("d", ".", "database directory")
	progress     = flag.Bool("p", false, "show progress")
	profileMode  = flag.String("profile", "", "enable profile. cpu, mem, block or mutex")
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

func main() {

	flag.Parse()

	if *dir == "" {
		flag.Usage()
		return
	}

	switch *profileMode {
	case "cpu":
		defer profile.Start(profile.CPUProfile).Stop()
	case "mem":
		defer profile.Start(profile.MemProfile).Stop()
	case "block":
		defer profile.Start(profile.BlockProfile).Stop()
	case "mutex":
		defer profile.Start(profile.MutexProfile).Stop()
	}

	if err := benchmark(*engine, *dir, *numKeys, *minKeySize, *maxKeySize, *minValueSize, *maxValueSize, *concurrency, *progress); err != nil {
		fmt.Fprintf(os.Stderr, "Error running benchmark: %v\n", err)
	}

	// Opening a database.
	db, err := tracedb.Open("example", nil)
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	var start time.Time
	func(retry int) {
		i := 0
		for _ = range time.Tick(100 * time.Millisecond) {
			start = time.Now()
			for j := 0; j < 500; j++ {
				t := time.Now().Add(time.Duration(j) * time.Millisecond)
				p, _ := t.MarshalText()
				db.PutEntry(&tracedb.Entry{Topic: []byte("dev18.b.*?ttl=30m"), Payload: p})
			}
			log.Println("db.write ", time.Since(start).Seconds())
			if err != nil {
				log.Printf("Error update1: %s", err)
			}
			if i >= retry {
				break
			}
			i++
		}
	}(0)

	db.PutEntry(&tracedb.Entry{
		Topic:   []byte("ttl.ttl1?ttl=3m"),
		Payload: []byte("ttl.ttl1.1"),
	})

	messageId := db.NewID()
	err = db.PutEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("ttl.ttl1?ttl=3m"),
		Payload:  []byte("ttl.ttl1.2"),
		Contract: 3376684800,
	})

	// print([]byte("ttl.ttl1?last=2m"), db)

	err = db.DeleteEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("ttl.ttl1"),
		Contract: 3376684800,
	})

	// print([]byte("ttl.ttl1?last=2m"), db)

	func(retry int) {
		i := 0
		for _ = range time.Tick(100 * time.Millisecond) {
			err := db.Batch(func(b *tracedb.Batch) error {
				for j := 0; j < 500; j++ {
					t := time.Now().Add(time.Duration(j) * time.Millisecond)
					p, _ := t.MarshalText()
					b.Put([]byte("dev18.b.*?ttl=30m"), p)
				}
				start = time.Now()
				err := b.Write()
				log.Println("batch.write ", time.Since(start).Seconds())
				return err
			})
			log.Println("batch.commit ", time.Since(start).Seconds())
			if err != nil {
				log.Printf("Error update1: %s", err)
			}
			if i >= retry {
				break
			}
			i++
		}
	}(1)

	// print([]byte("dev18.b.b1?last=30m"), db)
	// print([]byte("dev18.b.b11?last=30m"), db)

	messageId = db.NewID()
	err = db.PutEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("ttl.ttl1?ttl=3m"),
		Payload:  []byte("ttl.ttl1.3"),
		Contract: 3376684800,
	})

	// print([]byte("ttl.ttl1?last=2m"), db)

	err = db.DeleteEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("ttl.ttl1"),
		Contract: 3376684800,
	})

	// print([]byte("ttl.ttl1?last=2m"), db)

	err = db.Batch(func(b *tracedb.Batch) error {
		// opts := tracedb.DefaultBatchOptions
		// opts.Encryption = true
		// b.SetOptions(opts)
		b.Put([]byte("ttl.ttl1?ttl=3m"), []byte("ttl.ttl1.1"))
		b.Put([]byte("ttl.ttl2?ttl=3m"), []byte("ttl.ttl2.1"))
		b.Put([]byte("ttl.ttl3?ttl=3m"), []byte("ttl.ttl3.1"))
		err = b.Write()
		return err
	})

	err = db.Batch(func(b *tracedb.Batch) error {
		t, _ := time.Now().MarshalText()
		b.Put([]byte("ttl.ttl3?ttl=3m"), t)
		err := b.Write()

		return err
	})
	if err != nil {
		log.Print(err)
	}

	// print([]byte("ttl.ttl3?last=2m"), db)

	err = db.Batch(func(b *tracedb.Batch) error {
		b.Put([]byte("dev18.*.b11"), []byte("dev18.*.b11.1"))
		b.Put([]byte("dev18.b.*"), []byte("dev18.b.*.1"))
		b.Put([]byte("dev18..."), []byte("dev18...1"))
		b.Put([]byte("*"), []byte("*.1"))
		b.Put([]byte("..."), []byte("...1"))
		err = b.Write()
		return err
	})

	g := db.NewBatchGroup()
	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b1?ttl=2m"), []byte("dev18.b1.1"))
		b.Put([]byte("dev18.c1?ttl=1m"), []byte("dev18.c1.1"))
		b.Put([]byte("dev18.b1?ttl=3m"), []byte("dev18.b1.1"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b.b11"), []byte("dev18.b.b11.1"))
		b.Put([]byte("dev18.b.b11"), []byte("dev18.b.b11.2"))
		b.Put([]byte("dev18.b.b1"), []byte("dev18.b.b1.1"))
		b.Put([]byte("dev18.c.c11"), []byte("dev18.c.c11.1"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b.b111"), []byte("dev18.b.b111.1"))
		b.Put([]byte("dev18.b.b1"), []byte("dev18.b.b1.2"))
		b.Put([]byte("dev18.b.b11"), []byte("dev18.b.b11.2"))
		b.Put([]byte("dev18.c.c111"), []byte("dev18.c.c111.1"))
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

	func(retry int) {
		i := 0
		for _ = range time.Tick(10000 * time.Millisecond) {
			print([]byte("dev18.b.b1?last=10m"), db)
			print([]byte("dev18.b.b11?last=10m"), db)
			print([]byte("dev18?last=10m"), db)
			print([]byte("dev19?last=10m"), db)
			if i >= retry {
				return
			}
			i++
		}
	}(1)
}
