package main

import (
	"log"

	kv "github.com/frontnet/tracedb"
)

func printGet(key string, testdb *kv.DB) {
	// Reading from a database.
	val, err := testdb.Get([]byte(key))
	if err != nil {
		log.Fatal(err)
		return
	}
	log.Printf("%s %s", key, val)
}

func main() {
	// Opening a database.
	testdb, err := kv.Open("example", nil)
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

	// testdb.PutWithTTL([]byte("b4"), []byte("bar"), "1m")

	err = testdb.Update(func(b *kv.Batch) error {
		b.Put([]byte("foo"), []byte("bar"))
		b.Put([]byte("ayaz"), []byte("bar"))
		b.Put([]byte("riz"), []byte("bar"))
		b.Put([]byte("b3"), []byte("bar"))
		b.Delete([]byte("foo"))
		b.Delete([]byte("b3"))
		b.Write()
		return err
	})

	if err != nil {
		log.Fatal(err)
		return
	}

	// printGet("foo", testdb)
	// printGet("ayaz", testdb)
	// printGet("riz", testdb)
	// printGet("yam", testdb)
	// printGet("b3", testdb)

	// Iterating over key/value pairs.
	bit := testdb.Items()
	for {
		key, val, err := bit.Next()
		if err != nil {
			if err != kv.ErrIterationDone {
				log.Fatal(err)
				return
			}
			break
		}
		log.Printf("%s %s", key, val)
	}
}
