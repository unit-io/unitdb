package main

import (
	"log"

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

	// err = testdb.Update(func(b *tracedb.Batch) error {
	// 	b.Put([]byte("foo"), []byte("bar"))
	// 	b.PutWithTTL([]byte("ayaz"), []byte("bar"), time.Second*30)
	// 	b.Put([]byte("riz"), []byte("bar"))
	// 	b.Put([]byte("b3"), []byte("bar"))
	// 	b.Delete([]byte("foo"))
	// 	b.Delete([]byte("b3"))
	// 	b.Write()
	// 	return err
	// })
	// if err != nil {
	// 	log.Fatal(err)
	// 	return
	// }

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
