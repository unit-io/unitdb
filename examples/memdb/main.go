package main

import (
	"fmt"
	"log"
	"time"

	"github.com/unit-io/unitdb/memdb"
)

func main() {
	// Opening a database.
	db, err := memdb.Open(memdb.WithLogFilePath("unitdb"))
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	gets := func(start, end int) {
		for i := start; i <= end; i++ {
			// Get message
			if msg, err := db.Get(uint64(i)); err == nil {
				log.Printf("%s ", msg)
			}
		}
	}

	fmt.Println("main: Print recovered entries on DB open")
	gets(1, 10)

	puts := func(start, end int) {
		for i := start; i <= end; i++ {
			val := []byte(fmt.Sprintf("msg.%2d", i))
			db.Put(uint64(i), val)
		}
		time.Sleep(100 * time.Millisecond)
	}

	deletes := func(start, end int) {
		for i := start; i <= end; i++ {
			if err := db.Delete(uint64(i)); err != nil {
				fmt.Println("error: ", err)
			}
		}
	}

	batch := func(start, end int) {
		err = db.Batch(func(b *memdb.Batch, completed <-chan struct{}) error {
			for i := start; i <= end; i++ {
				val := []byte(fmt.Sprintf("msg.%2d", i))
				b.Put(uint64(i), val)
			}
			return nil
		})
	}

	fmt.Println("main: Put 1-3")
	puts(1, 3)
	gets(1, 5)
	fmt.Println("main: Delete 1-2")
	deletes(1, 2)
	fmt.Println("main: Put 4-5")
	puts(4, 5)
	fmt.Println("main: Delete 3-5")
	deletes(3, 5)
	fmt.Println("main: Batch Put 4-10")
	batch(4, 10)
	fmt.Println("main: Delete 4-5")
	deletes(4, 5)
	gets(1, 10)
	// print stats
	if varz, err := db.Varz(); err == nil {
		fmt.Printf("%+v\n", varz)
	}
}
