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
		for i := start; i < end; i++ {
			// Get message
			if msg, err := db.Get(uint64(i)); err == nil {
				log.Printf("%s ", msg)
			}
		}
	}

	fmt.Println("main: Print recovered entries on DB open")
	gets(1, 10)

	sets := func(start, end int) {
		for i := start; i < end; i++ {
			val := []byte(fmt.Sprintf("msg.%2d", i))
			db.Set(uint64(i), val)
		}
		time.Sleep(100 * time.Millisecond)
	}

	deletes := func(start, end int) {
		for i := start; i < end; i++ {
			if err := db.Delete(uint64(i)); err != nil {
				fmt.Println("error: ", err)
			}
		}
	}

	batch := func(start, end int) {
		err = db.Batch(func(b *memdb.Batch, completed <-chan struct{}) error {
			for i := start; i < end; i++ {
				val := []byte(fmt.Sprintf("msg.%2d", i))
				b.Append(uint64(i), val)
			}
			return nil
		})
	}

	sets(1, 3)
	fmt.Println("main: Print entries after set")
	gets(1, 3)
	deletes(1, 2)
	batch(4, 10)
	gets(1, 10)
	deletes(4, 5)
	time.Sleep(100 * time.Millisecond)
}
