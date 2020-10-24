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

	gets := func() {
		// Get message
		if msg, err := db.Get(1); err == nil {
			log.Printf("%s ", msg)
		}

		// Get message
		if msg, err := db.Get(2); err == nil {
			log.Printf("%s ", msg)
		}

		// Get message
		if msg, err := db.Get(3); err == nil {
			log.Printf("%s ", msg)
		}
	}

	fmt.Println("main: Print recovered entries on DB open")
	gets()

	sets := func() {
		msg := []byte("teams.alpha.ch1")
		db.Set(1, msg)

		msg = []byte("teams.alpha.ch1.*")
		db.Set(2, msg)

		msg = []byte("teams.alpha...")
		db.Set(3, msg)
		time.Sleep(100 * time.Millisecond)
	}

	deletes := func() {
		if err := db.Delete(1); err != nil {
			fmt.Println("error: ", err)
		}
		if err := db.Delete(2); err != nil {
			fmt.Println("error: ", err)
		}
		if err := db.Delete(3); err != nil {
			fmt.Println("error: ", err)
		}
	}
	sets()
	deletes()
	fmt.Println("main: Print entries after delete")
	gets()
	sets()
	fmt.Println("main: Print entries after set")
	gets()
}
