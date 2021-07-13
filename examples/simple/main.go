package main

import (
	"log"

	"github.com/unit-io/unitdb"
)

func main() {
	// Opening a database.
	// Open DB with Mutable flag to allow deleting messages
	db, err := unitdb.Open("example", unitdb.WithDefaultOptions(), unitdb.WithMutable())
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	topic := []byte("teams.alpha.ch1")
	msg := []byte("msg for team alpha channel1")
	db.Put(topic, msg)

	// Send message to all recipients of channel1 in team alpha
	topic = []byte("teams.alpha.ch1.*")
	msg = []byte("msg for team alpha channel1 all recipients")
	db.Put(topic, msg)

	// Send message to all channels in team alpha
	topic = []byte("teams.alpha...")
	msg = []byte("msg for team alpha all channels")
	db.Put(topic, msg)

	// Get message for team alpha channel1
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.alpha.ch1?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Get message for team alpha channel1 recipient1
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.alpha.ch1.r1?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Get message for team alpha channel2
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.alpha.ch2?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Specify time to live so if you run the program again after 1 min you will not receive this message
	topic = []byte("teams.alpha.ch1.r1?ttl=1m")
	msg = []byte("msg with 1m ttl for team alpha channel1 recipient1")
	db.Put(topic, msg)

	// Delete message
	messageId := db.NewID()
	entry := unitdb.NewEntry([]byte("teams.alpha.ch1.r1"), []byte("msg for team alpha channel1 recipient1")).WithID(messageId)
	db.PutEntry(entry)

	db.Sync()

	err = db.DeleteEntry(unitdb.NewEntry([]byte("teams.alpha.ch1.r1"), nil).WithID(messageId))
	if err != nil {
		log.Fatal(err)
		return
	}

	// Get message for team alpha channel1 recipient1
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.alpha.ch1.r1?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Topic isolation using contract
	contract, err := db.NewContract()

	db.PutEntry(unitdb.NewEntry([]byte("teams.alpha.ch1.r1"), []byte("msg for team alpha channel1 recipient1")).WithContract(contract))

	// Get message for team alpha channel1 recipient1 with new contract
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.alpha.ch1.r1?last=1h")).WithContract(contract).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}
}
