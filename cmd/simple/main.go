package main

import (
	"log"

	"github.com/unit-io/unitdb"
)

func main() {
	// Opening a database.
	opts := &unitdb.Options{BufferSize: 1 << 27, MemdbSize: 1 << 32, LogSize: 1 << 30, MinimumFreeBlocksSize: 1 << 27}
	// open DB with Mutable flag to allow deleting messages
	db, err := unitdb.Open("example", opts, unitdb.WithMutable())
	if err != nil {
		log.Fatal(err)
		return
	}
	defer db.Close()

	topic := []byte("teams.alpha.ch1")
	msg := []byte("msg for team alpha channel1")
	db.Put(topic, msg)

	// send message to all receivers of channel1 for team alpha
	topic = []byte("teams.alpha.ch1.*")
	msg = []byte("msg for team alpha channel1 all receivers")
	db.Put(topic, msg)

	// send message to all channels for team alpha
	topic = []byte("teams.alpha...")
	msg = []byte("msg for team alpha all channels")
	db.Put(topic, msg)

	// Get message for team alpha channel1
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.alpha.ch1?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Get message for team alpha channel1 receiver1
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.alpha.ch1.u1?last=1h")).WithLimit(10)); err == nil {
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

	// specify time to live so if you run the program again after 1 min you will not receive this messages
	topic = []byte("teams.alpha.ch1.u1?ttl=1m")
	msg = []byte("msg with 1m ttl for team alpha channel1 receiver1")
	db.Put(topic, msg)

	// delete message
	messageId := db.NewID()
	entry := unitdb.NewEntry([]byte("teams.alpha.ch1.u1")).WithID(messageId).WithPayload([]byte("msg for team alpha channel1 receiver1"))
	db.PutEntry(entry)

	err = db.DeleteEntry(unitdb.NewEntry([]byte("teams.alpha.ch1.u1")).WithID(messageId))
	if err != nil {
		log.Fatal(err)
		return
	}

	// Get message for team alpha channel1 receiver1
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.alpha.ch1.u1?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Topic isolation using contract
	contract, err := db.NewContract()

	db.PutEntry(unitdb.NewEntry([]byte("teams.alpha.ch1.u1")).WithPayload([]byte("msg for team alpha channel1 receiver1")).WithContract(contract))

	// Get message for team alpha channel1 receiver1 with new contract
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.alpha.ch1.u1?last=1h")).WithContract(contract).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// iterating over items
	topic = []byte("teams.alpha.ch1.u1?ttl=1m")
	it, err := db.Items(unitdb.NewQuery(topic))
	if err != nil {
		log.Fatal(err)
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
