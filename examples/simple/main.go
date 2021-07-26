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

	topic := []byte("teams.private.sales.saleschannel.message")
	msg := []byte("msg for sales channel")
	db.Put(topic, msg)

	// Send message to all channels in team sales
	topic = []byte("teams.private.sales.*.message")
	msg = []byte("msg for team alpha channel1 all recipients")
	db.Put(topic, msg)

	// Send message to all private teams
	topic = []byte("teams.private...")
	msg = []byte("msg for team alpha all channels")
	db.Put(topic, msg)

	// Get message for sales channel
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.private.sales.saleschannel.message?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Delete message
	messageId := db.NewID()
	entry := unitdb.NewEntry([]byte("teams.public.customersupport.connectchannel.message"), []byte("msg for customer connect channel")).WithID(messageId)
	db.PutEntry(entry)

	db.Sync()

	err = db.DeleteEntry(unitdb.NewEntry([]byte("teams.public.customersupport.connectchannel.message"), nil).WithID(messageId))
	if err != nil {
		log.Fatal(err)
		return
	}

	// Get message for customer connect channel
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.public.customersupport.connectchannel.message?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Topic isolation using contract
	contract, err := db.NewContract()

	db.PutEntry(unitdb.NewEntry([]byte("teams.public.customersupport.connectchannel.message"), []byte("msg for customer connect channel")).WithContract(contract))

	// Get message for customer connect chanel with new contract
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.public.customersupport.connectchannel.message?last=1h")).WithContract(contract).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}
}
