package main

import (
	"fmt"
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

	// Use Entry.WithPayload() method to bulk store messages as topic is parsed one time on first request.
	topic := []byte("teams.private.sales.ch1.message")
	entry := &unitdb.Entry{Topic: topic}
	for j := 0; j < 50; j++ {
		db.PutEntry(entry.WithPayload([]byte(fmt.Sprintf("msg for sales team channel1 #%2d", j))))
	}

	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.private.sales.ch1.message?last=1h")).WithLimit(100)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Writing to single topic in a batch
	err = db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		topic := []byte("teams.private.sales.*.message?ttl=1h")
		b.Put(topic, []byte("msg for sales team all channels"))
		return nil
	})
	if err != nil {
		log.Fatal(err)
		return
	}

	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.private.sales.ch2.message?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.private.sales.ch3.message?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Writing to multiple topics in a batch
	err = db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("teams.private.sales.ch2.message"), []byte("msg for sales team channel2")))
		b.PutEntry(unitdb.NewEntry([]byte("teams.private.sales.ch3.message"), []byte("msg for sales team channel3")))
		return nil
	})
	if err != nil {
		log.Fatal(err)
		return
	}

	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.private.sales.ch2.message?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.private.sales.ch3.message?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Topic isolation can be achieved using Contract while putting messages into unitdb and querying messages from a topic.
	// Use DB.NewContract() to generate a new Contract and then specify Contract while putting messages using Batch.PutEntry() function.
	contract, err := db.NewContract()

	// Writing to single topic in a batch
	err = db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.SetOptions(unitdb.WithBatchContract(contract))
		topic := []byte("teams.private.sales.*.message?ttl=1h")
		b.Put(topic, []byte("msg #1 for sales team all channels"))
		b.Put(topic, []byte("msg #2 for sales team all channels"))
		b.Put(topic, []byte("msg #3 for sales team all channels"))
		return nil
	})

	// Writing to multiple topics in a batch
	err = db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.SetOptions(unitdb.WithBatchContract(contract))
		b.PutEntry(unitdb.NewEntry([]byte("teams.private.*.ch1.message"), []byte("msg for channel1 in any teams")))
		b.PutEntry(unitdb.NewEntry([]byte("teams.private.sales.*.message"), []byte("msg for all channels in sales team")))
		b.PutEntry(unitdb.NewEntry([]byte("teams.private..."), []byte("msg for all private teams and all channels")))
		b.PutEntry(unitdb.NewEntry([]byte("..."), []byte("msg broadcast to all channels in all teams")))
		return nil
	})
	if err != nil {
		log.Fatal(err)
		return
	}

	// Get message for sales team channel1
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.private.sales.ch1.message?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Get message for customer support team channel1
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.public.customersupport.ch1.message?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Get message for customer support team channel2
	if msgs, err := db.Get(unitdb.NewQuery([]byte("teams.public.customersupport.ch2.message?last=1h")).WithLimit(10)); err == nil {
		for _, msg := range msgs {
			log.Printf("%s ", msg)
		}
	}

	// Set encryption flag in batch options to encrypt all messages in a batch.
	// Note, encryption can also be set on entire database using DB.Open() and set encryption flag in options parameter.
	err = db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.SetOptions(unitdb.WithBatchEncryption())
		topic := []byte("teams.private.sales.ch1.message?ttl=1h")
		b.Put(topic, []byte("msg for sales team channel1"))
		return nil
	})

	if err != nil {
		log.Fatal(err)
		return
	}
}
