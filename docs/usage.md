# unitdb [![GoDoc](https://godoc.org/github.com/unit-io/unitdb?status.svg)](https://pkg.go.dev/github.com/unit-io/unitdb) [![Go Report Card](https://goreportcard.com/badge/github.com/unit-io/unitdb)](https://goreportcard.com/report/github.com/unit-io/unitdb)

The unitdb is blazing fast time-series database for IoT, realtime messaging  applications. Unitdb can be used for online gaming and mobile apps as it satisfy the requirements for low latency and binary messaging. The unitdb is a perfect time-series database for applications such as internet of things and internet connected devices.

# About unitdb 

## Key characteristics
- 100% Go
- Optimized for fast lookups and writes
- Can store larger-than-memory data sets
- Data is safely written to disk with accuracy and high performant block sync technique
- Supports opening database with immutable flag
- Supports database encryption
- Supports time-to-live on message entry
- Supports writing to wildcard topics
- Queried data is returned complete and correct

## Table of Contents
 * [Quick Start](#Quick-Start)
 * [Usage](#Usage)
 * [Opening a database](#Opening-a-database)
 + [Writing to a database](#Writing-to-a-database)
   - [Store a message](#Store-a-message)
   - [Store a message](#Store-bulk-messages)
   - [Specify ttl](#Specify-ttl)
   - [Read messages](#Read-messages)
   - [Deleting a message](#Deleting-a-message)
   - [Topic isolation](#Topic-isolation)
 + [Batch operation](#Batch-operation)
   - [Writing to a batch](#Writing-to-a-batch)
   - [Writing to multiple topics in a batch](#Writing-to-multiple-topics-in-a-batch)
   - [Non-blocking batch operation](#Non-blocking-batch-operation)
 * [Iterating over items](#Iterating-over-items)
 + [Advanced](#Advanced)
   - [Writing to wildcard topics](#Writing-to-wildcard-topics)
   - [Topic isolation in batch operation](#Topic-isolation-in-batch-operation)
   - [Message encryption](#Message-encryption)
   - [Batch group](#Batch-group)
 * [Statistics](#Statistics)

## Quick Start
To build unitdb from source code use go get command.

> go get -u github.com/unit-io/unitdb

## Usage

### Opening a database
To open or create a new database, use the unitdb.Open() function:

```

	package main

	import (
		"log"

		"github.com/unit-io/unitdb"
	)

	func main() {
		// Opening a database.
		opts := &unitdb.Options{BufferSize: 1 << 27, MemdbSize: 1 << 32, LogSize: 1 << 30}
		// Open DB with Mutable flag to allow DB.Delete operation
		db, err := unitdb.Open("unitdb", opts, unitdb.WithMutable())
		if err != nil {
			log.Fatal(err)
			return
		}	
		defer db.Close()
	}

```

### Writing to a database

#### Store a message
Use DB.Put() or DB.PutEntry() to store message to a topic. You can send messages to specific topic or wildcard topics.

```

	topic := []byte("teams.alpha.ch1")
	msg := []byte("msg for team alpha channel1")
	db.Put(topic, msg)

	or

	// send message to all receivers of channel1 for team alpha
	topic := []byte("teams.alpha.ch1.*")
	msg := []byte("msg for team alpha channel1 receivers")
	db.Put(topic, msg)

	// send message to all channels for team alpha
	topic := []byte("teams.alpha...")
	msg := []byte("msg for team alpha all channels")
	db.Put(topic, msg)

```

#### Store bulk messages
Use DB.SetEntry() method to bulk store messages as topic is parsed on first request and subsequent requests skips parsing.

```
	topic := []byte("teams.alpha.ch1.u1")
	entry := &unitdb.Entry{Topic: []byte("teams.alpha.ch1.u1?ttl=1h")}
	for j := 0; j < 50; j++ {
		db.SetEntry(entry, []byte(fmt.Sprintf("msg for team alpha channel1 receiver1 #%2d", j)))
	}

```

#### Specify ttl 
Specify ttl parameter to a topic while storing messages to expire it after specific duration. 

```
	topic := []byte("teams.alpha.ch1.u1?ttl=1h")
	msg := []byte("msg for team alpha channel1 receiver1")
	b.PutEntry(unitdb.NewEntry(topic, msg))

```

#### Read messages
UUse DB.Get() to read messages from a topic. Use last parameter to specify duration or specify number of recent messages to read from a topic. for example, "last=1h" gets messages from unitdb stored in last 1 hour, or "last=100" to get last 100 messages from unitdb. Specify an optional parameter Query.Limit to retrieve messages from a topic with a limit.

```

	var err error
	var msg [][]byte
	msgs, err = db.Get(&unitdb.Query{Topic: []byte("teams.alpha.ch1?last=100")})
    ....
	msgs, err = db.Get(&unitdb.Query{Topic: []byte("teams.alpha.ch1.u1?last=1h", Limit: 100}))

```

#### Deleting a message
Deleting a message in unitdb is rare and it require additional steps to delete message from a given topic. Generate a unique message ID using DB.NewID() and use this unique message ID while putting message to the unitdb using DB.PutEntry(). To delete message provide message ID to the DB.DeleteEntry() function. If Immutable flag is set when DB is open then DB.DeleteEntry() returns an error.

```

	messageId := db.NewID()
	err := db.PutEntry(&unitdb.Entry{
		ID:       messageId,
		Topic:    []byte("teams.alpha.ch1.u1"),
		Payload:  []byte("msg for team alpha channel1 receiver1"),
	})
	
	err := db.DeleteEntry(&unitdb.Entry{
		ID:       messageId,
		Topic:    []byte("teams.alpha.ch1.u1"),
	})

```

#### Topic isolation
Topic isolation can be achieved using Contract while putting messages into unitdb or querying messages from a topic. Use DB.NewContract() to generate a new Contract and then specify Contract while putting messages using DB.PutEntry() method. Use Contract in the query to get messages from a topic specific to the contract.

```
	contract, err := db.NewContract()

	err := db.PutEntry(&unitdb.Entry{
		Topic:    []byte("teams.alpha.ch1"),
		Payload:  []byte("msg for team alpha channel1"),
		Contract: contract,
	})
	....
	msgs, err := db.Get(&unitdb.Query{Topic: []byte("teams.alpha.ch1?last=1h", Contract: contract, Limit: 100}))

```

### Batch operation
Use batch operation to bulk insert records into unitdb or bulk delete records from unitdb. See benchmark examples and run it locally to see performance of running batches concurrently.

#### Writing to a batch
Use Batch.Put() to write to a single topic in a batch.

```
	// Writing to single topic in a batch
	err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		opts := unitdb.DefaultBatchOptions
		opts.Topic = []byte("teams.alpha.ch1.*?ttl=1h")
		b.SetOptions(opts)
		b.Put([]byte("msg for team alpha channel1 all receivers"))
		return b.Write()
    })

```

#### Writing to multiple topics in a batch
Use Batch.PutEntry() function to store messages to multiple topics in a batch.

```

    // Writing to multiple topics in a batch
    err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("teams.alpha.ch1.u1"), []byte("msg for team alpha channel1 receiver1")))
		b.PutEntry(unitdb.NewEntry([]byte("teams.alpha.ch1.u2"), []byte("msg for team alpha channel1 receiver2")))
		return b.Write()
    })

```

#### Non-blocking batch operation
All batch operations are non-blocking so client program can decide to wait for completed signal and further execute any additional tasks.

```
    err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("teams.alpha.ch1.u1"), []byte("msg for team alpha channel1 receiver1")))
		err := b.Write()
			go func() {
				<-completed // it signals batch has completed and fully committed to db
				...
			}()
		return err
    })

```

### Iterating over items
Use the DB.Items() function which returns a new instance of ItemIterator. 
Specify topic to retrieve values and use last parameter to specify duration or specify number of recent messages to retrieve from the topic. for example, "last=1h" retrieves messages from unitdb stored in last 1 hour, or "last=100" to retrieves last 100 messages from the unitdb:

```

	topic := "teams.alpha.ch1.u1?last=1h"
	it, err := db.Items(&unitdb.Query{Topic: topic})
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

```

### Advanced

#### Writing to wildcard topics
unitdb supports writing to wildcard topics. Use "`*`" in the topic to write to wildcard topic or use "`...`" at the end of topic to write to all sub-topics. Writing to following wildcard topics are also supported, "`*`" or "`...`"

```
	b.PutEntry(unitdb.NewEntry([]byte("teams.*.ch1"), []byte("msg for any team channel1")))
	b.PutEntry(unitdb.NewEntry([]byte("teams.alpha.*"), []byte("msg for team alpha all channels")))
	b.PutEntry(unitdb.NewEntry([]byte("teams..."), []byte("msg for all teams all channels")))
	b.PutEntry(unitdb.NewEntry([]byte("..."), []byte("msg broadcast to all receivers of all teams all channels")))

```

#### Topic isolation in batch operation
Topic isolation can be achieved using Contract while putting messages into unitdb and querying messages from a topic. Use DB.NewContract() to generate a new Contract and then specify Contract while putting messages using Batch.PutEntry() function.

```
	contract, err := db.NewContract()

    // Writing to single topic in a batch
	err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		opts := unitdb.DefaultBatchOptions
		opts.Topic = []byte("teams.alpha.ch1.*?ttl=1h")
		opts.Contract = contract
		b.SetOptions(opts)
		b.Put([]byte("msg for team alpha channel1 all receivers #1"))
		b.Put([]byte("msg for team alpha channel1 all receivers #2"))
		b.Put([]byte("msg for team alpha channel1 all receivers #3"))
		return b.Write()
    })

    // Writing to multiple topics in a batch
    err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		opts := unitdb.DefaultBatchOptions
		opts.Contract = contract
		b.SetOptions(opts)
		b.PutEntry(unitdb.NewEntry([]byte("teams.*.ch1"), []byte("msg for any team channel1")))
		b.PutEntry(unitdb.NewEntry([]byte("teams.alpha.*"), []byte("msg for team alpha all channels")))
		b.PutEntry(unitdb.NewEntry([]byte("teams..."), []byte("msg for all teams all channels")))
		b.PutEntry(unitdb.NewEntry([]byte("..."), []byte("msg broadcast to all receivers of all teams all channels")))
		return b.Write()
	})

```

#### Message encryption
Set encryption flag in batch options to encrypt all messages in a batch. 

Note, encryption can also be set on entire database using DB.Open() and set encryption flag in options parameter. 

```
	err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		opts := unitdb.DefaultBatchOptions
		opts.Encryption = true
		opts.Topic = []byte("teams.alpha.ch1?ttl=1h")
		b.SetOptions(opts)
		b.Put([]byte("msg for team alpha channel1"))
		err := b.Write()
		return err
	})

```

#### Batch group
Use BatchGroup.Add() function to group batches and run concurrently without causing write conflict. Use the BatchGroup.Run to run group of batches concurrently. See usage example in below code snippet.

```
    g := db.NewBatchGroup()
	g.Add(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("teams.alpha.ch1?ttl=1h"), []byte("msg for team alpha channel1 #1")))
		b.PutEntry(unitdb.NewEntry([]byte("teams.beta.ch1?ttl=1h"), []byte("msg for team beta channel1 #1")))
		return b.Write()
	})

	g.Add(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("teams.alpha.ch1?ttl=1h"), []byte("msg for team alpha channel1 #2")))
		b.PutEntry(unitdb.NewEntry([]byte("teams.beta.ch1?ttl=1h"), []byte("msg for team beta channel1 #2")))
		return b.Write()
	})

	g.Add(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("teams.alpha.ch1?ttl=1h"), []byte("msg for team alpha channel1 #3")))
		b.PutEntry(unitdb.NewEntry([]byte("teams.beta.ch1?ttl=1h"), []byte("msg for team beta channel1 #3")))
		return b.Write()
	})

	err = g.Run()

```

### Statistics
The unitdb keeps a running metrics of internal operations it performs. To get unitdb metrics use DB.Varz() function.

```

	if varz, err := db.Varz(); err == nil {
		fmt.Printf("%+v\n", varz)
	}

```

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
Copyright (c) 2016-2020 Saffat IT Solutions Pvt Ltd. This project is licensed under [Apache-2.0 License](https://github.com/unit-io/unitdb/blob/master/LICENSE).
