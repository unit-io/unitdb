# unitdb: blazing fast time-series database for IoT and real-time messaging applications

unitdb is blazing fast time-series database for IoT, realtime messaging  applications. 

The unitdb performs millions of writes in sub-second and it can out perform any similar time-series databases for IoT, realtime applications in its write performance.

# Key characteristics
- 100% Go.
- Optimized for fast lookups and bulk inserts.
- Can store larger-than-memory data sets.
- Entire database can run in memory backed with file storage if system memory is larger than data sets. 
- All DB methods are safe for concurrent use by multiple goroutines.

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
		opts := &unitdb.Options{BufferSize: 1 << 27, MemdbSize: 1 << 32, LogSize: 1 << 30, MinimumFreeBlocksSize: 1 << 27}
		// Flag: 1 - Set or -1 - Unset
		flags := &unitdb.Flags{Immutable: 1, Encryption: -1, BackgroundKeyExpiry: -1}
		db, err := unitdb.Open("unitdb.example", flags, opts)
		if err != nil {
			log.Fatal(err)
			return
		}	
		defer db.Close()
	}

```

### Writing to a database

#### Store a message
Use DB.Put() to store message to a topic or use DB.PutEntry() to store message entry to a topic. DB.PutEntry() allows client to specify ID and Contract parameters. See topic isolation section for more detail. 

```

	topic := []byte("unit8.b.b1")
	msg := []byte("msg.b.b1.1")
	db.Put(topic, msg)

	or
	
	db.PutEntry(unitdb.NewEntry(topic, msg))

```

#### Store bulk messages
Use Entry.SetPayload() method to store bulk messages and then use DB.PutEntry() method for better performance as topic is parsed on first entry into DB and subsequent entries skips parsing of the topic.

```
	topic := []byte("unit8.b.b1")
	entry := &unitdb.Entry{Topic: topic}
	for j := uint8(0); j < 50; j++ {
		entry.SetPayload(append([]byte("msg."), j))
		db.PutEntry(entry)
	}

```

#### Specify ttl 
Specify ttl parameter to a topic while storing messages to expire it after specific duration. DB query does not fetch expired messages. 

```
	topic := []byte("unit8.b.b1?ttl=1h")
	msg := []byte("msg.b.b1.1")
	b.PutEntry(unitdb.NewEntry(topic, msg))

```

#### Read messages
Use DB.Get() to read messages from a topic. Use last parameter to specify duration or specify number of recent messages to read from a topic. for example, "last=1h" gets messages from unitdb stored in last 1 hour, or "last=100" to get last 100 messages from unitdb. Specify an optional parameter Query.Limit to retrieve messages from a topic with a limit.

```

	var err error
	var msg [][]byte
	msgs, err = db.Get(&unitdb.Query{Topic: []byte("unit8.b.b1?last=100")})
    ....
	msgs, err = db.Get(&unitdb.Query{Topic: []byte("unit8.b.b1?last=1h", Limit: 100}))

```

#### Deleting a message
Deleting a message in unitdb is rare and it require additional steps to delete message from a given topic. Generate a unique message ID using DB.NewID() and use this unique message ID while putting message to the unitdb using DB.PutEntry(). To delete message provide message ID to the DB.DeleteEntry() function. If Immutable flag is set when DB is open then DB.DeleteEntry() returns an error.

```

	messageId := db.NewID()
	err := db.PutEntry(&unitdb.Entry{
		ID:       messageId,
		Topic:    []byte("unit8.b.b1"),
		Payload:  []byte("msg.b.b1.deleting"),
	})
	
	err := db.DeleteEntry(&unitdb.Entry{
		ID:       messageId,
		Topic:    []byte("unit8.b.b1"),
	})

```

#### Topic isolation
Topic isolation can be achieved using Contract while putting messages into unitdb or querying messages from a topic. Use DB.NewContract() to generate a new Contract and then specify Contract while putting messages using DB.PutEntry() function. Use Contract in the query to get messages from a topic.

```
	contract, err := db.NewContract()

	messageId := db.NewID()
	err := db.PutEntry(&unitdb.Entry{
		ID:       messageId,
		Topic:    []byte("unit8.b.b1"),
		Payload:  []byte("msg.b.b1.1"),
		Contract: contract,
	})
	....
	msgs, err := db.Get(&unitdb.Query{Topic: []byte("unit8.b.b1?last=1h", Contract: contract, Limit: 100}))

```

### Batch operation
Use batch operation to bulk insert records into unitdb or bulk delete records from unitdb. Batch operation also speeds up reading data if batch operation is used then reading records within short span of time while db is still open. See benchmark examples and run it locally to see performance of running batches concurrently.

#### Writing to a batch
Use Batch.Put() to write to a single topic in a batch. To write to single topic in a batch specify topic in batch options.

```
	// Writing to single topic in a batch
	err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		opts := unitdb.DefaultBatchOptions
		opts.Topic = []byte("unit8.b.*?ttl=1h")
		b.SetOptions(opts)
		b.Put([]byte("msg.b.*.1"))
		err := b.Write()
		return err
    })

```

#### Writing to multiple topics in a batch
Use Batch.PutEntry() function to store messages to multiple topics in a batch.

```

    // Writing to multiple topics in a batch
    err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b1"), []byte("msg.b.b1.1")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b11"), []byte("msg.b.b11.1")))
		err := b.Write()
		return err
    })

```

#### Non-blocking batch operation
All batch operations are non-blocking so client program can decide to wait for completed signal and further execute any additional tasks.

```
    err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b1"), []byte("msg.b.b11.1")))
		err := b.Write()
			go func() {
				<-completed // it signals batch has completed and fully committed to db
				print([]byte("unit8.b.b1?last=1h"), db)
			}()
		return err
    })

```

### Iterating over items
Use the DB.Items() function which returns a new instance of ItemIterator. 
Specify topic to retrieve values and use last parameter to specify duration or specify number of recent messages to retrieve from the topic. for example, "last=1h" retrieves messages from unitdb stored in last 1 hour, or "last=100" to retrieve last 100 messages from the unitdb:

```

	func print(topic []byte, db *unitdb.DB) {
		// topic -> "unit8.b.b1?last=1h"
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
}

```

### Advanced

#### Writing to wildcard topics
unitdb supports writing to wildcard topics. Use "`*`" in the topic to write to wildcard topic or use "`...`" at the end of topic to write to all sub-topics. Writing to following wildcard topics are also supported, "`*`" or "`...`"

```
	b.PutEntry(unitdb.NewEntry([]byte("unit8.*.b1"), []byte("msg.*.b1.1")))
	b.PutEntry(unitdb.NewEntry([]byte("unit8.b.*"), []byte("msg.b.*.1")))
	b.PutEntry(unitdb.NewEntry([]byte("unit8..."), []byte("msg...1")))
	b.PutEntry(unitdb.NewEntry([]byte("*"), []byte("msg.*.1")))
	b.PutEntry(unitdb.NewEntry([]byte("..."), []byte("msg...1")))

```

#### Topic isolation in batch operation
Topic isolation can be achieved using Contract while putting messages into unitdb and querying messages from a topic. Use DB.NewContract() to generate a new Contract and then specify Contract while putting messages using Batch.PutEntry() function.

```
	contract, err := db.NewContract()

    // Writing to single topic in a batch
	err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		opts := unitdb.DefaultBatchOptions
		opts.Topic = []byte("unit8.b.*?ttl=1h")
		opts.Contract = contract
		b.SetOptions(opts)
		b.Put([]byte("msg.b.*.1"))
		b.Put([]byte("msg.b.*.2"))
		b.Put([]byte("msg.b.*.3"))
		return b.Write()
    })

    // Writing to multiple topics in a batch
    err := db.Batch(func(b *unitdb.Batch, completed <-chan struct{}) error {
		opts := unitdb.DefaultBatchOptions
		opts.Contract = contract
		b.SetOptions(opts)
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.*"), []byte("msg.b.*.1")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8..."), []byte("msg...")))
		b.PutEntry(unitdb.NewEntry([]byte("*"), []byte("msg.*.1")))
		b.PutEntry(unitdb.NewEntry([]byte("..."), []byte("msg...1")))
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
		opts.Topic = []byte("unit8.b.b1?ttl=1h")
		b.SetOptions(opts)
		b.Put([]byte("msg.b.b1.1"))
		err := b.Write()
		return err
	})

```

#### Batch group
Use BatchGroup.Add() function to group batches and run concurrently without causing write conflict. Use the BatchGroup.Run to run group of batches concurrently. See usage example in below code snippet.

```
    g := db.NewBatchGroup()
	g.Add(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b1?ttl=2h"), []byte("msg.b.b1.1")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.c.c1?ttl=1h"), []byte("msg.c.c1.1")))
		return b.Write()
	})

	g.Add(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b1"), []byte("msg.b.b1.2")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.c.c1"), []byte("msg.c.c1.2")))
		return b.Write()
	})

	g.Add(func(b *unitdb.Batch, completed <-chan struct{}) error {
		b.PutEntry(unitdb.NewEntry([]byte("unit8.b.b1"), []byte("msg.b.b1.3")))
		b.PutEntry(unitdb.NewEntry([]byte("unit8.c.c1"), []byte("msg.c.c1.3")))
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