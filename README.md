# tracedb [![GoDoc](https://godoc.org/github.com/unit-io/tracedb?status.svg)](https://godoc.org/github.com/unit-io/tracedb) [![Go Report Card](https://goreportcard.com/badge/github.com/unit-io/tracedb)](https://goreportcard.com/report/github.com/unit-io/tracedb) [![Coverage Status](https://coveralls.io/repos/github/unit-io/tracedb/badge.svg?branch=master)](https://coveralls.io/github/unit-io/tracedb?branch=master)

<p align="left">
  <img src="tracedb.png" width="70" alt="Trace" title="tracedb: Blazing fast timeseries database fro IoT and real-time messaging applications"> 
</p>

# tracedb: blazing fast timeseries database for IoT and real-time messaging application

tracedb is blazing fast timeseries database for IoT, realtime messaging  application. Access tracedb with pubsub over tcp or websocket using [trace](https://github.com/unit-io/trace) application.

Tracedb can be used for online gaming and mobile apps as it satisfy the requirements for low latency and binary messaging. Tracedb is perfect timeseries data store for applications such as internet of things and internet connected devices.

[unitdb](https://github.com/unit-io/unitdb) repo is forked from tracedb for more advanced use case for timeseries database. Keep watch on [unitdb](https://github.com/unit-io/unitdb)

# Key characteristics
- 100% Go.
- Optimized for fast lookups and bulk inserts.
- Can store larger-than-memory data sets.
- Entire database can run in memory backed with file storage if system memory is larger than data sets. 
- All DB methods are safe for concurrent use by multiple goroutines.

# Planned
- Documentation - document the technical atchitecture, technical design and advanced usage guides.

## Table of Contents
 * [Quick Start](#Quick-Start)
 * [Usage](#Usage)
 * [Opening a database](#Opening-a-database)
 + [Writing to a database](#Writing-to-a-database)
   - [Store a message](#Store-a-message)
   - [Writing to wildcard topics](#Writing-to-wildcard-topics)
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
   - [Writing batch chunk](#Writing-batch-chunk)
   - [Topic isolation in batch operation](#Topic-isolation-in-batch-operation)
   - [Message encryption](#Message-encryption)
   - [Batch group](#Batch-group)
 * [Statistics](#Statistics)

## Quick Start
To build tracedb from source code use go get command.

> go get -u github.com/unit-io/tracedb

## Usage

### Opening a database

To open or create a new database, use the tracedb.Open() function:

```
package main

import (
	"log"

	"github.com/unit-io/tracedb"
)

func main() {
    db, err := tracedb.Open("tracedb.example", nil)
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

	err := db.Put([]byte("unit8.b.b1"), []byte("msg.b.b1.1"))

	err := db.PutEntry(tracedb.NewEntry([]byte("unit8.b.b1"),[]byte("msg.b.b1.2")))

```

#### Writing to wildcard topics
tracedb supports wrting to wildcard topics. Use "`*`" in the topic to write to wildcard topic or use "`...`" at the end of topic to write to all sub-topics. Writing to following wildcard topics are also supported, "`*`" or "`...`"

```
	b.PutEntry(tracedb.NewEntry([]byte("unit8.*.b11"), []byte("msg.*.b11.1")))
	b.PutEntry(tracedb.NewEntry([]byte("unit8.b.*"), []byte("msg.b.*.1")))
	b.PutEntry(tracedb.NewEntry([]byte("unit8..."), []byte("msg...1")))
	b.PutEntry(tracedb.NewEntry([]byte("*"), []byte("msg.*.1")))
	b.PutEntry(tracedb.NewEntry([]byte("..."), []byte("msg...1")))

```

#### Specify ttl 
Specify ttl parameter to a topic while storing messages to expire it after specific duration. 
Note, DB.Get() or DB.Items() function does not fetch expired messages. 

```
	b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b1?ttl=1h"), []byte("msg.b.b1.1")))

```

#### Read messages
Use DB.Get() to read messages from a topic. Use last parameter to specify duration or specify number of recent messages to read from a topic. for example, "last=1h" gets messsages from tracedb stored in last 1 hour, or "last=100" to gets last 100 messages from tracedb. Specify an optional parameter Query.Limit to retrives messages from a topic with a limit.

```

	msgs, err := db.Get(&tracedb.Query{Topic: []byte("unit8.b.b1?last=100")})
    ....
	msgs, err := db.Get(&tracedb.Query{Topic: []byte("unit8.b.b1?last=1h", Limit: 100}))

```

#### Deleting a message
Deleting a message in tracedb is rare and it require additional steps to delete message from a given topic. Generate a unique message ID using DB.NewID() and use this unique message ID while putting message to the tracedb using DB.PutEntry(). To delete message provide message ID to the DB.DeleteEntry() fucntion.

```

	messageId := db.NewID()
	err := db.PutEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("unit8.b.b1"),
		Payload:  []byte("msg.b.b1.deleting"),
	})
	
	err := db.DeleteEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("unit8.b.b1"),
	})

```

#### Topic isolation
Topic isolation can be achieved using Contract while putting messages into tracedb or querying messages from a topic. Use DB.NewContract() to generate a new Contract and then specify Contract while putting messages using DB.PutEntry() function. Use Contract in the query to get messages from a topic.

```
	contract, err := db.NewContract()

	messageId := db.NewID()
	err := db.PutEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("unit8.b.b1"),
		Payload:  []byte("msg.b.b1.1"),
		Contract: contract,
	})
	....
	msgs, err := db.Get(&tracedb.Query{Topic: []byte("unit8.b.b1?last=1h", Contract: contract, Limit: 100}))

```

### Batch operation
Use batch operation to bulk insert records into tracedb or bulk delete records from tracedb. Batch operation also speeds up reading data if batch operation is used then reading records within short span of time while db is still open. See benchmark examples and run it locally to see performance of runnig batches concurrently.

#### Writing to a batch
Use Batch.Put() to write to a single topic in a batch. To write to single topic in a batch specify topic in batch options.

```
	// Writing to single topic in a batch
	err := db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		opts := tracedb.DefaultBatchOptions
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
    err := db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b1"), []byte("msg.b.b11.1")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b11"), []byte("msg.b.b11.2")))
		err := b.Write()
		return err
    })

```

#### Non-blocking batch operation
All batch operations are non-blocking so client program can decide to wait for completed signal and further execute any additional tasks.

```
    err := db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b1"), []byte("msg.b.b11.1")))
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
Specify topic to retrives values and use last parameter to specify duration or specify number of recent messages to retreive from the topic. for example, "last=1h" retrieves messsages from tracedb stored in last 1 hour, or "last=100" to retreives last 100 messages from the tracedb:

```
func print(topic []byte, db *tracedb.DB) {
	// topic -> "unit8.b.b1?last=1h"
	it, err := db.Items(&tracedb.Query{Topic: topic})
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

#### Writing batch chunk
Batch operation support writing chunk for large batch. It is safe to use Batch.Write() function multiple times within a batch operation.

As duplicate values are removed in a batch write operation, so specify batch option "AllowDuplicates" to keep duplicate values.

```
	err := db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		opts := tracedb.DefaultBatchOptions
		opts.Topic = []byte("unit8.b.*?ttl=1h")
		opts.AllowDuplicates = true
		b.SetOptions(opts)
		t := time.Now()
		for j := 0; j < 250; j++ {
			b.Put(t.MarshalText())
			if j%100 == 0 {
				if err := b.Write(); err != nil {
					return err
				}
			}
		}
		if err := b.Write(); err != nil {
			return err
		}
		return nil
	})

```

#### Topic isolation in batch operation
Topic isolation can be achieved using Contract while putting messages into tracedb and querying messages from a topic. Use DB.NewContract() to generate a new Contract and then specify Contract while putting messages using Batch.PutEntry() function.

```
	contract, err := db.NewContract()

    // Writing to single topic in a batch
	err := db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		opts := tracedb.DefaultBatchOptions
		opts.Topic = []byte("unit8.b.*?ttl=1h")
		opts.Contract = contract
		b.SetOptions(opts)
		b.Put([]byte("msg.b.*.1"))
		b.Put([]byte("msg.b.*.2"))
		b.Put([]byte("msg.b.*.3"))
		return b.Write()
    })

    // Writing to multiple topics in a batch
    err := db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		opts := tracedb.DefaultBatchOptions
		opts.Contract = contract
		b.SetOptions(opts)
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.*"), []byte("msg.b.*.1")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8..."), []byte("msg...")))
		b.PutEntry(tracedb.NewEntry([]byte("*"), []byte("msg.*.1")))
		b.PutEntry(tracedb.NewEntry([]byte("..."), []byte("msg...1")))
		return b.Write()
	})

```

#### Message encryption
Set encyrption flag in batch options to encrypt all messages in a batch. 

Note, encryption can also be set on entire database using DB.Open() and set encryption flag in options parameter. 

```
	err := db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
		opts := tracedb.DefaultBatchOptions
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
	g.Add(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b1?ttl=2h"), []byte("msg.b.b1.1")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.c.c1?ttl=1h"), []byte("msg.c.c1.1")))
		return b.Write()
	})

	g.Add(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b1"), []byte("msg.b.b1.2")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.c.c1"), []byte("msg.c.c1.2")))
		return b.Write()
	})

	g.Add(func(b *tracedb.Batch, completed <-chan struct{}) error {
		b.PutEntry(tracedb.NewEntry([]byte("unit8.b.b1"), []byte("msg.b.b1.3")))
		b.PutEntry(tracedb.NewEntry([]byte("unit8.c.c1"), []byte("msg.c.c1.3")))
		return b.Write()
	})

	err = g.Run()

```

### Statistics
The tracedb keeps a running metrics of internal operations it performs. To get tracedb metrics use DB.Varz() function.

```

	if varz, err := db.Varz(); err == nil {
		fmt.Printf("%+v\n", varz)
	}

```

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
Copyright (c) 2016-2020 Saffat IT Solutions Pvt Ltd. This project is licensed under [Affero General Public License v3](https://github.com/unit-io/tracedb/blob/master/LICENSE).
