# tracedb [![GoDoc](https://godoc.org/github.com/unit-io/tracedb?status.svg)](https://pkg.go.dev/github.com/unit-io/tracedb) [![Go Report Card](https://goreportcard.com/badge/github.com/unit-io/tracedb)](https://goreportcard.com/report/github.com/unit-io/tracedb) [![Coverage Status](https://coveralls.io/repos/github/unit-io/tracedb/badge.svg?branch=master)](https://coveralls.io/github/unit-io/tracedb?branch=master)

<p align="left">
  <img src="tracedb.png" width="70" alt="Trace" title="tracedb: Blazing fast timeseries database fro IoT and real-time messaging applications"> 
</p>

# tracedb: blazing fast time-series database for IoT and real-time messaging applications

tracedb is blazing fast time-series database for IoT, realtime messaging  applications. Access tracedb with pubsub over tcp or websocket using [trace](https://github.com/unit-io/trace) application.

Tracedb can be used for online gaming and mobile apps as it satisfy the requirements for low latency and binary messaging. Tracedb is a perfect time-series database for applications such as internet of things and internet connected devices.

# Key characteristics
- 100% Go.
- Optimized for fast lookups and bulk inserts.
- Can store larger-than-memory data sets.
- Entire database can run in memory backed with file storage if system memory is larger than data sets. 
- All DB methods are safe for concurrent use by multiple goroutines.

# Planned
- Documentation - document the technical architecture, technical design and advanced usage guides.

## Table of Contents
 * [Quick Start](#Quick-Start)
 * [Usage](#Usage)
 * [Opening a database](#Opening-a-database)
 + [Writing to a database](#Writing-to-a-database)
   - [Store a message](#Store-a-message)
   - [Specify ttl](#Specify-ttl)
   - [Read messages](#Read-messages)
   - [Deleting a message](#Deleting-a-message)
   - [Topic isolation](#Topic-isolation)
 * [Iterating over items](#Iterating-over-items)
 * [Statistics](#Statistics)

## Quick Start
To build tracedb from source code use go get command.

> go get -u github.com/unit-io/tracedb

## Usage

The tracedb support Get, Put, Delete operations. It also support encryption, batch operations, group batch operations, and writing to wildcard topics. See complete [usage guide](https://github.com/unit-io/tracedb/docs/usage/advanced.md) for more advanced use case. 

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

	topic := []byte("unit8.b.b1")
	msg := []byte("msg.b.b1.1")
	db.Put(topic, msg)

	or
	
	db.PutEntry(tracedb.NewEntry(topic, msg))

```

#### Specify ttl 
Specify ttl parameter to a topic while storing messages to expire it after specific duration. 
Note, DB.Get() or DB.Items() function does not fetch expired messages. 

```
	topic := []byte("unit8.b.b1?ttl=1h")
	msg := []byte("msg.b.b1.1")
	b.PutEntry(tracedb.NewEntry(topic, msg))

```

#### Read messages
Use DB.Get() to read messages from a topic. Use last parameter to specify duration or specify number of recent messages to read from a topic. for example, "last=1h" gets messages from tracedb stored in last 1 hour, or "last=100" to gets last 100 messages from tracedb. Specify an optional parameter Query.Limit to retrieves messages from a topic with a limit.

```

	var err error
	var msg [][]byte
	msgs, err = db.Get(&tracedb.Query{Topic: []byte("unit8.b.b1?last=100")})
    ....
	msgs, err = db.Get(&tracedb.Query{Topic: []byte("unit8.b.b1?last=1h", Limit: 100}))

```

#### Deleting a message
Deleting a message in tracedb is rare and it require additional steps to delete message from a given topic. Generate a unique message ID using DB.NewID() and use this unique message ID while putting message to the tracedb using DB.PutEntry(). To delete message provide message ID to the DB.DeleteEntry() function.

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

### Iterating over items
Use the DB.Items() function which returns a new instance of ItemIterator. 
Specify topic to retrieves values and use last parameter to specify duration or specify number of recent messages to retrieve from the topic. for example, "last=1h" retrieves messages from tracedb stored in last 1 hour, or "last=100" to retrieves last 100 messages from the tracedb:

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
