# unitdb [![GoDoc](https://godoc.org/github.com/unit-io/unitdb?status.svg)](https://pkg.go.dev/github.com/unit-io/unitdb) [![Go Report Card](https://goreportcard.com/badge/github.com/unit-io/unitdb)](https://goreportcard.com/report/github.com/unit-io/unitdb) [![Build Status](https://travis-ci.org/unit-io/unitdb.svg?branch=master)](https://travis-ci.org/unit-io/unitdb) [![Coverage Status](https://coveralls.io/repos/github/unit-io/unitdb/badge.svg?branch=master)](https://coveralls.io/github/unit-io/unitdb?branch=master)

Unitdb is blazing fast time-series database for IoT, realtime messaging  applications. Access unitdb with grpc client ot pubsub over tcp or websocket using [unitd](https://github.com/unit-io/unitd) application.

Unitdb can be used for online gaming and mobile apps as it satisfy the requirements for low latency and binary messaging. unitdb is a perfect time-series database for applications such as internet of things and internet connected devices.

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

## Unitd Clients
To run unitdb as daemon service start [unitd](https://github.com/unit-io/unitd) application and copy unitd.conf to the path unitd binary is placed. Unitd supports pubsub using GRPC client or MQTT client to connect to service using tcp or websocket.
- [unitd-go](https://github.com/unit-io/unitd-go) is Go client to pubsub messages using GRPC application
- [unitd-ws](https://github.com/unit-io/unitd-ws) is javascript client to pubsub messages using MQTT over websocket 

## Quick Start
To build unitdb from source code use go get command.

> go get -u github.com/unit-io/unitdb

## Usage

The unitdb supports Get, Put, Delete operations. It also supports encryption, batch operations, group batch operations, and writing to wildcard topics. See complete [usage guide](https://github.com/unit-io/unitdb/tree/master/docs/usage/advanced.md). 

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

#### Specify ttl 
Specify ttl parameter to a topic while storing messages to expire it after specific duration. 

```
	topic := []byte("teams.alpha.ch1.u1?ttl=1h")
	msg := []byte("msg for team alpha channel1 receiver1")
	b.PutEntry(unitdb.NewEntry(topic, msg))

```

#### Read messages
Use DB.Get() to read messages from a topic. Use last parameter to specify duration or specify number of recent messages to read from a topic. for example, "last=1h" gets messages from unitdb stored in last 1 hour, or "last=100" to get last 100 messages from unitdb. Specify an optional parameter Query.Limit to retrieve messages from a topic with a limit.

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

	messageId := db.NewID()
	err := db.PutEntry(&unitdb.Entry{
		ID:       messageId,
		Topic:    []byte("teams.alpha.ch1"),
		Payload:  []byte("msg for team alpha channel1"),
		Contract: contract,
	})
	....
	msgs, err := db.Get(&unitdb.Query{Topic: []byte("teams.alpha.ch1?last=1h", Contract: contract, Limit: 100}))

```

### Iterating over items
Use the DB.Items() function which returns a new instance of ItemIterator. 
Specify topic to retrieve values and use last parameter to specify duration or specify number of recent messages to retrieve from the topic. for example, "last=1h" retrieves messages from unitdb stored in last 1 hour, or "last=100" to retrieves last 100 messages from the unitdb:

```

	func print(topic []byte, db *unitdb.DB) {
		// topic -> "teams.alpha.ch1?last=1h"
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
Copyright (c) 2016-2020 Saffat IT Solutions Pvt Ltd. This project is licensed under [Affero General Public License v3](https://github.com/unit-io/unitdb/blob/master/LICENSE).
