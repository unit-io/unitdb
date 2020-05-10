# unitdb [![GoDoc](https://godoc.org/github.com/unit-io/unitdb?status.svg)](https://pkg.go.dev/github.com/unit-io/unitdb) [![Go Report Card](https://goreportcard.com/badge/github.com/unit-io/unitdb)](https://goreportcard.com/report/github.com/unit-io/unitdb) [![Coverage Status](https://coveralls.io/repos/github/unit-io/unitdb/badge.svg?branch=master)](https://coveralls.io/github/unit-io/unitdb?branch=master)

<p align="left">
  <img src="unitdb.png" width="300" alt="unitdb" title="unitdb: Blazing fast timeseries database fro IoT and real-time messaging applications"> 
</p>

# unitdb: blazing fast time-series database for IoT and real-time messaging applications

unitdb is blazing fast time-series database for IoT, realtime messaging  applications. Access unitdb with pubsub over tcp or websocket using [unitd](https://github.com/unit-io/unitd) application.

unitdb can be used for online gaming and mobile apps as it satisfy the requirements for low latency and binary messaging. unitdb is a perfect time-series database for applications such as internet of things and internet connected devices.

# About unitdb 

## Key characteristics
- 100% Go
- Optimized for fast lookups and hyper scale writes
- Can store larger-than-memory data sets
- Data is safely written to disk with accuracy and high performant block sync technique
- Supports time-to-live on message entry
- Supports writing to wildcard topics
- Queried data is returned complete and correct

The unitdb engine includes the following components:

- Buffer Pool
- Block Cache
- Write Ahead Log (WAL)
- Lookup Trie
- Writing to timeWindow file
- Writing to Block Index file
- Writing to Data file

### Writing data to disk 
The unitdb engine handles data from the point put request is received through writing data to the physical disk. Data is written to unitdb using low latency binary messaging entry. Data is compressed and encrypted (if encryption is set) then written to a WAL for immediate durability. Entries are written to memdb block cache and become immediately queryable. The memdb block cache is periodically written to disk in the form of blocks.

### Write Ahead Log (WAL)
The Write Ahead Log (WAL) retains unitdb data when the db restarts. The WAL ensures data is durable in case of an unexpected failure.

When the unitdb engine receives a put request, the following steps occur:

- The put request is parsed, packed and appended to a tinyBatch buffer.
- Topic is parsed into parts and added to the lookup Trie. Contract is added to the first part of the parts in the lookup Trie.
- The data is added to the memdb block cache.
- The tinyBatch is appended to the WAL in cyclic order.
- The last offset of topic from timeWindow block is added to the Trie.
- Data is written to disk using block sync.
- The memdb block cache is updated with free offset. The memdb block cache shrinks if it reaches target size.
- When data is successfully written to WAL, a response confirms the write request was successful.

Blocks sync writes the timeWindow blocks, index blocks, and data blocks to disk.

When the unitdb restarts, last offset of all topics is loaded into Trie, the WAL file is read back and pending writes are applied to the unitdb.

### Block Cache
The memdb block cache is an in-memory copy of entries that currently stored in the WAL. The block cache:

- Organizes entries as per contract into shards.
- Stores keys and offsets into map
- Stores compressed data into data blocks.

Queries to the unitdb merge data from the block cache with data from the files. Queries first lookup topic offset from lookup Trie. Topic offset is used to traverse timeWindow blocks and get entries sequence. Entry sequence is used to calculate index block offset and index block is read from the index file, then it uses entry information from index block to read data from data file and un-compresses the data. As encryption flag is set on first bit of sequence so if data is encrypted then it get un-encrypted while data is read.

### Block Sync
To efficiently compact and store data, the unitdb engine groups entries sequence by topic key, and then orders those sequences by time and each block keep offset to next field of previous block in reverse time order.

Index block stores entry sequence, data block offset, message size and expiry details. The block offset of index block is calculated from entry sequence. The unitdb compress data and store it into data blocks. If an entry expires or deleted then the data offset and size is marked as free and added to the lease blocks so that it can get allocated by new request.

After data is stored safely in files, the WAL is truncated and the block cache free offset is updated to shrink memdb.

## Quick Start
To build unitdb from source code use go get command.

> go get -u github.com/unit-io/unitdb

## Usage

The unitdb support Get, Put, Delete operations. It also support encryption, batch operations, group batch operations, and writing to wildcard topics. See complete [usage guide](https://github.com/unit-io/unitdb/tree/master/docs/usage/advanced.md) for more advanced use case. 

### Opening a database

To open or create a new database, use the unitdb.Open() function:

```

	package main

	import (
		"log"

		"github.com/unit-io/unitdb"
	)

	func main() {
		db, err := unitdb.Open("unitdb.example", nil)
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

#### Specify ttl 
Specify ttl parameter to a topic while storing messages to expire it after specific duration. 
Note, DB.Get() or DB.Items() function does not fetch expired messages. 

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
Deleting a message in unitdb is rare and it require additional steps to delete message from a given topic. Generate a unique message ID using DB.NewID() and use this unique message ID while putting message to the unitdb using DB.PutEntry(). To delete message provide message ID to the DB.DeleteEntry() function.

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
Topic isolation can be achieved using Contract while putting messages into unitdb or querying messages from a topic. Use DB.NewContract() to generate a new Contract and then specify Contract while putting messages using DB.PutEntry() method. Use Contract in the query to get messages from a topic specific to the contract.

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

### Iterating over items
Use the DB.Items() function which returns a new instance of ItemIterator. 
Specify topic to retrieve values and use last parameter to specify duration or specify number of recent messages to retrieve from the topic. for example, "last=1h" retrieves messages from unitdb stored in last 1 hour, or "last=100" to retrieves last 100 messages from the unitdb:

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

### Statistics
The unitdb keeps a running metrics of internal operations it performs. To get unitdb metrics use DB.Varz() function.

```

	if varz, err := db.Varz(); err == nil {
		fmt.Printf("%+v\n", varz)
	}

```

# Demo
To access unitdb using websocket build unitd from source code using go get command.

> go get -u github.com/unit-io/unitd && unitd

Open [unitd.html](https://github.com/unit-io/unitd/blob/master/examples/html/unitd.html) under example/html folder in browser.

## Steps
- Generate Client ID
- Specify new client ID and connect to client
- Specify topics to subscribe/publish messages and generate key
- Specify key to the topics with separator '/' and subscribe to topic
- Specify message to send and publish to topic

### First Client
![Client1](https://github.com/unit-io/unitdb/tree/master/docs/img/client1.png)

### Second Client
![Client2](https://github.com/unit-io/unitdb/tree/master/docs/img/client2.png)

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
Copyright (c) 2016-2020 Saffat IT Solutions Pvt Ltd. This project is licensed under [Affero General Public License v3](https://github.com/unit-io/unitdb/blob/master/LICENSE).
