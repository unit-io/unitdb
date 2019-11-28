# tracedb

<p align="left">
  <img src="tracedb.png" width="70" alt="Trace" title="tracedb: Blazing fast timeseries database fro IoT and real-time gaming application"> 
</p>

# tracedb: Blazing fast timeseries database for IoT and real-time gaming applications

tracedb is blazing fast timeseries database for IoT, realtime gaming, messaging or chat applications. Use trace application under, github.com/saffat-in/trace to interact with tracedb using pubsub over tcp or websocket.

# Key characteristics
- 100% Go.
- Optimized for fast lookups and bulk inserts.
- Can store larger-than-memory data sets.
- Low memory usage.
- All DB methods are safe for concurrent use by multiple goroutines.

Tracedb can be used for online gaming and mobile apps as it satisfy the requirements for low latency and binary messaging. Tracedb is perfect timeseries data store for applications such as internet of things and internet connected devices.

## Quick Start
To build tracedb from source code use go get command.

> go get -u github.com/saffat-in/tracedb

## Usage

### Opening a database

To open or create a new database, use the tracedb.Open() function:


```
package main

import (
	"log"

	"github.com/saffat-in/tracedb"
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
Use the DB.Batch() function to store messages to topic or delete a message from topic. Batch operation speeds up bulk insert records into tracedb. Reading data is blazing fast if batch operation is used to insert records into tracedb and reading those records within short span of time for example 24 hours:

```
    err = db.Batch(func(b *tracedb.Batch) error {
		b.Put([]byte("dev18.b.b11"), []byte("dev18.b.b11.1"))
		b.Put([]byte("dev18.b.b11"), []byte("dev18.b.b11.2"))
		b.Put([]byte("dev18.b.b1"), []byte("dev18.b.b1.1"))
		b.Put([]byte("dev18.c.c11"), []byte("dev18.c.c11.1"))
		err = b.Write()
		return err
    })

```

Deleting message.
Deleting a message in tracedb is rare and it require additional steps to delete message from given topic. Generate a unique message ID using DB.GenID() and use this unique message ID while putting message to the tracedb using DB.PutEntry(). To delete message provide message ID to the DB.DeleteEntry() fucntion.

```

	messageId := db.GenID()
	err := db.PutEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("dev18.b.b1"),
		Payload:  []byte("dev18.b.b1.2"),
		Contract: 3376684800,
	})
	
	err := db.DeleteEntry(&tracedb.Entry{
		ID:       messageId,
		Topic:    []byte("dev18.b.b1"),
		Contract: 3376684800,
	})

```

Writing to wildcard topics.
Tracedb supports wrting to wildcard topics. Use "*" in the topic to write to wildcard topic or use "..." at the end of topic to write to all sub-topics. Writing to following wildcard topics are also supported, "*" or "..."

```
	err = db.Batch(func(b *tracedb.Batch) error {
		b.Put([]byte("dev18.*.b11"), []byte("dev18.*.b11.1"))
		b.Put([]byte("dev18.b.*"), []byte("dev18.b.*.1"))
		b.Put([]byte("dev18..."), []byte("dev18...1"))
		b.Put([]byte("*"), []byte("*.1"))
		b.Put([]byte("..."), []byte("...1"))
		err = b.Write()
		return err
    })

```

Specify ttl to expires keys. 
To encrypt messages use batch options and set message encryption. Note, encryption can also be set on entire database using DB.Open() and provide encryption in the option parameter.

```
err = db.Batch(func(b *tracedb.Batch) error {
		opts := tracedb.DefaultBatchOptions
		opts.Encryption = true
		b.SetOptions(opts)
		b.Put([]byte("dev18.b.b1?ttl=3m"), []byte("dev18.b.b1.1"))
		b.Put([]byte("dev18.b.b11?ttl=3m"), []byte("dev18.b.b11.1"))
		b.Put([]byte("dev18.b.b111?ttl=3m"), []byte("dev18.b.b111.1"))
		err = b.Write()
		return err
	})
```

Use the BatchGroup.Add() function to group batches and run concurrently without causing write conflict. Use the BatchGroup.Run to run group of batches concurrently:

```
    g := db.NewBatchGroup()
	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b.b1?ttl=2m"), []byte("dev18.b.b1.1"))
		b.Put([]byte("dev18.c.c1?ttl=1m"), []byte("dev18.c.c1.1"))
		b.Put([]byte("dev18.b.b1?ttl=3m"), []byte("dev18.b.b1.2"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b.b11"), []byte("dev18.b.b11.1"))
		b.Put([]byte("dev18.b.b11"), []byte("dev18.b.b11.2"))
		b.Put([]byte("dev18.b.b1"), []byte("dev18.b.b1.3"))
		b.Put([]byte("dev18.c.c11"), []byte("dev18.c.c11.1"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b.b111"), []byte("dev18.b.b111.1"))
		b.Put([]byte("dev18.b.b111"), []byte("dev18.b.b111.2"))
		b.Put([]byte("dev18.b.b11"), []byte("dev18.b.b11.3"))
		b.Put([]byte("dev18.c.c111"), []byte("dev18.c.c111"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	err = g.Run()

```

```
    func(retry int) {
		i := 0
		for j := range time.Tick(60 * time.Second) {
			print([]byte("dev18.b.b1?last=2m"), db)
			print([]byte("dev18.b.b11?last=2m"), db)
			print([]byte("dev18?last=2m"), db)
			print([]byte("dev19?last=2m"), db)
			if i >= retry {
				return
			}
			i++
		}
	}(4)
```

### Iterating over items
Use the DB.Items() function which returns a new instance of ItemIterator. 
Specify topic to retrives values and use last parameter to specify duration or specify number of recent messages to retreive from the topic. for example, "last=1h" retrieves messsages from tracedb stored in last 1 hour, or "last=100" to retreives last 100 messages from the tracedb:

```
func print(topic []byte, db *tracedb.DB) {
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

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
Copyright (c) 2016-2019 Saffat IT Solutions Pvt Ltd. This project is licensed under [Affero General Public License v3](https://github.com/saffat-in/tracedb/blob/master/LICENSE).
