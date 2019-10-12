# tracedb

<p align="left">
  <img src="tracedb.png" width="70" alt="Trace" title="tracedb: Blazing fast timeseries database fro IoT and real-time gaming application"> 
</p>

# tracedb: Blazing fast timeseries database for IoT and real-time gaming application

tracedb is a timeseries database for IoT application and real-time gaming applications

# Key characteristics
- 100% Go.
- Optimized for fast lookups and bulk inserts.
- Can store larger-than-memory data sets.
- Low memory usage.
- All DB methods are safe for concurrent use by multiple goroutines.

Tracedb can be used for online gaming and mobile apps as it satisfy the requirements for low latency and binary messaging. Tracedb is perfect timeseries data store such as internet of things and internet connected devices.

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
Use the DB.Batch() function to insert a new key/value pair or delete a record:

```
    err = db.Batch(func(b *tracedb.Batch) error {
		b.Put([]byte("dev18.b.b11"), []byte("bar"))
		b.Put([]byte("dev18.b.b11"), []byte("bar2"))
		b.Put([]byte("dev18.b.b1"), []byte("bar3"))
		b.Put([]byte("dev18.c.c11"), []byte("bar"))
		err = b.Write()
		return err
    })
```

Specify ttl to expires keys. 
To encrypt messages use batch options and set message encryption.

```
err = db.Batch(func(b *tracedb.Batch) error {
		opts := tracedb.DefaultBatchOptions
		opts.Encryption = true
		b.SetOptions(opts)
		b.Put([]byte("ttl.ttl1?ttl=3m"), []byte("bar"))
		b.Put([]byte("ttl.ttl2?ttl=3m"), []byte("bar"))
		b.Put([]byte("ttl.ttl3?ttl=3m"), []byte("bar"))
		err = b.Write()
		return err
	})
```

Use the BatchGroup.Add() function to group batches and run concurrently without causing write conflict. Use the BatchGroup.Run to run group of batches concurrently:

```
    g := db.NewBatchGroup()
	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b1?ttl=2m"), []byte("bar"))
		b.Put([]byte("dev18.c1?ttl=1m"), []byte("bar"))
		b.Put([]byte("dev18.b1?ttl=3m"), []byte("bar2"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b.b11"), []byte("bar"))
		b.Put([]byte("dev18.b.b11"), []byte("bar2"))
		b.Put([]byte("dev18.b.b1"), []byte("bar3"))
		b.Put([]byte("dev18.c.c11"), []byte("bar"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("dev18.b.b111"), []byte("bar"))
		b.Put([]byte("dev18.b.b111"), []byte("bar2"))
		b.Put([]byte("dev18.b.b11"), []byte("bar3"))
		b.Put([]byte("dev18.c.c111"), []byte("bar"))
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
			err := db.Batch(func(b *tracedb.Batch) error {
				t, _ := j.MarshalText()
				b.Put([]byte("dev18.b.b11?ttl=1m"), t)
				err := b.Write()
				return err
			})
			if err != nil {
				log.Printf("Error update1: %s", err)
			}
			print(db)
			if i >= retry {
				return
			}
			i++
		}
	}(7)
```

### Iterating over items
Use the DB.Items() function which returns a new instance of ItemIterator. 
Specify topic to retrives values and use last parameter to specify duration or specify number of recent messages to retreive from the topic:

```
func print(db *tracedb.DB) {
	it, err := db.Items(&tracedb.Query{Topic: []byte("dev18.b.b11?last=3m")})
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
		log.Printf("%s %s", it.Item().Key(), it.Item().Value())
	}
}
```

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
Copyright (c) 2016-2019 Saffat IT Solutions Pvt Ltd. This project is licensed under [Affero General Public License v3](https://github.com/saffat-in/tracedb/blob/master/LICENSE).
