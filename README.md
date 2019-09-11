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

> go get -u github.com/frontnet/tracedb

## Usage

### Opening a database

To open or create a new database, use the tracedb.Open() function:

```
package main

import (
	"log"

	"github.com/frontnet/tracedb"
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
Use the DB.Update() function to insert a new key/value pair or delete a record:

```
err := db.Update(func(b *tracedb.Batch) error {
			b.PutWithTTL([]byte("b1"), []byte("bar"), time.Second*30)
			b.Put([]byte("b2"), []byte("bar"))
			b.Put([]byte("b3"), []byte("bar"))
			b.Delete([]byte("b3"))
			b.Write()
			return err
		})
```

Use the BatchGroup.Add() function to group batches and run concurrently without causing write conflict. Use the BatchGroup.Run to run group of batches concurrently:

```
    g := testdb.NewBatchGroup()
	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		//b.PutWithTTL([]byte("ttl1"), []byte("bar"), time.Second*30)
		b.Put([]byte("b1"), []byte("bar"))
		b.Put([]byte("c1"), []byte("bar"))
		b.Put([]byte("b1"), []byte("bar2"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

	g.Add(func(b *tracedb.Batch, stop <-chan struct{}) error {
		b.Put([]byte("b11"), []byte("bar"))
		b.Put([]byte("b11"), []byte("bar2"))
		b.Put([]byte("b1"), []byte("bar3"))
		b.Put([]byte("c11"), []byte("bar"))
		b.Write()
		go func() {
			<-stop // it signals batch group completion
			log.Printf("batch group completed")
		}()
		return nil
	})

    g.Run()

```

### Reading from a database
Use the DB.Get() function to retrieve the inserted value:

```
val, err := db.Get([]byte("foo"))
if err != nil {
	log.Fatal(err)
}
log.Printf("%s", val)
```

### Iterating over items
Use the DB.Items() function which returns a new instance of ItemIterator:

```
it := db.Items()
for it.First(); it.Valid(); it.Next()
    if it.Error() != nil {
        if err != tracedb.ErrIterationDone {
            log.Fatal(err)
        }
        break
    }
    log.Printf("%s %s", it.Item().Key(), it.Item().Value())
}
```

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
Copyright (c) 2016-2019 Saffat IT Solutions Pvt Ltd. This project is licensed under [Affero General Public License v3](https://github.com/frontnet/tracedb/blob/master/LICENSE).
