# memdb [![GoDoc](https://godoc.org/github.com/unit-io/unitdb/memdb?status.svg)](https://pkg.go.dev/github.com/unit-io/unitdb/memdb) [![Go Report Card](https://goreportcard.com/badge/github.com/unit-io/unitdb/memdb)](https://goreportcard.com/report/github.com/unit-io/unitdb/memdb)

The memdb is blazing fast specialized in memory key-value store for time-series database. The in-memory key-value data store persist entries into a WAL for immediate durability. The Write Ahead Log (WAL) retains memdb data when the db restarts. The WAL ensures data is durable in case of an unexpected failure.

# About memdb

## Key characteristics
- 100% Go
- Optimized for fast lookups and writes
- All DB methods are safe for concurrent use by multiple goroutines.

## Quick Start
To build memdb from source code use go get command.

> go get -u github.com/unit-io/unitdb/memdb

## Usage
Detailed API documentation is available using the [go.dev](https://pkg.go.dev/github.com/unit-io/unitdb/memdb) service.

Make use of the client by importing it in your Go client source code. For example,

> import "github.com/unit-io/unitdb/memdb"

The memdb supports Get, Set, Delete operations. It also supports batch operations.

Samples are available in the cmd directory for reference.

### Opening a database
To open or create a new database, use the memdb.Open() function:

```
	package main

	import (
		"log"

		"github.com/unit-io/unitdb/memdb"
	)

	func main() {
		// Opening a database.
		// Open DB with reset flag to to skip recovery from log
		db, err := memdb.Open(memdb.WithLogFilePath("unitdb"), memdb.WithLogReset())
		if err != nil {
			log.Fatal(err)
			return
		}	
		defer db.Close()
	}

```

### Writing to a database

#### Store a message
Use DB.Put() function to insert a new key-value pair. 
Note, if key exists then it overrides key-value when the writes happen within same timeID (see timeRecordInterval option) or it insert a new key-value pair in different timeID.

```
	if timeID, err := db.Put(1, []byte("msg 1")); err != nil {
		log.Fatal(err)
		return
    }

```

#### Read messages
Use DB.Get() function to read inserted value. It gets entry from most recent timeID for the provided key. 

```
	if val, err := db.Get(1); err == nil {
        log.Printf("%s ", val)
    }

```

#### Read messages
Use DB.Look() function to look entry for faster read.

```
	timeID, err := db.Put(1, []byte("msg 1"))
	if err != nil {
		log.Fatal(err)
		return
    }

	if val, err := db.Lookup(timeID, 1); err == nil {
        log.Printf("%s ", val)
    }

```

#### Deleting a message
use DB.Delete() function to delete a key-value pair.

```
    if err := db.Delete(1); err != nil {
        fmt.Println("error: ", err)
    }

```

### Batch operation
Use batch operation to bulk insert records into memdb or bulk delete records from memdb. See examples under cmd/memdb folder.

#### Writing to a batch
Use Batch.Put() to insert a new key-value or Batch.Delete() to delete a key-value from DB.

```
	db.Batch(func(b *memdb.Batch, completed <-chan struct{}) error {
		for i := 1; i <= 10; i++ {
            val := []byte(fmt.Sprintf("msg.%2d", i))
            b.Put(uint64(i), val)
        }
		return nil
    })

```

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
This project is licensed under [Apache-2.0 License](https://github.com/unit-io/unitdb/blob/master/LICENSE).
