# tracedb

<p align="left">
  <img src="tracedb.png" width="70" alt="Trace" title="tracedb: Blazing fast timeseries database fro IoT and real-time gaming application"> 
</p>

# tracedb: Blazing fast timeseries database fro IoT and real-time gaming application

## Trace is an open source messaging borker for IoT and other real-time messaging service. Trace messaging API is built for speed and security.

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
Use the DB.Put() function to insert a new key/value pair:

```
err := db.Put([]byte("foo"), []byte("bar"))
if err != nil {
	log.Fatal(err)
}
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
for {
    key, val, err := it.Next()
    if err != nil {
        if err != tracedb.ErrIterationDone {
            log.Fatal(err)
        }
        break
    }
    log.Printf("%s %s", key, val)
}
```

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
Copyright (c) 2016-2019 Saffat IT Solutions Pvt Ltd. This project is licensed under [Affero General Public License v3](https://github.com/frontnet/tracedb/blob/master/LICENSE).
