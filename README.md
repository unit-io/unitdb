# unitdb [![GoDoc](https://godoc.org/github.com/unit-io/unitdb?status.svg)](https://pkg.go.dev/github.com/unit-io/unitdb) [![Go Report Card](https://goreportcard.com/badge/github.com/unit-io/unitdb)](https://goreportcard.com/report/github.com/unit-io/unitdb) [![Build Status](https://travis-ci.org/unit-io/unitdb.svg?branch=master)](https://travis-ci.org/unit-io/unitdb) [![Coverage Status](https://coveralls.io/repos/github/unit-io/unitdb/badge.svg?branch=master)](https://coveralls.io/github/unit-io/unitdb?branch=master)

Unitdb is blazing fast specialized time-series database for microservices, IoT, and realtime internet connected devices. The unitdb satisfy the requirements for low latency and binary messaging, it is a perfect time-series database for applications such as internet of things and internet connected devices.

```
Don't forget to ⭐ this repo if you like unitdb!
```

# About unitdb 

## Key characteristics
- 100% Go
- Can store larger-than-memory data sets
- Optimized for fast lookups and writes
- Supports writing billions of messages (or metrics) per hour
- Supports opening database with immutable flag
- Supports database encryption
- Supports time-to-live on message entries
- Supports writing to wildcard topics
- Data is safely written to disk with accuracy and high performant block sync technique

## Quick Start
To build unitdb from source code use go get command.

> go get -u github.com/unit-io/unitdb

## Usage
Detailed API documentation is available using the [go.dev](https://pkg.go.dev/github.com/unit-io/unitdb) service.

Make use of the client by importing it in your Go client source code. For example,

> import "github.com/unit-io/unitdb"

The in-memory key-value data store persist entries into a WAL for immediate durability. The Write Ahead Log (WAL) retains memdb data when the db restarts. The WAL ensures data is durable in case of an unexpected failure. Make use of the client by importing in your Go client source code. For example,

> import "github.com/unit-io/unitdb/memdb"

The unitdb supports Get, Put, Delete operations. It also supports encryption, batch operations, and writing to wildcard topics. See [usage guide](https://github.com/unit-io/unitdb/tree/master/docs/usage.md). 

Samples are available in the cmd directory for reference.

## About
Entries are written to memdb and becomes immediately queryable. The memdb entries are periodically written to log files in the form of blocks. Time mark keeps record of tiny logs written to the memdb and releases the time IDs when time blocks are committed to the WAL. The released time blocks records are then sync to the unitdb.

To efficiently compact and store data, the unitdb engine groups entries sequence by topic key, and then orders those sequences by time and each block keep offset of previous block in reverse time order. Index block offset is calculated from entry sequence in the time block. Data is read from data block using index entry information and the it un-compresses the data on read (if encryption flag was set then it un-encrypts the data on read).

```
  Time-block                                                      Write-ahead log
	+---------+---------+-----------+-...-+---------------+         +--------------+--------------+--------------+-...-+-----------------+
	| TinyLog | TinyLog |  TinyLog  |     |   TinyLog     | ------->| TimeID|block | TimeID|block | TimeID|block |     |   TimeID|block  |
	+---------+---------+-----------+-...-+---------------+         +--------------+--------------+--------------+-...-+-----------------+
      |
      |
      |
      v
    Topic trie                                                                       Window block
    +----------+        +------+         +------+      +--------+    Window-offset   +-----------------------------+-----------------------------+-...-+-----------------------------+
    | Contract | -----> | Hash | ----->  | Hash | ---> | Offset |      ----->        | Topic hash|sequence...|next | Topic hash|sequence...|next |     | Topic hash|sequence...|next |
    +----------+        +------+   |     +------+      +--------+                    +-----------------------------+-----------------------------+-...-+-----------------------------+
    | Contract |  ---              |     +------+      +------+      +--------+      +-----------------------------+-----------------------------+-...-+-----------------------------+
    +----------+                   --->  | Hash | ---> | Hash | ---> | Offset | ---> | Topic hash|sequence...|next | Topic hash|sequence...|next |     | Topic hash|sequence...|next |
    | Contract | --                |     +------+      +------+      +--------+      +-----------------------------+-----------------------------+-...-+-----------------------------+
    +----------+  |                |     +------+      +------+      +--------+      +-----------------------------+-----------------------------+-...-+-----------------------------+
                  |                --->  | Hash | ---> | Hash | ---> | Offset | ---> | Topic hash|sequence...|next | Topic hash|sequence...|next |     | Topic hash|sequence...|next |
                  |                      +------+      +------+      +--------+      +-----------------------------+-----------------------------+-...-+-----------------------------+
                  |    +------+      +--------+                                                           |
                  ---> | Hash | ---> | Offset |                                                           | Index block-offset = (sequence-1)/(block-size)
                    +------+      +--------+                                                              |
                                            Index block                                                   v
                                            +-------------------+-------------------+-------------------+-...-+-------------------+
                                            | Offset|value size | Offset|value size | Offset|value size |     | Offset|value size |
                                            +-------------------+-------------------+-------------------+-...-+-------------------+
                                                              |
                                                              | Data block-offset
                                            Data block        v
                                            +------------+------------+------------+------------+-...-+------------+
                                            | Topic|data | Topic|data | Topic|data | Topic|data |     | Topic|data |
                                            +------------+------------+------------+------------+-...-+------------+ 

```

## Projects Using Unitdb
Below is a list of projects that use unitdb.

- [unite](https://github.com/unit-io/unite) Lightweight, high performance messaging system for microservices, and internet connected devices.

## Contributing
The unitdb is under active development and at this time unitdb is not seeking major changes or new features from new contributors. However, small bugfixes are encouraged.

## Licensing
This project is licensed under [Apache-2.0 License](https://github.com/unit-io/unitdb/blob/master/LICENSE).
