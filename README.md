# unitdb [![GoDoc](https://godoc.org/github.com/unit-io/unitdb?status.svg)](https://pkg.go.dev/github.com/unit-io/unitdb) [![Go Report Card](https://goreportcard.com/badge/github.com/unit-io/unitdb)](https://goreportcard.com/report/github.com/unit-io/unitdb) [![Build Status](https://travis-ci.org/unit-io/unitdb.svg?branch=master)](https://travis-ci.org/unit-io/unitdb) [![Coverage Status](https://coveralls.io/repos/github/unit-io/unitdb/badge.svg?branch=master)](https://coveralls.io/github/unit-io/unitdb?branch=master)

Unitdb is blazing fast specialized time-series database for microservices, IoT, and realtime internet connected devices. The unitdb satisfy the requirements for low latency and binary messaging, it is a perfect time-series database for applications such as internet of things and internet connected devices.

# About unitdb 

## Key characteristics
- 100% Go
- Can store larger-than-memory data sets
- Optimized for fast lookups and writes
- Supports writing billions of messages (or metrics) per hour with very low memory usages
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

import "github.com/unit-io/unitdb"

The unitdb supports Get, Put, Delete operations. It also supports encryption, batch operations, and writing to wildcard topics. See [usage guide](https://github.com/unit-io/unitdb/tree/master/docs/usage.md). 

Samples are available in the cmd directory for reference.

## Projects Using Unitdb
Below is a list of projects that use unitdb.

- [Unite](https://github.com/unit-io/unite) Lightweight, high performance messaging system for microservices, and internet connected devices.

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
This project is licensed under [Apache-2.0 License](https://github.com/unit-io/unitdb/blob/master/LICENSE).
