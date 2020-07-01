# unitdb [![GoDoc](https://godoc.org/github.com/unit-io/unitdb?status.svg)](https://pkg.go.dev/github.com/unit-io/unitdb) [![Go Report Card](https://goreportcard.com/badge/github.com/unit-io/unitdb)](https://goreportcard.com/report/github.com/unit-io/unitdb) [![Build Status](https://travis-ci.org/unit-io/unitdb.svg?branch=master)](https://travis-ci.org/unit-io/unitdb) [![Coverage Status](https://coveralls.io/repos/github/unit-io/unitdb/badge.svg?branch=master)](https://coveralls.io/github/unit-io/unitdb?branch=master)

Unitdb is blazing fast specialized time-series database for IoT, realtime messaging  applications or AI analytics. Unitdb satisfy the requirements for low latency and binary messaging, it is a perfect time-series database for applications such as internet of things and internet connected devices.

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

## Quick Start
To build unitdb from source code use go get command.

> go get -u github.com/unit-io/unitdb

## Usage
Detailed API documentation is available using the [godoc.org](https://godoc.org/github.com/unit-io/unitd-go) service.

Make use of the client by importing it in your Go client source code. For example,

import "github.com/unit-io/unitdb"

The unitdb supports Get, Put, Delete operations. It also supports encryption, batch operations, and writing to wildcard topics. See [usage guide](https://github.com/unit-io/unitdb/tree/master/docs/usage.md). 

Samples are available in the cmd directory for reference.

## Projects Using Unitdb
Below is a list of projects that use unitdb.

- [Unitd](https://github.com/unit-io/unitd) Lightweight, high performance messaging system for microservices, IoT, and internet connected devices.
- [Unitd Go Client](https://github.com/unit-io/unitd-go) Lightweight and high performance publish-subscribe messaging system client library.

## Contributing
If you'd like to contribute, please fork the repository and use a feature branch. Pull requests are welcome.

## Licensing
Copyright (c) 2016-2020 Saffat IT Solutions Pvt Ltd. This project is licensed under [Apache-2.0 License](https://github.com/unit-io/unitdb/blob/master/LICENSE).
