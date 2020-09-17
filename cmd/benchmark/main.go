package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	numKeys      = flag.Int("n", 100000, "number of keys")
	minKeySize   = flag.Int("mink", 16, "minimum key size")
	maxKeySize   = flag.Int("maxk", 64, "maximum key size")
	minValueSize = flag.Int("minv", 128, "minimum value size")
	maxValueSize = flag.Int("maxv", 512, "maximum value size")
	concurrency  = flag.Int("c", 10, "number of concurrent goroutines")
	dir          = flag.String("d", ".", "database directory")
)

func main() {

	flag.Parse()

	if *dir == "" {
		flag.Usage()
		return
	}

	if err := benchmark1(*dir, *numKeys, *minKeySize, *maxKeySize, *minValueSize, *maxValueSize, *concurrency); err != nil {
		fmt.Fprintf(os.Stderr, "Error running benchmark: %v\n", err)
	}
	if err := benchmark2(*dir, *numKeys, *minKeySize, *maxKeySize, *minValueSize, *maxValueSize, *concurrency); err != nil {
		fmt.Fprintf(os.Stderr, "Error running benchmark: %v\n", err)
	}
	if err := benchmark3(*dir, *numKeys, *minKeySize, *maxKeySize, *minValueSize, *maxValueSize, *concurrency); err != nil {
		fmt.Fprintf(os.Stderr, "Error running benchmark: %v\n", err)
	}
	if err := recovery(*dir); err != nil {
		fmt.Fprintf(os.Stderr, "Error running recovery: %v\n", err)
	}
}
