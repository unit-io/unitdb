package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/pkg/profile"
)

var (
	engine       = flag.String("e", "tracedb", "database engine name. tracedb, pogreb, goleveldb, bbolt or badger")
	numKeys      = flag.Int("n", 1000000, "number of keys")
	minKeySize   = flag.Int("mink", 16, "minimum key size")
	maxKeySize   = flag.Int("maxk", 64, "maximum key size")
	minValueSize = flag.Int("minv", 128, "minimum value size")
	maxValueSize = flag.Int("maxv", 512, "maximum value size")
	concurrency  = flag.Int("c", 271, "number of concurrent goroutines")
	dir          = flag.String("d", ".", "database directory")
	progress     = flag.Bool("p", false, "show progress")
	profileMode  = flag.String("profile", "", "enable profile. cpu, mem, block or mutex")
)

func main() {

	flag.Parse()

	if *dir == "" {
		flag.Usage()
		return
	}

	switch *profileMode {
	case "cpu":
		defer profile.Start(profile.CPUProfile).Stop()
	case "mem":
		defer profile.Start(profile.MemProfile).Stop()
	case "block":
		defer profile.Start(profile.BlockProfile).Stop()
	case "mutex":
		defer profile.Start(profile.MutexProfile).Stop()
	}

	if err := benchmark(*engine, *dir, *numKeys, *minKeySize, *maxKeySize, *minValueSize, *maxValueSize, *concurrency, *progress); err != nil {
		fmt.Fprintf(os.Stderr, "Error running benchmark: %v\n", err)
	}
}
