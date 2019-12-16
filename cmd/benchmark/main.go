package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/pkg/profile"
)

var (
	numKeys     = flag.Int("n", 500000, "number of keys")
	minKeySize  = flag.Int("mink", 16, "minimum key size")
	maxKeySize  = flag.Int("maxk", 64, "maximum key size")
	minValueSize = flag.Int("minv", 128, "minimum value size")
	maxValueSize = flag.Int("maxv", 512, "maximum value size")
	concurrency = flag.Int("c", 271, "number of concurrent goroutines")
	dir         = flag.String("d", ".", "database directory")
	profileMode = flag.String("profile", "", "enable profile. cpu, mem, block or mutex")
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

	if err := benchmark(*dir, *numKeys, *minKeySize, *maxKeySize, *minValueSize, *maxValueSize, *concurrency); err != nil {
		fmt.Fprintf(os.Stderr, "Error running benchmark: %v\n", err)
	}
}
