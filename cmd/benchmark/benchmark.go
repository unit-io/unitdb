package main

import (
	"fmt"
	"math/rand"
	"path"
	"runtime"
	"time"

	"github.com/saffat-in/tracedb"
	"golang.org/x/sync/errgroup"
)

func randKey(minL int, maxL int) string {
	n := rand.Intn(maxL-minL+1) + minL
	buf := make([]byte, n)
	for i := 0; i < n; i++ {
		buf[i] = byte(rand.Intn(95) + 32)
	}
	return string(buf)
}

func randValue(rnd *rand.Rand, src []byte, minS int, maxS int) []byte {
	n := rnd.Intn(maxS-minS+1) + minS
	return src[:n]
}

func forceGC() {
	runtime.GC()
	time.Sleep(time.Millisecond * 500)
}

func shuffle(a [][]byte) {
	for i := len(a) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		a[i], a[j] = a[j], a[i]
	}
}

func generateTopics(count int, minL int, maxL int) [][]byte {
	topics := make([][]byte, 0, count)
	seen := make(map[string]struct{}, count)
	for len(topics) < count {
		k := randKey(minL, maxL)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		topic := make([]byte, len(k)+5)
		topic = append(topic, []byte("dev18.")...)
		topic = append(topic, []byte(k)...)
		topics = append(topics, topic)
	}
	return topics
}

func printStats(db *tracedb.DB) {
	fmt.Printf("%+v\n", db.Metrics())
}

func benchmark(dir string, numKeys int, minKS int, maxKS int, minVS int, maxVS int, concurrency int) error {
	batchSize := numKeys / concurrency
	dbpath := path.Join(dir, "bench_tracedb")
	db, err := tracedb.Open(dbpath, nil)
	if err != nil {
		return err
	}

	fmt.Printf("Number of keys: %d\n", numKeys)
	fmt.Printf("Minimum key size: %d, maximum key size: %d\n", minKS, maxKS)
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Running tracedb benchmark...\n")

	topics := generateTopics(concurrency, minKS, maxKS)
	valSrc := make([]byte, maxVS)
	if _, err := rand.Read(valSrc); err != nil {
		return err
	}
	forceGC()

	start := time.Now()
	eg := &errgroup.Group{}

	func(concurrent int) error {
		i := 0
		for {
			eg.Go(func() error {
				err = db.Batch(func(b *tracedb.Batch) error {
					rnd := rand.New(rand.NewSource(int64(rand.Uint64())))
					for k := 0; k < batchSize; k++ {
						b.Put(topics[i], randValue(rnd, valSrc, minVS, maxVS))
					}
					err := b.Write()
					return err
				})
				return err
			})
			if i >= concurrent-1 {
				return nil
			}
			i++
		}
	}(concurrency)

	err = eg.Wait()
	if err != nil {
		return err
	}

	endsecs := time.Since(start).Seconds()
	totalalsecs := endsecs
	fmt.Printf("Put: %.3f sec, %d ops/sec\n", endsecs, int(float64(numKeys)/endsecs))

	sz, err := db.FileSize()
	if err != nil {
		return err
	}
	fmt.Printf("Fie sie: %s\n", byteSize(sz))
	printStats(db)

	// if err := db.Close(); err != nil {
	// 	return err
	// }
	// db, err = tracedb.Open(dbpath, nil)
	// if err != nil {
	// 	return err
	// }
	// forceGC()

	start = time.Now()
	func(concurrent int) error {
		i := 0
		for {
			topic := append(topics[i], []byte("?last=1m")...)
			_, err := db.Get(&tracedb.Query{Topic: topic})
			if err != nil {
				return err
			}
			if i >= concurrent-1 {
				return nil
			}
			i++
		}
	}(concurrency)

	endsecs = time.Since(start).Seconds()
	totalalsecs += endsecs
	fmt.Printf("Get: %.3f sec, %d ops/sec\n", endsecs, int(float64(numKeys)/endsecs))
	fmt.Printf("Put + Get time: %.3f sec\n", totalalsecs)
	sz, err = db.FileSize()
	if err != nil {
		return err
	}
	fmt.Printf("File size: %s\n", byteSize(sz))
	printStats(db)
	return db.Close()
}
