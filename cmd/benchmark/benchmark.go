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

func forceGC() {
	runtime.GC()
	time.Sleep(time.Millisecond * 500)
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

func generateVals(count int, minL int, maxL int) [][]byte {
	vals := make([][]byte, 0, count)
	seen := make(map[string]struct{}, count)
	for len(vals) < count {
		v := randKey(minL, maxL)
		if _, ok := seen[v]; ok {
			continue
		}
		seen[v] = struct{}{}
		val := make([]byte, len(v)+5)
		val = append(val, []byte("msg.")...)
		val = append(val, []byte(v)...)
		vals = append(vals, val)
	}
	return vals
}

func printStats(db *tracedb.DB) {
	fmt.Printf("%+v\n", db.Metrics())
}

func showProgress(gid int, total int) {
	fmt.Printf("Goroutine %d. Processed %d items...\n", gid, total)
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
	vals := generateVals(numKeys, minVS, maxVS)
	forceGC()

	start := time.Now()
	eg := &errgroup.Group{}

	func(concurrent int) error {
		i := 1
		for {
			eg.Go(func() error {
				err = db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
					for k := 0; k < batchSize; k++ {
						b.PutEntry(&tracedb.Entry{Topic: topics[i-1], Payload: vals[k]})
					}
					err := b.Write()
					return err
				})
				return err
			})
			if i >= concurrent {
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
		i := 1
		for {
			topic := append(topics[i-1], []byte("?last=1m")...)
			_, err := db.Get(&tracedb.Query{Topic: topic})
			if err != nil {
				return err
			}
			if i >= concurrent {
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

func benchmark2(dir string, numKeys int, minKS int, maxKS int, minVS int, maxVS int, concurrency int) error {
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
	vals := generateVals(numKeys, minVS, maxVS)
	forceGC()

	start := time.Now()
	eg := &errgroup.Group{}

	func(concurrent int) error {
		i := 1
		for {
			eg.Go(func() error {
				for k := 0; k < batchSize; k++ {
					if err := db.PutEntry(&tracedb.Entry{Topic: topics[i-1], Payload: vals[k]}); err != nil {
						return err
					}
				}
				return err
			})
			if i >= concurrent {
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
		i := 1
		for {
			topic := append(topics[i-1], []byte("?last=1m")...)
			_, err := db.Get(&tracedb.Query{Topic: topic})
			if err != nil {
				return err
			}
			if i >= concurrent {
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

func generateKeys(count int, minL int, maxL int, db *tracedb.DB) map[uint32][][]byte {
	keys := make(map[uint32][][]byte, count/1000)
	seen := make(map[string]struct{}, count)
	contract, _ := db.NewContract()
	keyCount := 0
	for len(keys)*1000 < count {
		k := randKey(minL, maxL)
		if _, ok := seen[k]; ok {
			continue
		}
		if keyCount%1000 == 0 {
			contract, _ = db.NewContract()
		}
		seen[k] = struct{}{}
		topic := make([]byte, len(k)+5)
		topic = append(topic, []byte("dev18.")...)
		topic = append(topic, []byte(k)...)
		keys[contract] = append(keys[contract], topic)
		keyCount++
	}
	return keys
}

func benchmark3(dir string, numKeys int, minKS int, maxKS int, minVS int, maxVS int, concurrency int, progress bool) error {
	batchSize := numKeys / concurrency
	dbpath := path.Join(dir, "bench_tracedb")
	db, err := tracedb.Open(dbpath, nil)
	if err != nil {
		return err
	}

	fmt.Printf("Number of keys: %d\n", numKeys)
	fmt.Printf("Minimum key size: %d, maximum key size: %d\n", minKS, maxKS)
	fmt.Printf("Minimum value size: %d, maximum value size: %d\n", minVS, maxVS)
	fmt.Printf("Concurrency: %d\n", concurrency)
	fmt.Printf("Running tracedb benchmark...\n")

	keys := generateKeys(batchSize, minKS, maxKS, db)
	vals := generateVals(batchSize, minVS, maxVS)
	forceGC()

	start := time.Now()
	eg := &errgroup.Group{}

	func(concurrent int) error {
		i := 1
		for {
			eg.Go(func() error {
				err = db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
					for contract := range keys {
						for i, k := range keys[contract] {
							b.PutEntry(&tracedb.Entry{Topic: k, Payload: vals[i], Contract: contract})
						}
					}
					err := b.Write()
					return err
				})
				return err
			})
			if i >= concurrent {
				return nil
			}
			i++
		}
	}(concurrency)

	err = eg.Wait()
	if err != nil {
		return err
	}

	// if err := db.Close(); err != nil {
	// 	return err
	// }
	endsecs := time.Since(start).Seconds()
	totalalsecs := endsecs
	fmt.Printf("Put: %.3f sec, %d ops/sec\n", endsecs, int(float64(numKeys)/endsecs))
	printStats(db)
	forceGC()

	start = time.Now()
	func(concurrent int) error {
		i := 1
		for {
			eg.Go(func() error {
				for contract := range keys {
					for _, k := range keys[contract] {
						topic := append(k, []byte("?last=1m")...)
						_, err := db.Get(&tracedb.Query{Topic: topic, Contract: contract})
						if err != nil {
							return err
						}
					}
				}
				return nil
			})
			if i >= concurrent {
				return nil
			}
			i++
		}
	}(concurrency)

	err = eg.Wait()
	if err != nil {
		return err
	}
	endsecs = time.Since(start).Seconds()
	totalalsecs += endsecs
	fmt.Printf("Get: %.3f sec, %d ops/sec\n", endsecs, int(float64(numKeys)/endsecs))
	fmt.Printf("Put + Get time: %.3f sec\n", totalalsecs)
	sz, err := db.FileSize()
	if err != nil {
		return err
	}
	fmt.Printf("File size: %s\n", byteSize(sz))
	printStats(db)
	return db.Close()
}
