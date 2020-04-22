package main

import (
	"fmt"
	"math/rand"
	"path"
	"runtime"
	"time"

	_ "net/http/pprof"

	"github.com/unit-io/tracedb"
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
	if varz, err := db.Varz(); err == nil {
		fmt.Printf("%+v\n", varz)
	}
}

func showProgress(gid int, total int) {
	fmt.Printf("Goroutine %d. Processed %d items...\n", gid, total)
}

func benchmark1(dir string, numKeys int, minKS int, maxKS int, minVS int, maxVS int, concurrency int) error {
	// p := profile.Start(profile.MemProfile, profile.ProfilePath("."), profile.NoShutdownHook)
	// defer p.Stop()
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

	func(retry int) error {
		r := 1
		for range time.Tick(100 * time.Millisecond) {
			start := time.Now()
			var entries []tracedb.Entry
			for i := 0; i < concurrency; i++ {
				topic := append(topics[i], []byte("?ttl=1m")...)
				entries = append(entries, tracedb.Entry{Topic: topic})
			}
			for _, entry := range entries {
				for k := 0; k < batchSize; k++ {
					entry.SetPayload(vals[k])
					if err := db.PutEntry(&entry); err != nil {
						return err
					}
				}
			}
			endsecs := time.Since(start).Seconds()
			fmt.Printf("Put: %d %.3f sec, %d ops/sec\n", r, endsecs, int(float64(numKeys)/endsecs))

			sz, err := db.FileSize()
			if err != nil {
				return err
			}
			fmt.Printf("File size: %s\n", byteSize(sz))
			if r >= retry {
				return nil
			}
			r++
		}
		return nil
	}(7)

	printStats(db)

	start := time.Now()

	for i := 0; i < concurrency; i++ {
		topic := append(topics[i], []byte("?last=1m")...)
		_, err := db.Get(&tracedb.Query{Topic: topic, Limit: batchSize})
		if err != nil {
			return err
		}
	}

	endsecs := time.Since(start).Seconds()
	fmt.Printf("Get: %.3f sec, %d ops/sec\n", endsecs, int(float64(numKeys)/endsecs))
	sz, err := db.FileSize()
	if err != nil {
		return err
	}
	fmt.Printf("File size: %s\n", byteSize(sz))
	printStats(db)
	time.Sleep(100 * time.Millisecond)
	return db.Close()
}

func benchmark2(dir string, numKeys int, minKS int, maxKS int, minVS int, maxVS int, concurrency int) error {
	// p := profile.Start(profile.MemProfile, profile.ProfilePath("."), profile.NoShutdownHook)
	// defer p.Stop()
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

	func(retry int) error {
		r := 1
		for range time.Tick(100 * time.Millisecond) {
			start := time.Now()

			func(concurrent int) error {
				i := 1
				for {
					db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
						opts := tracedb.DefaultBatchOptions
						opts.AllowDuplicates = true
						opts.Topic = append(topics[i-1], []byte("?ttl=1h")...)
						b.SetOptions(opts)
						for k := 0; k < batchSize; k++ {
							b.Put(vals[k])
						}
						if err := b.Write(); err != nil {
							return err
						}
						return nil
					})
					if i >= concurrent {
						return nil
					}
					i++
				}
			}(concurrency)

			endsecs := time.Since(start).Seconds()
			fmt.Printf("Put: %d %.3f sec, %d ops/sec\n", r, endsecs, int(float64(numKeys)/endsecs))

			sz, err := db.FileSize()
			if err != nil {
				return err
			}
			fmt.Printf("File size: %s\n", byteSize(sz))
			if r >= retry {
				return nil
			}
			r++
		}
		return nil
	}(7)

	printStats(db)
	time.Sleep(100 * time.Millisecond)
	if err := db.Close(); err != nil {
		return err
	}

	db, err = tracedb.Open(dbpath, nil)
	if err != nil {
		return err
	}

	sz, err := db.FileSize()
	if err != nil {
		return err
	}
	fmt.Printf("File size: %s\n", byteSize(sz))
	printStats(db)

	return db.Close()
}

func benchmark3(dir string, numKeys int, minKS int, maxKS int, minVS int, maxVS int, concurrency int) error {
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
			db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
				opts := tracedb.DefaultBatchOptions
				opts.AllowDuplicates = true
				opts.Topic = append(topics[i-1], []byte("?ttl=1h")...)
				b.SetOptions(opts)
				for k := 0; k < batchSize; k++ {
					b.Put(vals[k])
				}
				return b.Write()
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
	fmt.Printf("File size: %s\n", byteSize(sz))
	printStats(db)

	forceGC()

	start = time.Now()

	for i := 0; i < concurrency; i++ {
		topic := append(topics[i], []byte("?last=1m")...)
		_, err := db.Get(&tracedb.Query{Topic: topic, Limit: batchSize})
		if err != nil {
			return err
		}
	}

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
	time.Sleep(100 * time.Millisecond)
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

func benchmark4(dir string, numKeys int, minKS int, maxKS int, minVS int, maxVS int, concurrency int) error {
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
			db.Batch(func(b *tracedb.Batch, completed <-chan struct{}) error {
				opts := tracedb.DefaultBatchOptions
				opts.AllowDuplicates = true
				b.SetOptions(opts)
				for contract := range keys {
					for _, k := range keys[contract] {
						topic := append(k, []byte("?ttl=1m")...)
						b.PutEntry(&tracedb.Entry{Topic: topic, Payload: vals[i], Contract: contract})
					}
				}
				return b.Write()
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
	printStats(db)
	forceGC()

	for contract := range keys {
		for _, k := range keys[contract] {
			k = append(k, []byte("?last=1m")...)
		}
	}

	start = time.Now()

	for contract := range keys {
		for _, k := range keys[contract] {
			_, err := db.Get(&tracedb.Query{Topic: k, Contract: contract})
			if err != nil {
				return err
			}
		}
	}

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
	time.Sleep(100 * time.Millisecond)
	return db.Close()
}

func recovery(dir string) error {
	// open database for recovery
	dbpath := path.Join(dir, "bench_tracedb")
	db, err := tracedb.Open(dbpath, nil)
	if err != nil {
		return err
	}
	sz, err := db.FileSize()
	if err != nil {
		return err
	}
	fmt.Printf("File size: %s\n", byteSize(sz))
	printStats(db)

	return db.Close()
}
