/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package memdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/unit-io/unitdb/hash"
)

const (
	maxShards = 27
	// maxSize value to limit maximum memory for the mem store.
	maxSize = (int64(1) << 34) - 1

	// defaultInitialInterval duration for waiting in the queue due to system memory surge operations.
	defaultInitialInterval = 500 * time.Millisecond
	// defaultRandomizationFactor sets factor to backoff when mem store reaches target size.
	defaultRandomizationFactor = 0.5
	// defaultMaxElapsedTime sets maximum elapsed time to wait during backoff.
	defaultMaxElapsedTime   = 15 * time.Second
	defaultBackoffThreshold = 0.7

	defaultDrainInterval = 1 * time.Second
	defaultDrainFactor   = 0.7
	defaultShrinkFactor  = 0.33 // shrinker try to free 33% of total mem store size.
)

var timerPool sync.Pool

// To avoid lock bottlenecks block cache is divided into several (nShards) shards.
type blockCache []*block

type block struct {
	data         dataTable
	freeOffset   int64            // mem cache keep lowest offset that can be free.
	m            map[uint64]int64 // map[key]offset
	sync.RWMutex                  // Read Write mutex, guards access to internal map.
}

// newBlockCache creates a new concurrent block cache.
func newBlockCache(nShards int) blockCache {
	m := make(blockCache, nShards)
	for i := 0; i < nShards; i++ {
		m[i] = &block{data: dataTable{}, m: make(map[uint64]int64)}
	}
	return m
}

// DB represents the block cache mem store.
// All DB methods are safe for concurrent use by multiple goroutines.
type (
	// Capacity manages the mem store capacity to limit excess memory usage.
	Capacity struct {
		sync.RWMutex

		size       int64
		targetSize int64

		InitialInterval     time.Duration
		RandomizationFactor float64
		currentInterval     time.Duration
		MaxElapsedTime      time.Duration

		WriteBackOff bool
	}
	DB struct {
		drainLockC chan struct{}

		// block cache
		consistent *hash.Consistent
		blockCache blockCache

		// Capacity
		nShards int
		cap     *Capacity

		// close
		closeW sync.WaitGroup
		closeC chan struct{}
	}
)

// Open opens or creates a new mem store.
func Open(size int64, opts *Options) (*DB, error) {
	opts = opts.copyWithDefaults()
	if size > maxSize {
		size = maxSize
	}

	cap := &Capacity{
		targetSize:          size,
		InitialInterval:     opts.InitialInterval,
		RandomizationFactor: opts.RandomizationFactor,
		MaxElapsedTime:      opts.MaxElapsedTime,

		WriteBackOff: opts.WriteBackOff,
	}
	cap.Reset()

	db := &DB{
		drainLockC: make(chan struct{}, 1),
		blockCache: newBlockCache(opts.MaxShards),

		// Capacity
		nShards: opts.MaxShards,
		cap:     cap,

		// Close
		closeC: make(chan struct{}),
	}

	db.consistent = hash.InitConsistent(int(opts.MaxShards), int(opts.MaxShards))

	go db.drain(opts.DrainFactor, opts.DrainInterval)

	return db, nil
}

func (db *DB) drain(drainFactor float64, interval time.Duration) {
	drainTicker := time.NewTicker(interval)
	defer drainTicker.Stop()
	for {
		select {
		case <-db.closeC:
			return
		case <-drainTicker.C:
			if db.Capacity() > drainFactor {
				db.shrinkDataTable()
			}
		}
	}
}

func (db *DB) shrinkDataTable() error {
	db.drainLockC <- struct{}{}
	db.closeW.Add(1)
	defer func() {
		db.closeW.Done()
		<-db.drainLockC
	}()

	for i := 0; i < db.nShards; i++ {
		block := db.blockCache[i]
		block.Lock()
		if block.freeOffset > 0 {
			if err := block.data.shrink(block.freeOffset); err != nil {
				block.Unlock()
				return err
			}
			db.cap.Lock()
			db.cap.size -= int64(block.freeOffset)
			db.cap.Unlock()
		}
		for seq, off := range block.m {
			if off < block.freeOffset {
				delete(block.m, seq)
			} else {
				block.m[seq] = off - block.freeOffset
			}
		}
		block.freeOffset = 0
		block.Unlock()
	}

	db.cap.Reset()

	return nil
}

// Close closes the mem store.
func (db *DB) Close() error {
	// Signal all goroutines.
	close(db.closeC)

	// Acquire lock.
	db.drainLockC <- struct{}{}

	// Wait for all goroutines to exit.
	db.closeW.Wait()
	return nil
}

// getBlock returns block under given blockID.
func (db *DB) getBlock(blockID uint64) *block {
	return db.blockCache[db.consistent.FindBlock(blockID)]
}

// Get gets data for the provided key under a blockID.
func (db *DB) Get(blockID uint64, key uint64) ([]byte, error) {
	// Get block
	block := db.getBlock(blockID)
	block.RLock()
	defer block.RUnlock()
	// Get item from block.
	off, ok := block.m[key]
	if off == -1 {
		return nil, errors.New("entry deleted")
	}
	if !ok {
		return nil, nil
	}
	scratch, err := block.data.readRaw(off, 4) // read data length.
	if err != nil {
		return nil, err
	}
	dataLen := binary.LittleEndian.Uint32(scratch[:4])
	data, err := block.data.readRaw(off, dataLen)
	if err != nil {
		return nil, err
	}
	return data[4:], nil
}

// Remove sets data offset to -1 for the key under a blockID.
func (db *DB) Remove(blockID uint64, key uint64) error {
	// Get block
	block := db.getBlock(blockID)
	block.RLock()
	defer block.RUnlock()
	// Get item from block.
	if _, ok := block.m[key]; ok {
		block.m[key] = -1
	}
	return nil
}

// Set sets the value for the given entry for a blockID.
func (db *DB) Set(blockID uint64, key uint64, data []byte) error {
	if db.cap.WriteBackOff {
		t := db.cap.NewTicker()
		select {
		case <-t.C:
			timerPool.Put(t)
		}
	}
	// Get block
	block := db.getBlock(blockID)
	block.Lock()
	defer block.Unlock()
	dataLen := uint32(len(data) + 4)
	off, err := block.data.allocate(dataLen)
	if err != nil {
		return err
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], dataLen)

	if _, err := block.data.writeAt(scratch[:], off); err != nil {
		return err
	}
	if _, err := block.data.writeAt(data, off+4); err != nil {
		return err
	}
	block.m[key] = off

	db.cap.Lock()
	defer db.cap.Unlock()
	db.cap.size += int64(dataLen)
	return nil
}

// Keys gets all keys from block cache for the provided blockID.
func (db *DB) Keys(blockID uint64) []uint64 {
	// Get block
	block := db.getBlock(blockID)
	block.RLock()
	defer block.RUnlock()
	// Get keys from  block.
	keys := make([]uint64, 0, len(block.m))
	for k := range block.m {
		keys = append(keys, k)
	}
	return keys
}

// Free free keeps first offset that can be free if mem store exceeds target size.
func (db *DB) Free(blockID, key uint64) error {
	// Get block
	block := db.getBlock(blockID)
	block.Lock()
	defer block.Unlock()
	if block.freeOffset > 0 {
		return nil
	}
	off, ok := block.m[key]
	// Get item from block.
	if ok {
		if (block.freeOffset == 0 || block.freeOffset < off) && float64(off) > float64(block.data.size)*defaultShrinkFactor {
			block.freeOffset = off
		}
	}

	return nil
}

// Count returns the number of items in mem store.
func (db *DB) Count() uint64 {
	count := 0
	for i := 0; i < db.nShards; i++ {
		block := db.blockCache[i]
		block.RLock()
		count += len(block.m)
		block.RUnlock()
	}
	return uint64(count)
}

// Size returns the total size of mem store.
func (db *DB) Size() (int64, error) {
	size := int64(0)
	for i := 0; i < db.nShards; i++ {
		block := db.blockCache[i]
		block.RLock()
		size += int64(block.data.size)
		block.RUnlock()
	}
	return size, nil
}

// Capacity return the mem store capacity in proportion to target size.
func (db *DB) Capacity() float64 {
	db.cap.RLock()
	defer db.cap.RUnlock()
	return float64(db.cap.size) / float64(db.cap.targetSize)
}

// Reset the interval back to the initial interval.
// Reset must be called before using db.
func (cap *Capacity) Reset() {
	cap.Lock()
	defer cap.Unlock()
	cap.currentInterval = cap.InitialInterval
}

// NextBackOff calculates the next backoff interval using the formula:
// 	Randomized interval = RetryInterval * (1 ± RandomizationFactor).
func (cap *Capacity) NextBackOff(multiplier float64) time.Duration {
	defer cap.incrementCurrentInterval(multiplier)
	return getRandomValueFromInterval(cap.RandomizationFactor, rand.Float64(), cap.currentInterval)
}

// Increments the current interval by multiplying it with the multiplier.
func (cap *Capacity) incrementCurrentInterval(multiplier float64) {
	cap.Lock()
	defer cap.Unlock()
	cap.currentInterval = time.Duration(float64(cap.currentInterval) * multiplier)
	if cap.currentInterval > cap.MaxElapsedTime {
		cap.currentInterval = cap.MaxElapsedTime
	}
}

// Decrements the current interval by multiplying it with factor.
func (cap *Capacity) decrementCurrentInterval(factor float64) {
	cap.currentInterval = time.Duration(float64(cap.currentInterval) * factor)
}

// Returns a random value from the following interval:
// [currentInterval - randomizationFactor * currentInterval, currentInterval + randomizationFactor * currentInterval].
func getRandomValueFromInterval(randomizationFactor, random float64, currentInterval time.Duration) time.Duration {
	var delta = randomizationFactor * float64(currentInterval)
	var minInterval = float64(currentInterval) - delta
	var maxInterval = float64(currentInterval) + delta

	// Get a random value from the range [minInterval, maxInterval].
	// The formula used below has a +1 because if the minInterval is 1 and the maxInterval is 3 then
	// we want a 33% chance for selecting either 1, 2 or 3.
	return time.Duration(minInterval + (random * (maxInterval - minInterval + 1)))
}

// NewTicker creates or get ticker from timer db. It uses backoff duration of the mem store for the timer.
func (cap *Capacity) NewTicker() *time.Timer {
	cap.RLock()
	factor := float64(cap.size) / float64(cap.targetSize)
	cap.RUnlock()
	d := time.Duration(time.Duration(factor) * time.Millisecond)
	if d > 1 {
		d = cap.NextBackOff(factor)
	}

	if v := timerPool.Get(); v != nil {
		t := v.(*time.Timer)
		if t.Reset(d) {
			panic(fmt.Sprintf("db.NewTicker: active timer trapped to the pool"))
		}
		return t
	}
	return time.NewTimer(d)
}

// Backoff backs-off mem store if the currentInterval is greater than Backoff threshold.
func (db *DB) Backoff() {
	t := db.cap.NewTicker()
	select {
	case <-t.C:
		timerPool.Put(t)
	}
}
