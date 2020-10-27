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

package cache

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
	nBlocks = 27
	// maxSize value to limit maximum memory for the blockcache.
	maxSize = (int64(1) << 34) - 1

	// defaultInitialInterval duration for waiting in the queue due to system memory surge operations.
	defaultInitialInterval = 500 * time.Millisecond
	// defaultRandomizationFactor sets factor to backoff when blockcache reaches target size.
	defaultRandomizationFactor = 0.5
	// defaultMaxElapsedTime sets maximum elapsed time to wait during backoff.
	defaultMaxElapsedTime   = 15 * time.Second
	defaultBackoffThreshold = 0.7

	defaultDrainInterval = 1 * time.Second
	defaultDrainFactor   = 0.7
	defaultShrinkFactor  = 0.33 // shrinker try to free 33% of total blockcache size.
)

var timerPool sync.Pool

// To avoid lock bottlenecks block cache is divided into several (nShards) shards.
type _BlockCache []*_Block

type _Block struct {
	data         _DataTable
	freeOffset   int64            // blockcache keep lowest offset that can be free.
	m            map[uint64]int64 // map[key]offset
	sync.RWMutex                  // Read Write mutex, guards access to internal map.
}

// newBlockCache creates a new concurrent block cache.
func newBlockCache(nShards int) _BlockCache {
	m := make(_BlockCache, nShards)
	for i := 0; i < nShards; i++ {
		m[i] = &_Block{data: _DataTable{}, m: make(map[uint64]int64)}
	}
	return m
}

// Cache represents the blockcache.
// All Cache methods are safe for concurrent use by multiple goroutines.
type (
	// Capacity manages the blockcache capacity to limit excess memory usage.
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
	Cache struct {
		drainLockC chan struct{}

		// block cache
		consistent *hash.Consistent
		blockCache _BlockCache

		// Capacity
		nBlocks int
		cap     *Capacity

		// close
		closeW sync.WaitGroup
		closeC chan struct{}
	}
)

// Open opens or creates a new blockcache.
func Open(size int64, opts *Options) (*Cache, error) {
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

	c := &Cache{
		drainLockC: make(chan struct{}, 1),
		blockCache: newBlockCache(opts.MaxBlocks),

		// Capacity
		nBlocks: opts.MaxBlocks,
		cap:     cap,

		// Close
		closeC: make(chan struct{}),
	}

	c.consistent = hash.InitConsistent(int(opts.MaxBlocks), int(opts.MaxBlocks))

	go c.drain(opts.DrainFactor, opts.DrainInterval)

	return c, nil
}

func (c *Cache) drain(drainFactor float64, interval time.Duration) {
	drainTicker := time.NewTicker(interval)
	defer drainTicker.Stop()
	for {
		select {
		case <-c.closeC:
			return
		case <-drainTicker.C:
			if c.Capacity() > drainFactor {
				c.shrinkDataTable()
			}
		}
	}
}

func (c *Cache) shrinkDataTable() error {
	c.drainLockC <- struct{}{}
	c.closeW.Add(1)
	defer func() {
		c.closeW.Done()
		<-c.drainLockC
	}()

	for i := 0; i < c.nBlocks; i++ {
		block := c.blockCache[i]
		block.Lock()
		if block.freeOffset > 0 {
			if err := block.data.shrink(block.freeOffset); err != nil {
				block.Unlock()
				return err
			}
			c.cap.Lock()
			c.cap.size -= int64(block.freeOffset)
			c.cap.Unlock()
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

	c.cap.Reset()

	return nil
}

// Close closes the blockcache.
func (c *Cache) Close() error {
	// Signal all goroutines.
	close(c.closeC)

	// Acquire lock.
	c.drainLockC <- struct{}{}

	// Wait for all goroutines to exit.
	c.closeW.Wait()
	return nil
}

// getBlock returns block under given blockID.
func (c *Cache) getBlock(blockID uint64) *_Block {
	return c.blockCache[c.consistent.FindBlock(blockID)]
}

// Get gets data for the provided key under a blockID.
func (c *Cache) Get(blockID uint64, key uint64) ([]byte, error) {
	// Get block
	block := c.getBlock(blockID)
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
func (c *Cache) Remove(blockID uint64, key uint64) error {
	// Get block
	block := c.getBlock(blockID)
	block.Lock()
	defer block.Unlock()
	// Get item from block.
	if _, ok := block.m[key]; ok {
		block.m[key] = -1
	}
	return nil
}

// Set sets the value for the given entry for a blockID.
func (c *Cache) Set(blockID uint64, key uint64, data []byte) error {
	if c.cap.WriteBackOff {
		t := c.cap.NewTicker()
		select {
		case <-t.C:
			timerPool.Put(t)
		}
	}
	// Get block
	block := c.getBlock(blockID)
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

	c.cap.Lock()
	defer c.cap.Unlock()
	c.cap.size += int64(dataLen)
	return nil
}

// Keys gets all keys from block cache for the provided blockID.
func (c *Cache) Keys(blockID uint64) []uint64 {
	// Get block
	block := c.getBlock(blockID)
	block.RLock()
	defer block.RUnlock()
	// Get keys from  block.
	keys := make([]uint64, 0, len(block.m))
	for k := range block.m {
		keys = append(keys, k)
	}
	return keys
}

// Free free keeps first offset that can be free if blockcache exceeds target size.
func (c *Cache) Free(blockID, key uint64) error {
	// Get block
	block := c.getBlock(blockID)
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

// Count returns the number of items in blockcache.
func (c *Cache) Count() uint64 {
	count := 0
	for i := 0; i < c.nBlocks; i++ {
		block := c.blockCache[i]
		block.RLock()
		count += len(block.m)
		block.RUnlock()
	}
	return uint64(count)
}

// Size returns the total size of blockcache.
func (c *Cache) Size() (int64, error) {
	size := int64(0)
	for i := 0; i < c.nBlocks; i++ {
		block := c.blockCache[i]
		block.RLock()
		size += int64(block.data.size)
		block.RUnlock()
	}
	return size, nil
}

// Capacity return the blockcache capacity in proportion to target size.
func (c *Cache) Capacity() float64 {
	c.cap.RLock()
	defer c.cap.RUnlock()
	return float64(c.cap.size) / float64(c.cap.targetSize)
}

// Reset the interval back to the initial interval.
// Reset must be called before using c.
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

// NewTicker creates or get ticker from timer c. It uses backoff duration of the blockcache for the timer.
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
			panic(fmt.Sprintf("c.NewTicker: active timer trapped to the pool"))
		}
		return t
	}
	return time.NewTimer(d)
}

// Backoff backs-off blockcache if the currentInterval is greater than Backoff threshold.
func (c *Cache) Backoff() {
	if c.Capacity() < 1 {
		return
	}
	t := c.cap.NewTicker()
	select {
	case <-t.C:
		timerPool.Put(t)
	}
}
