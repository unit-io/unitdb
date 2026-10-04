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
	"sync"

	"github.com/unit-io/unitdb/internal/lockcheck"
)

// Lock order. A goroutine holding a lock takes only locks below it here:
//
//  1. _TinyLogManager.rotateMu: held by Put and Delete, for reading, from
//     reading the time ID until their entries are in the block.
//  2. an index shard's lock (_IndexShard): one shard at a time, across a
//     put, delete or get of its keys.
//  3. the time lock of a time ID (_TimeLock): held by Put and Delete, for
//     reading, and by the commit loop writing the block's log.
//  4. _TinyLogManager.mu: the current tiny log.
//  5. a _Block's lock: one block at a time.
//  6. _DB.logMu: the blocks' logs, and what keeps them in the WAL.
//  7. DB.mu: the map of time blocks.
//  8. _TimeMark's lock.
//  9. a _TinyLog's lock.
//
// Taking them out of order deadlocks two goroutines taking them in order.
// The tests check the order when built with the lockcheck tag (go test
// -tags lockcheck), which CI does. What they don't check: a goroutine must
// not wait on a channel holding a lock that the goroutines it waits for
// take. Rotation holds rotateMu and mu sending a log to the write queue,
// which the commit loop drains: the commit loop takes neither, and reads
// the current time ID without a lock (timeID).
const (
	rankRotate = iota + 1
	rankIndex
	rankTimeLock
	rankManager
	rankBlock
	rankLog
	rankDB
	rankTimeMark
	rankTinyLog
)

// ranker gives a lock its rank in the order, and its name.
type ranker interface {
	rank() (int, string)
}

type (
	rotateRank   struct{}
	timeLockRank struct{}
	managerRank  struct{}
	blockRank    struct{}
	logRank      struct{}
	dbRank       struct{}
	indexRank    struct{}
	timeMarkRank struct{}
	tinyLogRank  struct{}
)

func (rotateRank) rank() (int, string)   { return rankRotate, "rotateMu" }
func (timeLockRank) rank() (int, string) { return rankTimeLock, "time lock" }
func (managerRank) rank() (int, string)  { return rankManager, "log manager" }
func (blockRank) rank() (int, string)    { return rankBlock, "block" }
func (logRank) rank() (int, string)      { return rankLog, "logMu" }
func (dbRank) rank() (int, string)       { return rankDB, "db.mu" }
func (indexRank) rank() (int, string)    { return rankIndex, "index shard" }
func (timeMarkRank) rank() (int, string) { return rankTimeMark, "time mark" }
func (tinyLogRank) rank() (int, string)  { return rankTinyLog, "tiny log" }

// rwMutex is a sync.RWMutex of rank R in the lock order.
type rwMutex[R ranker] struct {
	mu sync.RWMutex
}

func (m *rwMutex[R]) Lock() {
	var r R
	lockcheck.Acquire(r.rank())
	m.mu.Lock()
}

func (m *rwMutex[R]) Unlock() {
	var r R
	n, _ := r.rank()
	m.mu.Unlock()
	lockcheck.Release(n)
}

func (m *rwMutex[R]) RLock() {
	var r R
	lockcheck.Acquire(r.rank())
	m.mu.RLock()
}

func (m *rwMutex[R]) RUnlock() {
	var r R
	n, _ := r.rank()
	m.mu.RUnlock()
	lockcheck.Release(n)
}

// mutex is a sync.Mutex of rank R in the lock order.
type mutex[R ranker] struct {
	mu sync.Mutex
}

func (m *mutex[R]) Lock() {
	var r R
	lockcheck.Acquire(r.rank())
	m.mu.Lock()
}

func (m *mutex[R]) Unlock() {
	var r R
	n, _ := r.rank()
	m.mu.Unlock()
	lockcheck.Release(n)
}
