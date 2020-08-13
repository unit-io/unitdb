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

package unitdb

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/message"
	"golang.org/x/sync/errgroup"
)

type (
	tinyBatchInfo struct {
		entryCount uint32
	}

	tinyBatch struct {
		ID int64
		tinyBatchInfo
		buffer *bpool.Buffer
	}
)

func (b *tinyBatch) reset(timeID int64) {
	b.ID = timeID
	b.entryCount = 0
	atomic.StoreUint32(&b.entryCount, 0)
}

func (b *tinyBatch) timeID() int64 {
	return atomic.LoadInt64(&b.ID)
}

func (b *tinyBatch) count() uint32 {
	return atomic.LoadUint32(&b.entryCount)
}

func (b *tinyBatch) incount() uint32 {
	return atomic.AddUint32(&b.entryCount, 1)
}

// batchdb manages the batch execution
type batchdb struct {
	// batchDB.
	batchQueue  chan *Batch
	commitQueue chan *Batch

	bufPool *bpool.BufferPool
	//tiny Batch
	tinyBatch *tinyBatch
}

// Batch starts a new batch.
func (db *DB) batch() *Batch {
	opts := &options{}
	WithDefaultBatchOptions().set(opts)
	opts.batchOptions.encryption = db.encryption == 1
	b := &Batch{ID: db.timeWindow.timeID(), opts: opts, db: db, topics: make(map[uint64]*message.Topic)}
	b.buffer = db.bufPool.Get()

	return b
}

func (db *DB) initbatchdb(opts *options) error {
	bdb := &batchdb{
		// batchDB
		bufPool:     bpool.NewBufferPool(opts.bufferSize, nil),
		tinyBatch:   &tinyBatch{ID: db.timeWindow.timeID()},
		batchQueue:  make(chan *Batch, 100),
		commitQueue: make(chan *Batch, 1),
	}

	db.batchdb = bdb
	db.tinyBatch.buffer = db.bufPool.Get()

	db.tinyBatchLoop(opts.tinyBatchWriteInterval)
	return nil
}

// Batch executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is written.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the write is
// returned from the Batch() method.
//
// Attempting to manually commit or rollback within the function will cause a panic.
func (db *DB) Batch(fn func(*Batch, <-chan struct{}) error) error {
	b := db.batch()

	b.setManaged()
	b.commitComplete = make(chan struct{})
	// If an error is returned from the function then rollback and return error.
	if err := fn(b, b.commitComplete); err != nil {
		b.Abort()
		close(b.commitComplete)
		return err
	}
	b.unsetManaged()
	// defer b.Abort()
	return b.Commit()
}

// BatchGroup runs multiple batches concurrently without causing conflicts
type BatchGroup struct {
	fn []func(*Batch, <-chan struct{}) error
	*DB
	// batches []Batch
}

// NewBatchGroup create new group to runs multiple batches concurrently without causing conflicts
func (db *DB) NewBatchGroup() *BatchGroup {
	return &BatchGroup{DB: db}
}

// Add adds a function to the Group.
// The function will be executed in its own goroutine when Run is called.
// Add must be called before Run.
func (g *BatchGroup) Add(fn func(*Batch, <-chan struct{}) error) {
	g.fn = append(g.fn, fn)
}

// Run exectues each function registered via Add in its own goroutine.
// Run blocks until all functions have returned.
// The first function to return will trigger the closure of the channel
// passed to each function, who should in turn, return.
// The return value from the first function to exit will be returned to
// the caller of Run.
func (g *BatchGroup) Run() error {
	// if there are no registered functions, return immediately.
	if len(g.fn) < 1 {
		return nil
	}

	stop := make(chan struct{})
	eg := &errgroup.Group{}
	g.batchQueue = make(chan *Batch, len(g.fn))
	for i, fn := range g.fn {
		func(order int, fn func(*Batch, <-chan struct{}) error) {
			b := g.batch()
			b.setManaged()
			b.setGrouped(g)
			b.setOrder(int8(order))
			eg.Go(func() error {
				return fn(b, stop)
			})
		}(i, fn)
	}

	// Check whether any of the goroutines failed. Since eg is accumulating the
	// errors, we don't need to send them (or check for them) in the individual
	// results sent on the channel.
	if err := eg.Wait(); err != nil {
		return err
	}

	close(g.batchQueue)
	eg.Go(func() error {
		defer func() {
			g.Abort()
			close(stop)
		}()
		return g.writeBatchGroup()
	})

	return eg.Wait()
}

func (g *BatchGroup) writeBatchGroup() error {
	g.closeW.Add(1)
	defer g.closeW.Done()
	var batches []*Batch
	for batch := range g.batchQueue {
		batches = append(batches, batch)
	}
	sort.Slice(batches[:], func(i, j int) bool {
		return batches[i].order < batches[j].order
	})
	b := g.batch()
	b.commitComplete = make(chan struct{})
	for _, batch := range batches {
		logger.Debug().Str("Context", "batchdb.writeBatchGroup").Int8("oder", batch.order).Int("length", len(g.batchQueue))
		batch.index = append(batch.index, batch.pendingWrites...)
		b.append(batch)
	}
	err := b.Write()
	if err != nil {
		return err
	}
	return b.Commit()
}

// tinyBatchLoop handles tiny bacthes write
func (db *DB) tinyBatchLoop(interval time.Duration) {
	db.closeW.Add(1)
	tinyBatchWriterTicker := time.NewTicker(interval)
	go func() {
		defer func() {
			tinyBatchWriterTicker.Stop()
			db.closeW.Done()
		}()
		for {
			select {
			case <-tinyBatchWriterTicker.C:
				if err := db.tinyCommit(); err != nil {
					logger.Error().Err(err).Str("context", "tinyBatchLoop").Msgf("Error committing tinyBatch")
				}
			case <-db.closeC:
				return
			}
		}
	}()
}

//Abort abort is a batch cleanup operation on batch group complete
func (g *BatchGroup) Abort() {
	for b := range g.batchQueue {
		b.unsetManaged()
		b.Abort()
	}
}
