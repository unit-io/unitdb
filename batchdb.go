package tracedb

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/saffat-in/tracedb/memdb"
	"golang.org/x/sync/errgroup"
)

type (
	batchInfo struct {
		entryCount uint16
	}

	tinyBatch struct {
		batchInfo
		buffer *bytes.Buffer
	}
)

// batchdb manages the batch execution
type batchdb struct {
	// batchDB.
	memMu      sync.RWMutex
	memPool    chan *memdb.DB
	mem        *mem
	batchQueue chan *Batch

	opts *Options
	//tiny Batch
	tinyBatch *tinyBatch
	//once run batchLoop once
	once Once
}

// Batch starts a new batch.
func (db *DB) batch() *Batch {
	return &Batch{opts: DefaultBatchOptions, db: db}
}

func (db *DB) initbatchdb(opts *Options) error {
	bdb := &batchdb{
		// batchDB
		opts:       opts,
		tinyBatch:  &tinyBatch{buffer: bufPool.Get()},
		batchQueue: make(chan *Batch, 10),
	}

	db.batchdb = bdb
	// Create a memdb.
	if _, err := db.newMem(0); err != nil {
		return err
	}
	return nil
}

// Batch executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Update() method.
//
// Attempting to manually commit or rollback within the function will cause a panic.
func (db *DB) Batch(fn func(*Batch, <-chan struct{}) error) error {
	b := db.batch()

	b.setManaged()
	b.commitComplete = make(chan struct{})
	// If an error is returned from the function then rollback and return error.
	err := fn(b, b.commitComplete)
	if err != nil {
		return err
	}
	b.unsetManaged()
	// Make sure the transaction rolls back in the event of a panic.
	go func() {
		defer func() {
			b.Abort()
			close(b.commitComplete)
		}()
		b.Commit()
	}()
	return nil
}

// BatchGroup runs multiple batches concurrently without causing conflicts
type BatchGroup struct {
	fn []func(*Batch, <-chan struct{}) error
	*DB
	// batches []Batch
}

func (db *DB) NewBatchGroup() *BatchGroup {
	return &BatchGroup{DB: db}
}

// Add adds a function to the Group.
// The function will be exectuted in its own goroutine when Run is called.
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
	start := time.Now()
	defer logger.Debug().Str("context", "batchdb.Run").Dur("duration", time.Since(start)).Msg("")
	// if there are no registered functions, return immediately.
	if len(g.fn) < 1 {
		return nil
	}

	stop := make(chan struct{})
	eg := &errgroup.Group{}
	g.batchQueue = make(chan *Batch, len(g.fn))
	for i, fn := range g.fn {
		func(order int, fn func(*Batch, <-chan struct{}) error) {
			//TODO implement cloing to pass a copy of batch
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
	tinyBatchWriterTicker := time.NewTicker(interval)
	defer tinyBatchWriterTicker.Stop()
	go func() {
		for {
			select {
			case <-db.closeC:
				return
			case <-tinyBatchWriterTicker.C:
				if db.tinyBatch.entryCount > 0 {
					db.tinyCommit(db.tinyBatch.entryCount, db.tinyBatch.buffer.Bytes())
				}
			}
		}
	}()
}

func (g *BatchGroup) Abort() {
	for b := range g.batchQueue {
		b.unsetManaged()
		b.Abort()
	}
}

// Once is an object that will perform exactly one action
// until Reset is called.
// See http://golang.org/pkg/sync/#Once
type Once struct {
	m    sync.Mutex
	done uint32
}

// Do simulates sync.Once.Do by executing the specified function
// only once, until Reset is called.
// See http://golang.org/pkg/sync/#Once
func (o *Once) Do(f func()) {
	if atomic.LoadUint32(&o.done) == 1 {
		return
	}
	// Slow-path.
	o.m.Lock()
	defer o.m.Unlock()
	if o.done == 0 {
		defer atomic.StoreUint32(&o.done, 1)
		f()
	}
}

// Reset indicates that the next call to Do should actually be called
// once again.
func (o *Once) Reset() {
	o.m.Lock()
	defer o.m.Unlock()
	atomic.StoreUint32(&o.done, 0)
}
