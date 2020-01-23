package tracedb

import (
	"sort"
	"sync/atomic"
	"time"

	"github.com/unit-io/tracedb/bpool"
	"github.com/unit-io/tracedb/memdb"
	"github.com/unit-io/tracedb/uid"
	"golang.org/x/sync/errgroup"
)

type (
	tinyBatchInfo struct {
		entryCount uint32
	}

	tinyBatch struct {
		Id uid.LID
		tinyBatchInfo
		buffer *bpool.Buffer
		logs   []log
	}
)

func (b *tinyBatch) reset() {
	b.Id = uid.NewLID()
	b.entryCount = 0
	atomic.StoreUint32(&b.entryCount, 0)
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
	mem         *memdb.DB
	batchQueue  chan *Batch
	commitQueue chan *Batch

	opts    *Options
	bufPool *bpool.BufferPool
	//tiny Batch
	tinyBatch *tinyBatch
	//once run batchLoop once
	once Once
}

// Batch starts a new batch.
func (db *DB) batch() *Batch {
	opts := DefaultBatchOptions
	opts.Encryption = db.encryption == 1
	b := &Batch{opts: opts, batchId: uid.NewLID(), db: db}
	b.buffer = db.bufPool.Get()

	return b
}

func (db *DB) initbatchdb(opts *Options) error {
	bdb := &batchdb{
		// batchDB
		opts:        opts,
		bufPool:     bpool.NewBufferPool(opts.BufferSize),
		tinyBatch:   &tinyBatch{Id: uid.NewLID()},
		batchQueue:  make(chan *Batch, 100),
		commitQueue: make(chan *Batch, 1),
	}

	db.batchdb = bdb
	// Create a memdb.
	mem, err := memdb.Open("memdb", opts.MemdbSize)
	if err != nil {
		return err
	}
	db.mem = mem
	db.tinyBatch.buffer = db.bufPool.Get()
	// add close wait for commits
	db.tinyBatchLoop(opts.TinyBatchWriteInterval)
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
	// ctx, cancel := context.WithCancel(context.Background())
	// db.cancelCommit = cancel
	db.closeW.Add(1)
	tinyBatchWriterTicker := time.NewTicker(interval)
	go func() {
		defer func() {
			db.closeW.Done()
			tinyBatchWriterTicker.Stop()
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
