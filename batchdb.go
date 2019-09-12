package tracedb

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// batchdb manages the batch execution
type batchdb struct {
	// batchDB.
	memMu   sync.RWMutex
	memPool chan *memdb
	mem     *memdb
	// Active batches keeps batches in progress with batch seq as key and array of index hash
	activeBatches map[uint64][]uint32
	batchQueue    chan *Batch
	//once run batchLoop once
	once Once

	cancelBatchWriter context.CancelFunc
}

// Batch starts a new batch.
func (db *DB) Batch() *Batch {
	return &Batch{db: db}
}

func (db *DB) initbatchdb() error {
	bdb := &batchdb{
		// batchDB
		memPool:       make(chan *memdb, 1),
		activeBatches: make(map[uint64][]uint32, 100),
		batchQueue:    make(chan *Batch, 1),
	}
	db.batchdb = bdb
	// Create a memdb.
	if _, err := db.newmemdb(0); err != nil {
		return err
	}
	db.mem.incref()
	return nil
}

// Update executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Update() method.
//
// Attempting to manually commit or rollback within the function will cause a panic.
func (db *DB) Update(fn func(*Batch) error) error {
	b := db.Batch()
	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		b.Abort()
	}()

	b.setManaged()

	// If an error is returned from the function then rollback and return error.
	err := fn(b)
	if err != nil {
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
	defer logger.Print("BatchGroup.Run: ", time.Since(start))
	// if there are no registered functions, return immediately.
	if len(g.fn) < 1 {
		return nil
	}

	stop := make(chan struct{})

	var wg sync.WaitGroup
	wg.Add(len(g.fn))

	result := make(chan error, len(g.fn)+1)
	for i, fn := range g.fn {
		go func(order int, fn func(*Batch, <-chan struct{}) error) {
			//TODO implement cloing to pass a copy of batch
			b := g.Batch()
			b.setManaged()
			b.setGrouped(g)
			b.setOrder(int8(order))
			defer func() {
				wg.Done()
			}()
			// If an error is returned from the function then rollback and return error.
			result <- fn(b, stop)
		}(i, fn)
	}

	go g.writeBatchGroup()

	defer close(g.batchQueue)
	defer wg.Wait()
	defer close(stop)

	return <-result
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
	b := g.Batch()
	for _, batch := range batches {
		logger.Printf("Batchdb: writing now...%d length %d", batch.order, len(g.batchQueue))
		batch.index = append(batch.index, batch.pendingWrites...)
		b.append(batch)
	}
	err := b.Write()
	if err != nil {
		return err
	}
	defer g.Abort()
	return b.Commit()
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
