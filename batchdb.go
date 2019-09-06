package tracedb

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type batchdb struct {
	// batchDB.
	writeLockC chan struct{}
	memMu      sync.RWMutex
	memPool    chan *memdb
	mem        *memdb
	// Active batches keeps batches in progress with batch seq as key and array of index hash
	activeBatches map[uint64][]uint32
	//bonce run batchLoop once
	bonce Once
	//bwg is batch wait group to process batchqueue
	bwg sync.WaitGroup
	//batches keeps batches with batch order
	batches           []Batch
	batchC            chan struct{}
	cancelBatchWriter context.CancelFunc
}

func (db *DB) newbatchdb() (*batchdb, error) {
	bdb := &batchdb{
		// batchDB
		writeLockC:    make(chan struct{}, 1),
		memPool:       make(chan *memdb, 1),
		activeBatches: make(map[uint64][]uint32, 100),
		batchC:        make(chan struct{}),
	}
	db.batchdb = bdb
	// Create a memdb.
	if _, err := db.newmemdb(0); err != nil {
		return nil, err
	}
	return bdb, nil
}

// Batch starts a new batch.
func (db *DB) Batch(opt *BatchOptions) (*Batch, error) {
	if opt.Order > 0 {
		db.bwg.Add(1)
		db.bonce.Do(func() {
			//start batchLoop
			db.batchC = make(chan struct{})
			go db.batchLoop(opt.reconnectInterval, opt.retryTimeout)
		})
	}
	return db.initBatch(opt)
}

func (db *DB) initBatch(opt *BatchOptions) (*Batch, error) {
	b := &Batch{}
	b.init(opt, db)
	return b, nil
}

// Update executes a function within the context of a read-write managed transaction.
// If no error is returned from the function then the transaction is committed.
// If an error is returned then the entire transaction is rolled back.
// Any error that is returned from the function or returned from the commit is
// returned from the Update() method.
//
// Attempting to manually commit or rollback within the function will cause a panic.
func (db *DB) Update(opt *BatchOptions, fn func(*Batch) error) error {
	opt = opt.copyWithDefaults()
	b, err := db.Batch(opt)
	if err != nil {
		return err
	}
	// Make sure the transaction rolls back in the event of a panic.
	defer func() {
		b.Abort()
	}()

	// If an error is returned from the function then rollback and return error.
	err = fn(b)
	if err != nil {
		return err
	}
	return b.Commit()
}

// Count returns the number of items in the DB.
func (db *DB) Count() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.count
}

// Metrics returns the DB metrics.
func (db *DB) Metrics() Metrics {
	return db.metrics
}

// FileSize returns the total size of the disk storage used by the DB.
func (db *DB) FileSize() (int64, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	var err error
	is, err := db.index.Stat()
	if err != nil {
		return -1, err
	}
	ds, err := db.data.Stat()
	if err != nil {
		return -1, err
	}
	return is.Size() + ds.Size(), nil
}

// batchLoop handles bacthes set to write orderly
func (db *DB) batchLoop(reconnectInterval, retryTimeout time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	db.cancelBatchWriter = cancel
	var (
		reconnectC <-chan time.Time
	)

	if reconnectInterval > 0 {
		reconnectTicker := time.NewTicker(reconnectInterval)
		defer reconnectTicker.Stop()
		reconnectC = reconnectTicker.C
	}

WRITEQUEUE:
	for {
		select {
		case <-ctx.Done():
			return
		case <-reconnectC:
			goto WAIT
		case <-db.batchC:
			if len(db.batches) == 0 {
				goto WAIT
			}
			b, err := db.Batch(DefaultBatchOptions)
			if err != nil {
				return
			}
			db.memMu.Lock()
			defer func() {
				db.batches = db.batches[:0]
				b.Abort()
				db.bonce.Reset()
				db.memMu.Unlock()
			}()
			sort.Slice(db.batches, func(i, j int) bool { return db.batches[i].order < db.batches[j].order })
			for _, batch := range db.batches {
				batch.index = append(batch.index, batch.pendingWrites...)
				b.append(&batch)
				batch.Reset()
			}
			b.Write()
			b.Commit()
			return
		default:
			goto WAIT
		}
	}
WAIT:
	// Wait for a while
	db.bwg.Wait()
	close(db.batchC)
	goto WRITEQUEUE
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
