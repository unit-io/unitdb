package tracedb

import (
	"sync/atomic"
	"time"
)

type memdb struct {
	*DB
	ref int32
}

func (m *memdb) getref() int32 {
	return atomic.LoadInt32(&m.ref)
}

func (m *memdb) incref() {
	atomic.AddInt32(&m.ref, 1)
}

func (m *memdb) decref() {
	if ref := atomic.AddInt32(&m.ref, -1); ref == 0 {
		m.DB = nil
	} else if ref < 0 {
		panic("negative memdb ref")
	}
}

// Create new memdb and froze the old one; need external synchronization.
// newMem only called synchronously by the writer.
func (db *DB) newmemdb(n int) (mem *memdb, err error) {
	db.memMu.Lock()
	defer db.memMu.Unlock()
	mem = db.mpoolGet(n)
	mem.incref() // for self
	mem.incref() // for caller
	return mem, nil
}

func (db *DB) mpoolPut(mdb *memdb) {
	if !db.isClosed() {
		select {
		case db.memPool <- mdb:
		default:
		}
	}
}

func (db *DB) mpoolGet(n int) *memdb {
	var mdb *memdb
	select {
	case mdb = <-db.memPool:
	default:
	}
	if mdb == nil {
		var opts Options
		db, err := Open("example", opts.memWithDefaults())
		if err != nil {
			logger.Printf("Unable to open database: %v", err)
		}
		return &memdb{
			DB: db,
		}
	}
	return &memdb{
		DB: mdb.DB,
	}
}

func (db *DB) mpoolDrain() {
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			select {
			case <-db.memPool:
			default:
			}
		case <-db.closeC:
			ticker.Stop()
			// Make sure the pool is drained.
			select {
			case <-db.memPool:
			case <-time.After(time.Second):
			}
			close(db.memPool)
			return
		}
	}
}

// // Get all memdbs.
// func (db *DB) getMems() (e *memdb) {
// 	db.memMu.RLock()
// 	defer db.memMu.RUnlock()
// 	if db.mem != nil {
// 		db.mem.incref()
// 	} else if !db.isClosed() {
// 		panic("nil effective mem")
// 	}
// 	return db.mem
// }

// // Get effective memdb.
// func (db *DB) getEffectiveMem() *memdb {
// 	db.memMu.RLock()
// 	defer db.memMu.RUnlock()
// 	if db.mem != nil {
// 		db.mem.incref()
// 	} else if !db.isClosed() {
// 		panic("nil effective mem")
// 	}
// 	return db.mem
// }

// // Clear mems ptr; used by DB.Close().
// func (db *DB) clearMems() {
// 	db.memMu.Lock()
// 	db.mem = nil
// 	db.memMu.Unlock()
// }

// Check whether DB was closed.
func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) != 0
}

// Get latest sequence number.
func (db *DB) getSeq() uint64 {
	return atomic.LoadUint64(&db.seq)
}

// Atomically adds delta to seq.
func (db *DB) addSeq(delta uint64) {
	atomic.AddUint64(&db.seq, delta)
}

func (db *DB) setSeq(seq uint64) {
	atomic.StoreUint64(&db.seq, seq)
}

func ensureBuffer(b []byte, n int) []byte {
	if cap(b) < n {
		return make([]byte, n)
	}
	return b[:n]
}
