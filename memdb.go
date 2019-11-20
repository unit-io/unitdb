package tracedb

import (
	"sync/atomic"
	"time"

	"github.com/saffat-in/tracedb/memdb"
)

type mem struct {
	db *DB
	*memdb.DB
	ref int32
}

func (m *mem) getref() int32 {
	return atomic.LoadInt32(&m.ref)
}

func (m *mem) incref() {
	atomic.AddInt32(&m.ref, 1)
}

func (m *mem) decref() {
	if ref := atomic.AddInt32(&m.ref, -1); ref == 0 {
		m.db.mpoolPut(m.DB)
		m.DB = nil
	} else if ref < 0 {
		panic("negative mem ref")
	}
}

// Create new mem and froze the old one; need external synchronization.
// newMem only called synchronously by the writer.
func (db *DB) newMem(n int) (mem *mem, err error) {
	db.memMu.Lock()
	defer db.memMu.Unlock()
	mem = db.mpoolGet(n)
	mem.incref() // for self
	mem.incref() // for caller
	db.mem = mem
	return
}

func (db *DB) mpoolPut(mdb *memdb.DB) {
	if !db.isClosed() {
		select {
		case db.memPool <- mdb:
		default:
		}
	}
}

func (db *DB) mpoolGet(n int) *mem {
	var mdb *memdb.DB
	select {
	case mdb = <-db.memPool:
	default:
	}
	if mdb == nil {
		var err error
		var opts memdb.Options
		mdb, err = memdb.Open("memdb", opts.CopyWithDefaults())
		if err != nil {
			logger.Error().Err(err).Str("context", "mem.mpoolGet").Msg("Unable to open database")
		}
	}
	return &mem{
		db: db,
		DB: mdb,
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

// Get all mems.
func (db *DB) getMems() (e *mem) {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mem != nil {
		db.mem.incref()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	return db.mem
}

// Get effective mem.
func (db *DB) getEffectiveMem() *mem {
	db.memMu.RLock()
	defer db.memMu.RUnlock()
	if db.mem != nil {
		db.mem.incref()
	} else if !db.isClosed() {
		panic("nil effective mem")
	}
	return db.mem
}

// Clear mems ptr; used by DB.Close().
func (db *DB) clearMems() {
	db.memMu.Lock()
	db.mem = nil
	db.memMu.Unlock()
}

// Check whether DB was closed.
func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) != 0
}

// Get latest sequence number.
func (m *mem) getSeq() uint64 {
	return atomic.LoadUint64(&m.Seq)
}

// Atomically adds delta to seq.
func (m *mem) addSeq(delta uint64) {
	atomic.AddUint64(&m.Seq, delta)
}

func (m *mem) setSeq(seq uint64) {
	atomic.StoreUint64(&m.Seq, seq)
}

// func ensureBuffer(b []byte, n int) []byte {
// 	if cap(b) < n {
// 		return make([]byte, n)
// 	}
// 	return b[:n]
// }
