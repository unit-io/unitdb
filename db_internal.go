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
	"errors"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/crypto"
	"github.com/unit-io/unitdb/memdb"
	"github.com/unit-io/unitdb/message"
)

const (
	entriesPerIndexBlock  = 255 // (4096 i.e blocksize - 14 fixed/16 i.e entry size)
	entriesPerWindowBlock = 335 // ((4096 i.e. blocksize - 26 fixed)/12 i.e. window entry size)
	nBlocks               = 100000
	nShards               = 27
	nPoolSize             = 27
	lockPostfix           = ".lock"
	idSize                = 9 // message ID prefix with additional encryption bit.
	version               = 1 // file format version.

	// maxExpDur expired keys are deleted from DB after durType*maxExpDur.
	// For example if durType is Minute and maxExpDur then
	// all expired keys are deleted from db in 1 minutes
	maxExpDur = 1

	// maxWindowDur duration in hours to save summary of records to timewindow files
	maxWindowDur = 24 * 7

	// maxRetention in hours
	maxRetention = 28 * 24

	// maxTopicLength is the maximum size of a topic in bytes.
	maxTopicLength = 1 << 16

	// maxValueLength is the maximum size of a value in bytes.
	maxValueLength = 1 << 30

	// maxKeys is the maximum numbers of keys in the DB.
	maxKeys = math.MaxInt64

	// maxSeq is the maximum number of seq supported.
	maxSeq = math.MaxUint64
)

type (
	_DB struct {
		mutex _Mutex

		// The db start time.
		start time.Time
		// The metrics to measure timeseries on message events.
		meter *Meter

		dbInfo _DBInfo
		mac    *crypto.MAC

		mem      *memdb.DB
		bufPool  *bpool.BufferPool
		info     _FileSet
		filter   Filter
		freeList *_Lease

		timeWindow *_TimeWindowBucket

		// Trie
		trie *_Trie

		// Block reader
		reader *_BlockReader

		// sync handler
		syncLockC  chan struct{}
		syncWrites bool
		syncHandle _SyncHandle

		// Close.
		closeW sync.WaitGroup
		closeC chan struct{}
		closed uint32
		closer io.Closer
	}
)

func (db *DB) writeInfo() error {
	inf := _DBInfo{
		header: _Header{
			signature: signature,
			version:   version,
		},
		encryption: db.internal.dbInfo.encryption,
		sequence:   atomic.LoadUint64(&db.internal.dbInfo.sequence),
		count:      atomic.LoadUint64(&db.internal.dbInfo.count),
	}

	return db.internal.info.writeMarshalableAt(inf, 0)
}

// Close closes the DB.
func (db *DB) close() error {
	if !db.setClosed() {
		return errClosed
	}

	// Signal all goroutines.
	time.Sleep(db.opts.tinyBatchWriteInterval)
	close(db.internal.closeC)

	// Acquire lock.
	db.internal.syncLockC <- struct{}{}

	// Wait for all goroutines to exit.
	db.internal.closeW.Wait()

	// fmt.Println("db.close: pending timeIDs ", db.internal.timeWindow.timeIDs)
	// close memdb.
	db.internal.mem.Close()

	if err := db.writeInfo(); err != nil {
		return err
	}
	db.internal.freeList.defrag()
	if err := db.internal.freeList.write(); err != nil {
		return err
	}
	if err := db.fs.close(); err != nil {
		return err
	}
	if err := db.lock.unlock(); err != nil {
		return err
	}

	var err error
	if db.internal.closer != nil {
		if err1 := db.internal.closer.Close(); err1 != nil {
			err = err1
		}
		db.internal.closer = nil
	}

	db.internal.meter.UnregisterAll()

	return err
}

// loadTopicHash loads topic and offset from window file.
func (db *DB) loadTrie() error {
	r := newWindowReader(db.fs)
	err := r.foreachWindowBlock(func(startSeq, topicHash uint64, off int64) (bool, error) {
		// fmt.Println("db.loadTrie: topicHash, seq ", topicHash, startSeq)
		e, err := db.internal.reader.readEntry(startSeq)
		if err != nil {
			return true, err
		}
		if e.topicSize == 0 {
			// fmt.Println("db.loadTrie: topic not found topicHash, seq ", topicHash, startSeq)
			return false, nil
		}
		rawtopic, err := db.internal.reader.readTopic(e)
		if err != nil {
			return true, err
		}
		t := new(message.Topic)
		err = t.Unmarshal(rawtopic)
		if err != nil {
			return true, err
		}
		if ok := db.internal.trie.add(newTopic(topicHash, off), t.Parts, t.Depth); !ok {
			logger.Info().Str("context", "db.loadTrie: topic exist in the trie")
			return false, nil
		}
		return false, nil
	})
	return err
}

func (db *DB) readEntry(topicHash uint64, seq uint64) (_IndexEntry, error) {
	data, err := db.internal.mem.Get(seq)
	if err != nil {
		return _IndexEntry{}, errMsgIDDeleted
	}
	if data != nil {
		var m _Entry
		m.UnmarshalBinary(data[:entrySize])
		e := _IndexEntry{
			seq:       m.seq,
			topicSize: m.topicSize,
			valueSize: m.valueSize,

			cache: data[entrySize:],
		}
		return e, nil
	}

	return db.internal.reader.readEntry(seq)
}

// lookups are performed in following order
// ilookup lookups in memory entries from timeWindow
// lookup lookups persisted entries from timeWindow file.
func (db *DB) lookup(q *Query) error {
	topics := db.internal.trie.lookup(q.internal.parts, q.internal.depth, q.internal.topicType)
	sort.Slice(topics[:], func(i, j int) bool {
		return topics[i].offset > topics[j].offset
	})
	for _, topic := range topics {
		if len(q.internal.winEntries) > q.Limit {
			break
		}
		limit := q.Limit - len(q.internal.winEntries)
		wEntries := db.internal.timeWindow.lookup(db.fs, topic.hash, topic.offset, q.internal.cutoff, limit)
		for _, we := range wEntries {
			q.internal.winEntries = append(q.internal.winEntries, _Query{topicHash: topic.hash, seq: we.seq()})
		}
		// fmt.Println("db.lookup: topicHash, count ", topic.hash, len(wEntries))
	}

	return nil
}

func (db *DB) parseTopic(contract uint32, topic []byte) (*message.Topic, uint32, error) {
	t := new(message.Topic)

	//Parse the Key.
	t.ParseKey(topic)
	// Parse the topic.
	t.Parse(contract, true)
	if t.TopicType == message.TopicInvalid {
		return nil, 0, errBadRequest
	}
	// In case of ttl, add ttl to the msg and store to the db.
	if ttl, ok := t.TTL(); ok {
		return t, ttl, nil
	}
	return t, 0, nil
}

func (db *DB) setEntry(e *Entry) error {
	var id message.ID
	var eBit uint8
	var seq uint64
	var rawTopic []byte
	if !e.entry.parsed {
		if e.Contract == 0 {
			e.Contract = message.MasterContract
		}
		t, ttl, err := db.parseTopic(e.Contract, e.Topic)
		if err != nil {
			return err
		}
		if e.ExpiresAt == 0 && ttl > 0 {
			e.ExpiresAt = ttl
		}
		t.AddContract(e.Contract)
		e.entry.topicHash = t.GetHash(e.Contract)
		// topic is packed if it is new topic entry
		if _, ok := db.internal.trie.getOffset(e.entry.topicHash); !ok {
			rawTopic = t.Marshal()
			e.entry.topicSize = uint16(len(rawTopic))
		}
		e.entry.parsed = true
	}
	if e.ID != nil {
		id = message.ID(e.ID)
		seq = id.Sequence()
	} else {
		seq = db.nextSeq()
		id = message.NewID(seq)
	}
	if seq == 0 {
		panic("db.setEntry: seq is zero")
	}

	id.SetContract(e.Contract)
	e.entry.seq = seq
	e.entry.expiresAt = e.ExpiresAt
	val := snappy.Encode(nil, e.Payload)
	if db.internal.dbInfo.encryption == 1 || e.Encryption {
		eBit = 1
		val = db.internal.mac.Encrypt(nil, val)
	}
	e.entry.valueSize = uint32(len(val))
	mLen := entrySize + idSize + uint32(e.entry.topicSize) + uint32(e.entry.valueSize)
	e.entry.cache = make([]byte, mLen)
	entryData, err := e.entry.MarshalBinary()
	if err != nil {
		return err
	}
	copy(e.entry.cache, entryData)
	copy(e.entry.cache[entrySize:], id.Prefix())
	e.entry.cache[entrySize+idSize-1] = byte(eBit)
	// topic data is added on first entry for the topic.
	if e.entry.topicSize != 0 {
		copy(e.entry.cache[entrySize+idSize:], rawTopic)
		// fmt.Println("db.setEntry: topicHash, seq ", e.entry.topicHash, e.entry.seq)
	}
	copy(e.entry.cache[entrySize+idSize+uint32(e.entry.topicSize):], val)
	return nil
}

// delete deletes the given key from the DB.
func (db *DB) delete(topicHash, seq uint64) error {
	if db.opts.flags.immutable {
		return nil
	}

	db.internal.meter.Dels.Inc(1)
	if err := db.internal.mem.Delete(seq); err != nil {
		return err
	}

	// Test filter block for the message id presence.
	if !db.internal.filter.Test(seq) {
		return nil
	}

	w, err := newBlockWriter(db.fs, db.internal.freeList, nil)
	if err != nil {
		return err
	}
	e, err := w.del(seq)
	if err != nil {
		return err
	}
	db.internal.freeList.freeBlock(e.msgOffset, e.mSize())
	db.decount(1)
	if db.internal.syncWrites {
		return db.sync()
	}
	return nil
}

// batch starts a new batch.
func (db *DB) batch() *Batch {
	opts := &_Options{}
	WithDefaultBatchOptions().set(opts)
	opts.batchOptions.encryption = db.internal.dbInfo.encryption == 1
	b := &Batch{opts: opts, db: db, buffer: db.internal.bufPool.Get()}
	b.mem = db.internal.mem.NewBatch()
	b.commitComplete = make(chan struct{})

	return b
}

// seq current seq of the DB.
func (db *DB) seq() uint64 {
	return atomic.LoadUint64(&db.internal.dbInfo.sequence)
}

func (db *DB) nextSeq() uint64 {
	return atomic.AddUint64(&db.internal.dbInfo.sequence, 1)
}

func (db *DB) incount(count uint64) uint64 {
	return atomic.AddUint64(&db.internal.dbInfo.count, count)
}

func (db *DB) decount(count uint64) uint64 {
	return atomic.AddUint64(&db.internal.dbInfo.count, -count)
}

// setClosed flag; return true if not already closed.
func (db *DB) setClosed() bool {
	return atomic.CompareAndSwapUint32(&db.internal.closed, 0, 1)
}

// isClosed checks whether DB was closed.
func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.internal.closed) != 0
}

// ok checks read ok status.
func (db *DB) ok() error {
	if db.isClosed() {
		return errors.New("db is closed")
	}
	return nil
}
