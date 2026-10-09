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
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/crypto"
	fltr "github.com/unit-io/unitdb/filter"
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
	version               = 4 // file format version; 2 adds checksums, 3 the block being synced, 4 the topics file.

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

		// recounted is set when Open counted the entries on disk
		// (deriveFromIndex).
		recounted bool

		// hooks are called after each put and delete (OnWrite).
		hooks _Hooks

		// Trie
		trie *_Trie
		// topics names the topics: see _TopicNames.
		topics *_TopicNames
		// unnamed holds the newest window block of each topic stored with
		// no entry on disk holding its name: a topic's name is in its first
		// entry, which can reach disk after others of the topic. The topic
		// joins the trie, there, once an entry put or recovered names it.
		// Written by Open only.
		unnamed map[uint64]int64

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
		syncing:    db.internal.dbInfo.syncing,
	}

	return db.internal.info.writeMarshalableAt(inf, 0)
}

// Close closes the DB.
func (db *DB) close() error {
	if !db.setClosed() {
		return errClosed
	}
	// Stop the pool's drain goroutine, or every DB opened leaks one. The pool
	// still serves Get and Put until close returns.
	defer db.internal.bufPool.Done()

	// Signal all goroutines.
	close(db.internal.closeC)

	// Acquire lock.
	db.internal.syncLockC <- struct{}{}

	// Wait for all goroutines to exit.
	db.internal.closeW.Wait()

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

// deriveFromIndex sets, from the entries in the index, what is kept
// besides them and was kept apart from them, and so could disagree after a
// crash:
//
//   - Count: a crash between a delete's tombstone and its count left it one
//     high. The recovery then counts only the entries it writes. With
//     background expiry, which uncounts the entries it frees and leaves them
//     in the index, the count kept is kept.
//   - the filter of the sequences in the index, which reads test first: one
//     missing a sequence hides its entry. It is saved, as a sync saves it,
//     for older versions to read.
//   - the sequence, at least the last in the index, or a new entry would
//     take the sequence of one stored.
func (db *DB) deriveFromIndex() error {
	_, indexFile, _, _, err := db.files()
	if err != nil {
		return err
	}
	f := fltr.NewFilterGenerator()
	var live, last uint64
	if err := forEachBlock(indexFile, func(off int64, buf []byte) error {
		var b _IndexBlock
		if err := b.unmarshalBinary(buf); err != nil {
			return err
		}
		for _, e := range b.entries {
			if e.seq == 0 {
				continue
			}
			f.Append(e.seq)
			if e.seq > last {
				last = e.seq
			}
			if !e.deleted() {
				live++
			}
		}
		return nil
	}); err != nil {
		return err
	}
	db.internal.filter.filterBlock = f
	// Saved for an older version to read.
	if err := db.internal.filter.write(); err != nil {
		return err
	}
	if stored := db.seq(); stored < last {
		logger.Info().Uint64("stored", stored).Uint64("last", last).Str("context", "db.deriveFromIndex").Msg("sequence raised to the last in the index")
	}
	db.advanceSeq(last)
	if db.opts.flags.backgroundKeyExpiry {
		return nil
	}
	if stored := atomic.LoadUint64(&db.internal.dbInfo.count); stored != live {
		logger.Info().Uint64("stored", stored).Uint64("entries", live).Str("context", "db.deriveFromIndex").Msg("count set to the entries on disk")
	}
	atomic.StoreUint64(&db.internal.dbInfo.count, live)
	db.internal.recounted = true
	return nil
}

// loadTrie loads the topics of the window blocks on disk, with the offset of
// each topic's newest block. A topic's name is in the entry put first for it,
// which a sync can write after other entries of the topic: the topic's
// blocks are searched, oldest first, for the entry holding it.
func (db *DB) loadTrie() error {
	r := newWindowReader(db.fs)
	err := r.blockIterator(func(startSeq, topicHash uint64, off int64) (bool, error) {
		// The topics file names it, or else, written before the file, an
		// entry of it does; the file then records it.
		rawtopic, named := db.internal.topics.get(topicHash)
		if !named {
			var err error
			if rawtopic, err = db.storedTopic(r.winFile, startSeq, off); err != nil {
				return true, err
			}
			if rawtopic != nil {
				if err := db.internal.topics.name(topicHash, rawtopic, false); err != nil {
					return true, err
				}
			}
		}
		if rawtopic == nil {
			if db.internal.unnamed == nil {
				db.internal.unnamed = make(map[uint64]int64)
			}
			db.internal.unnamed[topicHash] = off
			return false, nil
		}
		t := new(message.Topic)
		if err := t.Unmarshal(rawtopic); err != nil {
			return true, err
		}
		if ok := db.internal.trie.add(newTopic(topicHash, off), t.Parts, t.Depth); !ok {
			logger.Info().Str("context", "db.loadTrie: topic exist in the trie")
		}
		return false, nil
	})
	if err != nil {
		return err
	}
	// Topics named with no window block yet: their entries are in the WAL,
	// or were deleted.
	for hash, raw := range db.internal.topics.all() {
		if _, ok := db.internal.trie.getOffset(hash); ok {
			continue
		}
		t := new(message.Topic)
		if err := t.Unmarshal(raw); err != nil {
			return fmt.Errorf("%w: topic %d: %v", errCorrupted, hash, err)
		}
		db.internal.trie.add(newTopic(hash, db.internal.unnamed[hash]), t.Parts, t.Depth)
	}
	return nil
}

// storedTopic returns the name of the topic whose newest window block is at
// head, from the first entry on disk holding it; nil if none does yet.
// Deleted entries that hold their topic still carry it.
func (db *DB) storedTopic(winFile *_File, startSeq uint64, head int64) ([]byte, error) {
	topicOf := func(seq uint64) ([]byte, error) {
		e, err := db.internal.reader.readIndexEntry(seq)
		if err == errEntryInvalid || (err == nil && e.topicSize == 0) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return db.internal.reader.readTopic(e)
	}
	if raw, err := topicOf(startSeq); raw != nil || err != nil {
		return raw, err
	}
	var chain []_WinBlock
	for off := head; ; {
		b, err := db.readWinBlock(winFile, off)
		if err != nil {
			return nil, err
		}
		chain = append(chain, b)
		if b.next == 0 || len(chain) > int(winFile.currSize()/int64(blockSize)) {
			break
		}
		off = b.next
	}
	for i := len(chain) - 1; i >= 0; i-- {
		for _, we := range chain[i].entries[:chain[i].entryIdx] {
			if raw, err := topicOf(we.seq()); raw != nil || err != nil {
				return raw, err
			}
		}
	}
	return nil, nil
}

// addTopic adds a topic named by an entry, written before the topics file,
// to the trie, at its newest window block if it has blocks on disk already,
// and records its name, for the topics file to name every topic; Open
// syncs the records.
func (db *DB) addTopic(topicHash uint64, parts []message.Part, depth uint8) {
	db.internal.trie.add(newTopic(topicHash, db.internal.unnamed[topicHash]), parts, depth)
	t := message.Topic{Parts: parts, Depth: depth}
	if err := db.internal.topics.name(topicHash, t.Marshal(), false); err != nil {
		logger.Error().Err(err).Uint64("topic", topicHash).Str("context", "db.addTopic").Msg("unable to record a topic")
	}
}

func (db *DB) readEntry(q _Query) (_IndexEntry, error) {
	if m, data, ok := db.memEntry(q.seq); ok {
		if m.valueSize == 0 {
			return _IndexEntry{}, errMsgIDDeleted // a tombstone
		}
		e := _IndexEntry{
			seq:       m.seq,
			topicSize: m.topicSize,
			valueSize: m.valueSize,

			cache: data[entrySize:],
		}
		return e, nil
	}

	e, err := db.internal.reader.readEntry(q.seq)
	if err == errEntryInvalid {
		// The entry is in neither memdb nor the index file, so it was deleted before it was synced.
		return e, errMsgIDDeleted
	}
	return e, err
}

// lookup sets the query's window entries to up to limit candidates across the
// matching topics and returns how many were found; fewer than limit means there
// are no more. Each topic is looked up in memory (ilookup) and then on disk.
func (db *DB) lookup(q *Query, limit int) int {
	q.internal.winEntries = q.internal.winEntries[:0]
	topics := db.internal.trie.lookup(q.internal.parts, q.internal.depth, q.internal.topicType)
	sort.Slice(topics[:], func(i, j int) bool {
		return topics[i].offset > topics[j].offset
	})
	for _, topic := range topics {
		if len(q.internal.winEntries) >= limit {
			break
		}
		wEntries := db.internal.timeWindow.lookup(db.fs, topic.hash, topic.offset, q.internal.cutoff, limit-len(q.internal.winEntries))
		for _, we := range wEntries {
			q.internal.winEntries = append(q.internal.winEntries, _Query{topicHash: topic.hash, seq: we.seq()})
		}
	}

	return len(q.internal.winEntries)
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

// setEntry packs the entry, naming its topic first if it is new.
func (db *DB) setEntry(e *Entry) error {
	if (db.internal.dbInfo.encryption == 1 || e.Encryption) && db.internal.mac == nil {
		return ErrNoEncryptionKey
	}
	var id message.ID
	var eBit uint8
	var seq uint64
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
		// The topic is named in the topics file before an entry of it is
		// put; entries no longer hold it.
		if err := db.nameTopic(e.entry.topicHash, t); err != nil {
			return err
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
	e.entry.id = id
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
	// An entry holds no topic name: the topics file has it.
	copy(e.entry.cache[entrySize+idSize:], val)
	return nil
}

// delete deletes the given key from the DB.
func (db *DB) delete(topicHash, seq uint64) error {
	if db.opts.flags.immutable {
		return nil
	}

	db.internal.meter.Dels.Inc(1)

	// A topic's name is packed into its first entry only. An entry that
	// carries it and isn't on disk yet is replaced in memory by its
	// tombstone: the entry with its topic and no value, as a delete leaves
	// it on disk (_IndexEntry.deleted). The tombstone goes to the WAL as a
	// put does, and a sync writes it, name and all. Dropping the entry
	// would leave the topic's other entries to reach disk without the name.
	if m, data, ok := db.memEntry(seq); ok && m.topicSize != 0 && !db.onDisk(seq) {
		if m.valueSize == 0 {
			return nil // a tombstone already
		}
		_, err := db.internal.mem.Replace(seq, tombstone(m, data))
		return err
	}
	db.internal.mem.Delete(seq)

	// Test filter block for the message id presence.
	if !db.internal.filter.Test(seq) {
		return nil
	}

	// Serialize with Sync, which writes index blocks through its own block writer.
	select {
	case db.internal.syncLockC <- struct{}{}:
	case <-db.internal.closeC:
		return errClosed
	}
	defer func() {
		<-db.internal.syncLockC
	}()

	buf := db.internal.bufPool.Get()
	defer db.internal.bufPool.Put(buf)
	w, err := newBlockWriter(db.fs, db.internal.freeList, buf)
	if err != nil {
		return err
	}
	e, err := w.del(seq, true)
	if err != nil {
		return err
	}
	if e.seq == 0 {
		// entry is not on disk.
		return nil
	}
	// Persist the tombstone before releasing the entry's data block.
	if err := w.write(); err != nil {
		return err
	}
	if testHookBeforeDecount != nil {
		testHookBeforeDecount()
	}
	// The data block of an entry holding its topic is kept so the trie can be loaded on open.
	if e.topicSize == 0 {
		db.internal.freeList.freeBlock(e.msgOffset, e.mSize())
	}
	db.decount(1)
	if db.internal.syncWrites {
		return db.sync()
	}
	// Persist the count so it is right after a crash.
	return db.writeInfo()
}

// memEntry returns the entry seq in memory, and its header.
func (db *DB) memEntry(seq uint64) (_Entry, []byte, bool) {
	var m _Entry
	data, _ := db.internal.mem.Get(seq)
	if len(data) < entrySize || m.UnmarshalBinary(data[:entrySize]) != nil {
		return m, nil, false
	}
	return m, data, true
}

// tombstone returns the entry m, stored as data, with no value: deleted, and
// holding its id and topic.
func tombstone(m _Entry, data []byte) []byte {
	m.valueSize = 0
	hdr, _ := m.MarshalBinary()
	out := make([]byte, entrySize+idSize+int(m.topicSize))
	copy(out, hdr)
	copy(out[entrySize:], data[entrySize:entrySize+idSize+int(m.topicSize)])
	return out
}

// onDisk reports whether the entry seq is in the index file.
func (db *DB) onDisk(seq uint64) bool {
	if !db.internal.filter.Test(seq) {
		return false
	}
	_, err := db.internal.reader.readEntry(seq)
	return err == nil
}

// batch starts a new batch.
func (db *DB) batch() *Batch {
	opts := &_Options{}
	WithDefaultBatchOptions().set(opts)
	opts.batchOptions.encryption = db.internal.dbInfo.encryption == 1
	b := &Batch{db: db, opts: opts, writeLockC: make(chan struct{}, 1), buffer: db.internal.bufPool.Get()}
	b.mem = db.internal.mem.NewBatch()
	b.commitComplete = make(chan struct{})

	return b
}

// seq current seq of the DB.
func (db *DB) seq() uint64 {
	return atomic.LoadUint64(&db.internal.dbInfo.sequence)
}

// advanceSeq raises the DB sequence to at least seq.
func (db *DB) advanceSeq(seq uint64) {
	for {
		cur := atomic.LoadUint64(&db.internal.dbInfo.sequence)
		if cur >= seq || atomic.CompareAndSwapUint64(&db.internal.dbInfo.sequence, cur, seq) {
			return
		}
	}
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
