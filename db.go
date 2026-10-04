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
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
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
	"github.com/unit-io/unitdb/wal"
)

// Lock order. A goroutine holding one takes only those below it:
//
//  1. syncLockC, a semaphore: held by a sync, a delete of an entry on disk,
//     expiry, Verify, recovery, and Close for good.
//  2. the mutex of a query's prefix (_Mutex), which Get holds for reading;
//     nothing holds it for writing.
//  3. memdb's locks, in its order (memdb/locks.go, which the tests check).
//  4. the trie's; a time window bucket's, then one of its blocks'; a lease
//     shard's; the file set's; the expiry window's: none held taking
//     another but the bucket's.
//
// A sync never takes the prefix mutex, and Get never takes syncLockC.

// DB represents the message storage for topic->keys-values.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	opts *_Options

	lock _LockFile
	fs   *_FileSet

	internal *_DB
}

// Open opens or creates a new DB.
func Open(path string, opts ...Options) (*DB, error) {
	options := &_Options{}
	WithDefaultOptions().set(options)
	WithDefaultFlags().set(options)
	WithDefaultQueryOptions().set(options)
	for _, opt := range opts {
		if opt != nil {
			opt.set(options)
		}
	}

	lock, err := createLockFile(path)
	if err != nil {
		if err == os.ErrExist {
			err = errLocked
		}
		return nil, err
	}

	infoFile, err := newFile(path, 1, _FileDesc{fileType: typeInfo})
	if err != nil {
		return nil, err
	}

	timeOptions := &_TimeOptions{
		maxDuration:         options.syncDurationType * time.Duration(options.maxSyncDurations),
		expDurationType:     time.Minute,
		maxExpDurations:     maxExpDur,
		backgroundKeyExpiry: options.flags.backgroundKeyExpiry,
	}
	winFile, err := newFile(path, 1, _FileDesc{fileType: typeTimeWindow})
	if err != nil {
		return nil, err
	}

	indexFile, err := newFile(path, 1, _FileDesc{fileType: typeIndex})
	if err != nil {
		return nil, err
	}

	dataFile, err := newFile(path, 1, _FileDesc{fileType: typeData})
	if err != nil {
		return nil, err
	}

	dbInfo := _DBInfo{}
	if infoFile.currSize() == 0 {
		dbInfo = _DBInfo{
			header: _Header{
				signature: signature,
				version:   version,
			},
		}
		if _, err = infoFile.extend(fixed); err != nil {
			return nil, err
		}
		if err := infoFile.writeMarshalableAt(dbInfo, 0); err != nil {
			return nil, err
		}
	}

	// A format 2 header is shorter; it is written as format 3 on its next
	// write.
	infoSize := fixed
	if infoFile.currSize() < int64(fixed) {
		infoSize = fixedV2
	}
	if err := infoFile.readUnmarshalableAt(&dbInfo, infoSize, 0); err != nil {
		logger.Error().Err(err).Str("context", "db.readHeader")
		return nil, err
	}
	if !bytes.Equal(dbInfo.header.signature[:], signature[:]) {
		return nil, errCorrupted
	}

	leaseFile, err := newFile(path, 1, _FileDesc{fileType: typeLease})
	if err != nil {
		return nil, err
	}
	lease := newLease(leaseFile, options.freeBlockSize)

	filterFile, err := newFile(path, 1, _FileDesc{fileType: typeFilter})
	if err != nil {
		return nil, err
	}

	sumFile, err := newFile(path, 1, _FileDesc{fileType: typeChecksum})
	if err != nil {
		return nil, err
	}

	topicsFile, err := newFile(path, 1, _FileDesc{fileType: typeTopics})
	if err != nil {
		return nil, err
	}

	fileset := &_FileSet{mu: new(sync.RWMutex), list: []_FileSet{infoFile, winFile, indexFile, dataFile, leaseFile, filterFile, sumFile, topicsFile}}
	internal := &_DB{
		mutex: newMutex(),
		start: time.Now(),
		meter: NewMeter(),

		dbInfo: dbInfo,

		bufPool: bpool.NewBufferPool(options.bufferSize, &bpool.Options{MaxElapsedTime: 10 * time.Second}),

		info:     infoFile,
		filter:   Filter{file: filterFile, filterBlock: fltr.NewFilterGenerator()},
		freeList: lease,

		timeWindow: newTimeWindowBucket(timeOptions),

		// Trie
		trie:   newTrie(),
		topics: newTopicNames(topicsFile),

		// Block reader
		reader: newBlockReader(fileset),

		// Sync Handler
		syncLockC: make(chan struct{}, 1),

		// Close
		closeC: make(chan struct{}),
	}

	db := &DB{
		opts: options,

		lock: lock,
		fs:   fileset,

		internal: internal,
	}
	// abort releases the files and lock when Open fails.
	abort := func(err error) (*DB, error) {
		if db.internal.mem != nil {
			db.internal.mem.Close()
		}
		db.fs.close()
		db.lock.unlock()
		return nil, err
	}

	// Verify checksums before anything reads or writes the files.
	if err := db.checkFiles(); err != nil {
		logger.Error().Err(err).Str("context", "db.checkFiles")
		return abort(err)
	}
	if err := db.deriveFromIndex(); err != nil {
		logger.Error().Err(err).Str("context", "db.deriveFromIndex")
		return abort(err)
	}

	// Create a new MAC from the key. Without one, the database neither
	// encrypts nor decrypts.
	if options.encryptionKey != nil {
		if internal.mac, err = crypto.New(options.encryptionKey); err != nil {
			return abort(err)
		}
	}

	// set encryption flag to encrypt messages.
	if options.flags.encryption {
		internal.dbInfo.encryption = 1
	}
	if internal.dbInfo.encryption == 1 && internal.mac == nil {
		return abort(ErrNoEncryptionKey)
	}

	// Create a blockcache.
	memdb, err := memdb.Open(memdb.WithLogFilePath(path), memdb.WithMemdbSize(options.memdbSize), memdb.WithBufferSize(options.bufferSize))
	if errors.Is(err, wal.ErrCorrupted) {
		return abort(fmt.Errorf("%w: %v", errCorrupted, err))
	}
	if err != nil {
		return abort(err)
	}
	internal.mem = memdb

	if err := db.internal.topics.load(); err != nil {
		logger.Error().Err(err).Str("context", "topics.load")
		return abort(err)
	}
	if err := db.loadTrie(); err != nil {
		logger.Error().Err(err).Str("context", "db.loadTrie")
		return abort(err)
	}


	// Read freeList.
	if err := db.internal.freeList.read(); err != nil {
		logger.Error().Err(err).Str("context", "db.readHeader")
		return abort(err)
	}
	if err := db.checkFreeList(); err != nil {
		logger.Error().Err(err).Str("context", "db.checkFreeList")
		return abort(err)
	}

	if err := db.recoverLog(); err != nil {
		logger.Error().Err(err).Str("context", "db.recoverLog")
		return abort(err)
	}
	// The topics named by their entries, in a DB from before the topics
	// file, are recorded now (addTopic).
	if err := db.internal.topics.sync(); err != nil {
		return abort(err)
	}

	db.internal.syncHandle = _SyncHandle{DB: db}
	db.startSyncer(options.syncDurationType * time.Duration(options.maxSyncDurations))

	if db.opts.flags.backgroundKeyExpiry {
		db.startExpirer(time.Minute, maxExpDur)
	}

	return db, nil
}

// Close closes the DB.
func (db *DB) Close() error {
	if err := db.close(); err != nil {
		return err
	}

	return nil
}

// Get return items matching the query paramater.
func (db *DB) Get(q *Query) (items [][]byte, err error) {
	items, _, err = db.get(q, false)
	return items, err
}

// GetWithIDs returns the items matching the query, as Get does, and the id of
// each, which DeleteEntry takes with the item's topic: a caller can move or
// delete the entries it reads. A wildcard query returns entries of several
// topics, and does not say which topic each is of.
func (db *DB) GetWithIDs(q *Query) (ids, items [][]byte, err error) {
	items, ids, err = db.get(q, true)
	return ids, items, err
}

func (db *DB) get(q *Query, withIDs bool) (items, ids [][]byte, err error) {
	if err := db.ok(); err != nil {
		return nil, nil, err
	}
	switch {
	case len(q.Topic) == 0:
		return nil, nil, errTopicEmpty
	case len(q.Topic) > maxTopicLength:
		return nil, nil, errTopicTooLarge
	}
	// // CPU profiling by default
	// defer profile.Start().Stop()
	q.internal.opts = &_QueryOptions{defaultQueryLimit: db.opts.queryOptions.defaultQueryLimit, maxQueryLimit: db.opts.queryOptions.maxQueryLimit}
	if err := q.parse(); err != nil {
		return nil, nil, err
	}
	mu := db.internal.mutex.getMutex(q.internal.prefix)
	mu.RLock()
	defer mu.RUnlock()

	// Deleted entries and entries outside the contract or cutoff don't count
	// towards the limit, so fetch more candidates until the limit is met or
	// there are no more.
	var outBytes int64
	for fetch := q.Limit; ; fetch *= 2 {
		found := db.lookup(q, fetch)
		sort.Slice(q.internal.winEntries[:], func(i, j int) bool {
			return q.internal.winEntries[i].seq > q.internal.winEntries[j].seq
		})
		// A sync writes entries to disk before releasing them from memory, so an
		// entry can be found in both; drop the duplicates.
		uniq := q.internal.winEntries[:0]
		for i, we := range q.internal.winEntries {
			if i > 0 && we.seq == q.internal.winEntries[i-1].seq {
				continue
			}
			uniq = append(uniq, we)
		}
		q.internal.winEntries = uniq

		items, ids, outBytes = nil, nil, 0
		for _, we := range q.internal.winEntries {
			if len(items) == q.Limit {
				break
			}
			val, stored, size, ok, err := db.readValue(q, we)
			if err != nil {
				return items, ids, err
			}
			if ok {
				items = append(items, val)
				outBytes += int64(size)
				if withIDs {
					// The stored prefix: the time the entry was put, and its
					// contract; and its sequence.
					id := message.NewID(we.seq)
					copy(id[:8], stored[:8])
					ids = append(ids, id)
				}
			}
		}
		if len(items) == q.Limit || found < fetch || fetch >= q.internal.opts.maxQueryLimit {
			break
		}
	}
	db.internal.meter.OutBytes.Inc(outBytes)
	db.internal.meter.Gets.Inc(int64(len(items)))
	db.internal.meter.OutMsgs.Inc(int64(len(items)))
	return items, ids, nil
}

// readValue reads and decodes the message for a window entry, and returns
// the stored prefix of its id. It reports false for entries that are deleted
// or outside the query's contract or cutoff.
func (db *DB) readValue(q *Query, we _Query) ([]byte, []byte, uint32, bool, error) {
	if we.seq == 0 {
		return nil, nil, 0, false, nil
	}
	s, err := db.readEntry(we)
	if err == errMsgIDDeleted {
		return nil, nil, 0, false, nil
	}
	if err != nil {
		logger.Error().Err(err).Str("context", "db.readEntry")
		return nil, nil, 0, false, err
	}
	id, val, err := db.internal.reader.readMessage(s)
	if err != nil {
		logger.Error().Err(err).Str("context", "data.readMessage")
		return nil, nil, 0, false, err
	}
	if !message.ID(id).EvalPrefix(q.Contract, q.internal.cutoff) {
		return nil, nil, 0, false, nil
	}

	// last bit of ID is an encryption flag.
	if uint8(id[idSize-1]) == 1 {
		if db.internal.mac == nil {
			return nil, nil, 0, false, ErrNoEncryptionKey
		}
		val, err = db.internal.mac.Decrypt(nil, val)
		if err != nil {
			logger.Error().Err(err).Str("context", "mac.decrypt")
			return nil, nil, 0, false, err
		}
	}
	val, err = snappy.Decode(nil, val)
	if err != nil {
		logger.Error().Err(err).Str("context", "snappy.Decode")
		return nil, nil, 0, false, err
	}
	return val, id, s.valueSize, true, nil
}

// NewContract generates a new Contract.
func (db *DB) NewContract() (uint32, error) {
	raw := make([]byte, 4)
	if _, err := rand.Read(raw); err != nil {
		return 0, err
	}

	contract := uint32(binary.LittleEndian.Uint32(raw[:4]))
	return contract, nil
}

// NewID generates new ID that is later used to put entry or delete entry.
func (db *DB) NewID() []byte {
	db.internal.meter.Leases.Inc(1)
	return message.NewID(db.nextSeq())
}

// Put puts entry into DB. It uses default Contract to put entry into DB.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (db *DB) Put(topic, payload []byte) error {
	return db.PutEntry(NewEntry(topic, payload))
}

// PutEntry puts entry into the DB, if Contract is not specified then it uses master Contract.
// It is safe to modify the contents of the argument after PutEntry returns but not
// before.
func (db *DB) PutEntry(e *Entry) error {
	if err := db.ok(); err != nil {
		return err
	}

	switch {
	case len(e.Topic) == 0:
		return errTopicEmpty
	case len(e.Topic) > maxTopicLength:
		return errTopicTooLarge
	case len(e.Payload) == 0:
		return errValueEmpty
	case len(e.Payload) > maxValueLength:
		return errValueTooLarge
	}

	if err := db.setEntry(e); err != nil {
		return err
	}

	timeID, err := db.internal.mem.Put(e.entry.seq, e.entry.cache)
	if err != nil {
		return err
	}

	if ok := db.internal.timeWindow.add(timeID, e.entry.topicHash, newWinEntry(e.entry.seq, e.entry.expiresAt)); !ok {
		return errForbidden
	}

	db.internal.meter.Puts.Inc(1)

	// reset message entry.
	e.reset()
	return nil
}

// Delete sets entry for deletion.
// It is safe to modify the contents of the argument after Delete returns but not
// before.
func (db *DB) Delete(id, topic []byte) error {
	return db.DeleteEntry(NewEntry(topic, nil).WithID(id))
}

// DeleteEntry deletes an entry from DB. you must provide an ID to delete an entry.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (db *DB) DeleteEntry(e *Entry) error {
	if err := db.ok(); err != nil {
		return err
	}

	switch {
	case db.opts.flags.immutable:
		return errImmutable
	case len(e.ID) == 0:
		return errMsgIDEmpty
	case len(e.Topic) == 0:
		return errTopicEmpty
	case len(e.Topic) > maxTopicLength:
		return errTopicTooLarge
	}
	id := message.ID(e.ID)
	topic, _, err := db.parseTopic(e.Contract, e.Topic)
	if err != nil {
		return err
	}
	if e.Contract == 0 {
		e.Contract = message.MasterContract
	}
	topic.AddContract(e.Contract)

	if err := db.delete(topic.GetHash(e.Contract), message.ID(id).Sequence()); err != nil {
		return err
	}

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

	// If an error is returned from the function then rollback and return error.
	if err := fn(b, b.commitComplete); err != nil {
		b.unsetManaged()
		b.Abort()
		close(b.commitComplete)
		return err
	}
	b.unsetManaged()
	return b.Commit()
}

// Flush writes the entries put so far to the write-ahead log, and returns
// once they are written. A Put is written there in the background, usually
// within milliseconds but with no bound under load: until then, a crash
// loses it. After Flush returns, the entries put before it are recovered
// after a crash. Batch waits for its own write already.
func (db *DB) Flush() error {
	if err := db.ok(); err != nil {
		return err
	}
	return db.internal.mem.Flush()
}

// Sync syncs entries into DB. Sync happens synchronously.
// Sync write window entries into summary file and write index, and data to respective index and data files.
// In case of any error during sync operation recovery is performed on log file (write ahead log).
func (db *DB) Sync() error {
	return db.syncOnce()
}

func (db *DB) syncOnce() error {
	// Sync happens synchronously. If a sync is in progress, wait for it and then
	// sync whatever it did not cover; close holds the lock for good.
	select {
	case db.internal.syncLockC <- struct{}{}:
	case <-db.internal.closeC:
		return errClosed
	}
	defer func() {
		<-db.internal.syncLockC
	}()

	if ok := db.internal.syncHandle.startSync(); !ok {
		return nil
	}
	defer func() {
		db.internal.syncHandle.finish()
	}()
	return db.internal.syncHandle.Sync()
}

// FileSize returns the total size of the disk storage used by the DB.
func (db *DB) FileSize() (int64, error) {
	return db.fs.size()
}

// Count returns the number of items in the DB.
func (db *DB) Count() uint64 {
	return atomic.LoadUint64(&db.internal.dbInfo.count)
}
