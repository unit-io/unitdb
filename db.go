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
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	bc "github.com/unit-io/unitdb/blockcache"
	"github.com/unit-io/unitdb/crypto"
	fltr "github.com/unit-io/unitdb/filter"
	"github.com/unit-io/unitdb/fs"
	"github.com/unit-io/unitdb/message"
	"github.com/unit-io/unitdb/wal"
)

// DB represents the message storage for topic->keys-values.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	lock  fs.LockFile
	index _File
	data  _DataTable
	opts  *_Options

	internal *_DB

	//batchdb
	batchdb *_Batchdb
}

// Open opens or creates a new DB.
func Open(path string, opts ...Options) (*DB, error) {
	options := &_Options{}
	WithDefaultOptions().set(options)
	WithDefaultFlags().set(options)
	for _, opt := range opts {
		if opt != nil {
			opt.set(options)
		}
	}

	fs := options.fileSystem
	lock, err := fs.CreateLockFile(path + lockPostfix)
	if err != nil {
		if err == os.ErrExist {
			err = errLocked
		}
		return nil, err
	}

	index, err := newFile(fs, path+indexPostfix)
	if err != nil {
		return nil, err
	}

	data, err := newFile(fs, path+dataPostfix)
	if err != nil {
		return nil, err
	}

	leaseFile, err := newFile(fs, path+leasePostfix)
	if err != nil {
		return nil, err
	}
	lease := newLease(leaseFile, options.minimumFreeBlocksSize)

	timeOptions := &_TimeOptions{
		maxDuration:         options.syncDurationType * time.Duration(options.maxSyncDurations),
		expDurationType:     time.Minute,
		maxExpDurations:     maxExpDur,
		backgroundKeyExpiry: options.flags.backgroundKeyExpiry,
	}
	timewindow, err := newFile(fs, path+windowPostfix)
	if err != nil {
		return nil, err
	}

	filter, err := newFile(fs, path+filterPostfix)
	if err != nil {
		return nil, err
	}

	internal := &_DB{
		mutex:      newMutex(),
		timeWindow: newTimeWindowBucket(timewindow, timeOptions),
		freeList:   lease,
		filter:     Filter{file: filter, filterBlock: fltr.NewFilterGenerator()},
		syncLockC:  make(chan struct{}, 1),
		dbInfo: _DBInfo{
			blockIdx: -1,
		},
		trie:  newTrie(),
		start: time.Now(),
		meter: NewMeter(),
		// Close
		closeC: make(chan struct{}),
	}

	db := &DB{
		lock:  lock,
		index: index,
		data:  _DataTable{file: data, lease: lease, offset: data.Size()},
		opts:  options,

		internal: internal,
		batchdb:  &_Batchdb{},
	}

	if index.size == 0 {
		if data.size != 0 {
			if err := index.Close(); err != nil {
				logger.Error().Err(err).Str("context", "db.Open")
			}
			if err := data.Close(); err != nil {
				logger.Error().Err(err).Str("context", "db.Open")
			}
			if err := lock.Unlock(); err != nil {
				logger.Error().Err(err).Str("context", "db.Open")
			}
			// Data file exists, but index is missing.
			return nil, errCorrupted
		}
		// memdb blockcache id.
		db.internal.dbInfo.cacheID = uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
		if err != nil {
			return nil, err
		}
		if _, err = db.index.extend(headerSize); err != nil {
			return nil, err
		}
		if _, err = db.data.extend(headerSize); err != nil {
			return nil, err
		}
		if err := db.writeHeader(); err != nil {
			return nil, err
		}
	} else {
		if err := db.readHeader(); err != nil {
			if err := index.Close(); err != nil {
				logger.Error().Err(err).Str("context", "db.Open")
			}
			if err := data.Close(); err != nil {
				logger.Error().Err(err).Str("context", "db.Open")
			}
			if err := lock.Unlock(); err != nil {
				logger.Error().Err(err).Str("context", "db.Open")
			}
			return nil, err
		}
	}

	// Create a new MAC from the key.
	if db.internal.mac, err = crypto.New(options.encryptionKey); err != nil {
		return nil, err
	}

	// set encryption flag to encrypt messages.
	if db.opts.flags.encryption {
		db.internal.dbInfo.encryption = 1
	}

	// Create a blockcache.
	blockCache, err := bc.Open(options.blockCacheSize, &bc.Options{MaxElapsedTime: 2 * time.Second})
	if err != nil {
		return nil, err
	}
	db.internal.blockCache = blockCache

	//initbatchdb
	if err = db.initbatchdb(options); err != nil {
		return nil, err
	}

	db.internal.filter.blockCache = db.internal.blockCache
	db.internal.filter.cacheID = db.internal.dbInfo.cacheID

	if err := db.loadTrie(); err != nil {
		logger.Error().Err(err).Str("context", "db.loadTrie")
	}

	// Read freeList before DB recovery
	if err := db.internal.freeList.read(); err != nil {
		logger.Error().Err(err).Str("context", "db.readHeader")
		return nil, err
	}

	logOpts := wal.Options{Path: path + logPostfix, TargetSize: options.logSize, BufferSize: options.bufferSize}
	wal, needLogRecovery, err := wal.New(logOpts)
	if err != nil {
		wal.Close()
		logger.Error().Err(err).Str("context", "wal.New")
		return nil, err
	}
	db.internal.closer = wal
	db.internal.wal = wal
	if needLogRecovery {
		if err := db.recoverLog(); err != nil {
			// if unable to recover db then close db.
			panic(fmt.Sprintf("Unable to recover db on sync error %v. Closing db...", err))
		}
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
	if err := db.ok(); err != nil {
		return nil, err
	}
	switch {
	case len(q.Topic) == 0:
		return nil, errTopicEmpty
	case len(q.Topic) > maxTopicLength:
		return nil, errTopicTooLarge
	}
	// // CPU profiling by default
	// defer profile.Start().Stop()
	q.internal.opts = &_QueryOptions{defaultQueryLimit: db.opts.queryOptions.defaultQueryLimit, maxQueryLimit: db.opts.queryOptions.maxQueryLimit}
	if err := q.parse(); err != nil {
		return nil, err
	}
	mu := db.internal.mutex.getMutex(q.internal.prefix)
	mu.RLock()
	defer mu.RUnlock()
	db.lookup(q)
	if len(q.internal.winEntries) == 0 {
		return
	}
	sort.Slice(q.internal.winEntries[:], func(i, j int) bool {
		return q.internal.winEntries[i].seq > q.internal.winEntries[j].seq
	})
	invalidCount := 0
	start := 0
	limit := q.Limit
	if len(q.internal.winEntries) < int(q.Limit) {
		limit = len(q.internal.winEntries)
	}
	for {
		for _, we := range q.internal.winEntries[start:limit] {
			err = func() error {
				if we.seq == 0 {
					return nil
				}
				s, err := db.readEntry(we.topicHash, we.seq)
				if err != nil {
					if err == errMsgIDDeleted {
						invalidCount++
						return nil
					}
					logger.Error().Err(err).Str("context", "db.readEntry")
					return err
				}
				id, val, err := db.data.readMessage(s)
				if err != nil {
					logger.Error().Err(err).Str("context", "data.readMessage")
					return err
				}
				msgID := message.ID(id)
				if !msgID.EvalPrefix(q.Contract, q.internal.cutoff) {
					invalidCount++
					return nil
				}

				// last bit of ID is an encryption flag.
				if uint8(id[idSize-1]) == 1 {
					val, err = db.internal.mac.Decrypt(nil, val)
					if err != nil {
						logger.Error().Err(err).Str("context", "mac.decrypt")
						return err
					}
				}
				var buffer []byte
				val, err = snappy.Decode(buffer, val)
				if err != nil {
					logger.Error().Err(err).Str("context", "snappy.Decode")
					return err
				}
				items = append(items, val)
				db.internal.meter.OutBytes.Inc(int64(s.valueSize))
				return nil
			}()
			if err != nil {
				return items, err
			}
		}

		if invalidCount == 0 || len(items) == int(q.Limit) || len(q.internal.winEntries) == limit {
			break
		}

		if len(q.internal.winEntries) <= int(q.Limit+invalidCount) {
			start = limit
			limit = len(q.internal.winEntries)
		} else {
			start = limit
			limit = limit + invalidCount
		}
	}
	db.internal.meter.Gets.Inc(int64(len(items)))
	db.internal.meter.OutMsgs.Inc(int64(len(items)))
	return items, nil
}

// Items returns a new ItemIterator.
func (db *DB) Items(q *Query) (*ItemIterator, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}
	switch {
	case len(q.Topic) == 0:
		return nil, errTopicEmpty
	case len(q.Topic) > maxTopicLength:
		return nil, errTopicTooLarge
	}

	q.internal.opts = &_QueryOptions{defaultQueryLimit: db.opts.queryOptions.defaultQueryLimit, maxQueryLimit: db.opts.queryOptions.maxQueryLimit}
	if err := q.parse(); err != nil {
		return nil, err
	}

	return &ItemIterator{db: db, query: q}, nil
}

// NewContract generates a new Contract.
func (db *DB) NewContract() (uint32, error) {
	raw := make([]byte, 4)
	rand.Read(raw)

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

	db.batchdb.tinyBatchLockC <- struct{}{}
	defer func() {
		<-db.batchdb.tinyBatchLockC
	}()

	if err := db.setEntry(db.batchdb.tinyBatch.timeID(), e); err != nil {
		return err
	}

	if e.entry.topicSize != 0 {
		t := new(message.Topic)
		rawTopic := e.entry.cache[entrySize+idSize : entrySize+idSize+e.entry.topicSize]
		t.Unmarshal(rawTopic)
		db.internal.trie.add(newTopic(e.entry.topicHash, 0), t.Parts, t.Depth)
	}

	blockID := startBlockIndex(e.entry.seq)
	memseq := db.internal.dbInfo.cacheID ^ e.entry.seq
	if err := db.internal.blockCache.Set(uint64(blockID), memseq, e.entry.cache); err != nil {
		return err
	}

	if ok := db.internal.timeWindow.add(db.batchdb.tinyBatch.timeID(), e.entry.topicHash, newWinEntry(e.entry.seq, e.entry.expiresAt)); !ok {
		return errForbidden
	}

	db.batchdb.tinyBatch.entries = append(db.batchdb.tinyBatch.entries, e.entry.seq)
	db.batchdb.tinyBatch.incount()
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

// Sync syncs entries into DB. Sync happens synchronously.
// Sync write window entries into summary file and write index, and data to respective index and data files.
// In case of any error during sync operation recovery is performed on log file (write ahead log).
func (db *DB) Sync() error {
	// start := time.Now()
	if ok := db.internal.syncHandle.status(); ok {
		// sync is in-progress.
		return nil
	}

	// Sync happens synchronously.
	db.internal.syncLockC <- struct{}{}
	db.internal.closeW.Add(1)
	defer func() {
		db.internal.closeW.Done()
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
	var err error
	is, err := db.index.Stat()
	if err != nil {
		return -1, err
	}
	ds, err := db.data.file.Stat()
	if err != nil {
		return -1, err
	}
	return is.Size() + ds.Size(), nil
}

// Count returns the number of items in the DB.
func (db *DB) Count() uint64 {
	return atomic.LoadUint64(&db.internal.dbInfo.count)
}
