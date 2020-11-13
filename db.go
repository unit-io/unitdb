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
	"encoding/binary"
	"fmt"
	"math/rand"
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
)

// DB represents the message storage for topic->keys-values.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	opts *_Options

	lock _LockFile
	fs   _FileSet

	internal *_DB
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

	// fsys := options.fileSystem
	lock, err := createLockFile(path + lockPostfix)
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
	if infoFile.Size() == 0 {
		dbInfo = _DBInfo{
			header: _Header{
				signature: signature,
				version:   version,
			},
			blockIdx:  -1,
			windowIdx: -1,
		}
		if _, err = infoFile.extend(fixed); err != nil {
			return nil, err
		}
		if err := infoFile.writeMarshalableAt(dbInfo, 0); err != nil {
			return nil, err
		}
	}

	if err := infoFile.readUnmarshalableAt(&dbInfo, fixed, 0); err != nil {
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
	lease := newLease(leaseFile, options.minimumFreeBlocksSize)

	filterFile, err := newFile(path, 1, _FileDesc{fileType: typeFilter})
	if err != nil {
		return nil, err
	}

	internal := &_DB{
		mutex:      newMutex(),
		dbInfo:     dbInfo,
		info:       infoFile,
		index:      indexFile,
		data:       _DataTable{lease: lease, offset: dataFile.Size()},
		timeWindow: newTimeWindowBucket(dbInfo.windowIdx, timeOptions),
		freeList:   lease,
		filter:     Filter{file: filterFile, filterBlock: fltr.NewFilterGenerator()},
		syncLockC:  make(chan struct{}, 1),
		bufPool:    bpool.NewBufferPool(options.bufferSize, &bpool.Options{MaxElapsedTime: 10 * time.Second}),
		trie:       newTrie(),
		start:      time.Now(),
		meter:      NewMeter(),

		// Close
		closeC: make(chan struct{}),
	}

	// Create a new MAC from the key.
	if internal.mac, err = crypto.New(options.encryptionKey); err != nil {
		return nil, err
	}

	// set encryption flag to encrypt messages.
	if options.flags.encryption {
		internal.dbInfo.encryption = 1
	}

	// Create a blockcache.
	memdb, err := memdb.Open(memdb.WithLogFilePath(path), memdb.WithMemdbSize(options.blockCacheSize))
	if err != nil {
		return nil, err
	}
	internal.mem = memdb

	internal.filter.blockCache = internal.mem

	fileset := _FileSet{mu: new(sync.RWMutex), list: []_FileSet{infoFile, winFile, indexFile, dataFile, leaseFile, filterFile}}

	db := &DB{
		opts: options,

		lock: lock,
		fs:   fileset,

		internal: internal,
	}

	if err := db.loadTrie(); err != nil {
		logger.Error().Err(err).Str("context", "db.loadTrie")
	}

	// Read freeList before DB recovery
	if err := db.internal.freeList.read(); err != nil {
		logger.Error().Err(err).Str("context", "db.readHeader")
		return nil, err
	}

	if err := db.recoverLog(); err != nil {
		// if unable to recover db then close db.
		panic(fmt.Sprintf("Unable to recover db on sync error %v. Closing db...", err))
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
	start := 0
	limit := q.Limit
	if len(q.internal.winEntries) < int(q.Limit) {
		limit = len(q.internal.winEntries)
	}

	dataFile, err := db.fs.getFile(_FileDesc{fileType: typeData})
	if err != nil {
		return nil, err
	}
	data := newDataReader(db.internal.data, dataFile)

	for {
		invalidCount := 0
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
				id, val, err := data.readMessage(s)
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

	if e.entry.topicSize != 0 {
		t := new(message.Topic)
		rawTopic := e.entry.cache[entrySize+idSize : entrySize+idSize+e.entry.topicSize]
		t.Unmarshal(rawTopic)
		db.internal.trie.add(newTopic(e.entry.topicHash, 0), t.Parts, t.Depth)
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
		b.Abort()
		close(b.commitComplete)
		return err
	}
	b.unsetManaged()
	return b.Commit()
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
	size := int64(0)
	indexFile, err := db.fs.getFile(_FileDesc{fileType: typeIndex})
	if err != nil {
		return 0, err
	}
	index := newBlockReader(indexFile)
	dataFile, err := db.fs.getFile(_FileDesc{fileType: typeData})
	if err != nil {
		return 0, err
	}
	data := newDataReader(db.internal.data, dataFile)

	if s, err := index.size(); err == nil {
		size += s
	}
	if s, err := data.size(); err == nil {
		size += s
	}
	return size, err
}

// Count returns the number of items in the DB.
func (db *DB) Count() uint64 {
	return atomic.LoadUint64(&db.internal.dbInfo.count)
}
