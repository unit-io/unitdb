package unitdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math/rand"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/unit-io/unitdb/crypto"
	fltr "github.com/unit-io/unitdb/filter"
	"github.com/unit-io/unitdb/fs"
	"github.com/unit-io/unitdb/memdb"
	"github.com/unit-io/unitdb/message"
	"github.com/unit-io/unitdb/wal"
)

// DB represents the message storage for topic->keys-values.
// All DB methods are safe for concurrent use by multiple goroutines.
type DB struct {
	// Need 64-bit alignment.
	mu sync.RWMutex
	mutex
	mac         *crypto.MAC
	writeLockC  chan struct{}
	syncLockC   chan struct{}
	expiryLockC chan struct{}
	// consistent     *hash.Consistent
	filter     Filter
	lock       fs.LockFile
	index      file
	data       dataTable
	lease      *lease
	wal        *wal.WAL
	syncWrites bool
	dbInfo
	timeWindow *timeWindowBucket
	opts       *Options
	flags      *flags
	mem        *memdb.DB

	//batchdb
	*batchdb
	//trie
	trie *trie
	// sync handler
	syncHandle syncHandle
	// The db start time
	start time.Time
	// The metrics to measure timeseries on message events
	meter *Meter
	// Close.
	closeW sync.WaitGroup
	closeC chan struct{}
	closed uint32
	closer io.Closer
}

// Open opens or creates a new DB.
func Open(path string, opts *Options, flgs ...Flags) (*DB, error) {
	opts = opts.copyWithDefaults()
	flags := &flags{}
	WithDefaultFlags().set(flags)
	for _, flg := range flgs {
		if flg != nil {
			flg.set(flags)
		}
	}
	fs := opts.FileSystem
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
	lease := newLease(leaseFile, opts.MinimumFreeBlocksSize)
	timeOptions := &timeOptions{expDurationType: time.Minute, maxExpDurations: maxExpDur, backgroundKeyExpiry: flags.BackgroundKeyExpiry}
	timewindow, err := newFile(fs, path+windowPostfix)
	if err != nil {
		return nil, err
	}
	filter, err := newFile(fs, path+filterPostfix)
	if err != nil {
		return nil, err
	}
	db := &DB{
		mutex:       newMutex(),
		lock:        lock,
		index:       index,
		data:        dataTable{file: data, lease: lease, offset: data.Size()},
		timeWindow:  newTimeWindowBucket(timewindow, timeOptions),
		lease:       lease,
		filter:      Filter{file: filter, filterBlock: fltr.NewFilterGenerator()},
		writeLockC:  make(chan struct{}, 1),
		syncLockC:   make(chan struct{}, 1),
		expiryLockC: make(chan struct{}, 1),
		dbInfo: dbInfo{
			blockIdx: -1,
		},
		opts:  opts,
		flags: flags,

		batchdb: &batchdb{},
		trie:    newTrie(),
		start:   time.Now(),
		meter:   NewMeter(),
		// Close
		closeC: make(chan struct{}),
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
		// memdb blockcache id
		db.cacheID = uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
		if err != nil {
			return nil, err
		}
		if _, err = db.index.extend(headerSize); err != nil {
			return nil, err
		}
		if _, err = db.data.extend(headerSize); err != nil {
			return nil, err
		}
		if err := db.writeHeader(false); err != nil {
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

	// db.consistent = hash.InitConsistent(int(nMutex), int(nMutex))

	// Create a new MAC from the key.
	if db.mac, err = crypto.New(opts.EncryptionKey); err != nil {
		return nil, err
	}

	// set encryption flag to encrypt messages
	if flags.Encryption {
		db.encryption = 1
	}

	// Create a memdb.
	mem, err := memdb.Open(opts.MemdbSize)
	if err != nil {
		return nil, err
	}
	db.mem = mem

	//initbatchdb
	if err = db.initbatchdb(opts); err != nil {
		return nil, err
	}

	db.filter.cache = db.mem
	db.filter.cacheID = db.cacheID

	if err := db.loadTrie(); err != nil {
		logger.Error().Err(err).Str("context", "db.loadTrie")
		// return nil, err
	}
	db.syncHandle = syncHandle{DB: db, internal: internal{}}

	logOpts := wal.Options{Path: path + logPostfix, TargetSize: opts.LogSize, BufferSize: opts.BufferSize}
	wal, needLogRecovery, err := wal.New(logOpts)
	if err != nil {
		wal.Close()
		logger.Error().Err(err).Str("context", "wal.New")
		return nil, err
	}

	db.closer = wal
	db.wal = wal

	if needLogRecovery {
		if err := db.recoverLog(); err != nil {
			// if unable to recover db then close db
			panic(fmt.Sprintf("Unable to recover db on sync error %v. Closing db...", err))
		}
	}

	db.startSyncer(opts.BackgroundSyncInterval)

	if flags.BackgroundKeyExpiry {
		db.startExpirer(time.Minute, maxExpDur)
	}
	return db, nil
}

// Close closes the DB.
func (db *DB) Close() error {
	if err := db.close(); err != nil {
		db.close()
	}

	if err := db.writeHeader(true); err != nil {
		return err
	}
	if err := db.timeWindow.Close(); err != nil {
		return err
	}
	if err := db.data.Close(); err != nil {
		return err
	}
	if err := db.index.Close(); err != nil {
		return err
	}
	if err := db.filter.close(); err != nil {
		return err
	}
	if err := db.lock.Unlock(); err != nil {
		return err
	}

	//close bufferpool
	db.bufPool.Done()

	// close memdb
	db.mem.Close()

	var err error
	if db.closer != nil {
		if err1 := db.closer.Close(); err == nil {
			err = err1
		}
		db.closer = nil
	}

	db.meter.UnregisterAll()

	return err
}

// Get return items matching the query paramater
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
	q.opts = &QueryOptions{DefaultQueryLimit: db.opts.DefaultQueryLimit, MaxQueryLimit: db.opts.MaxQueryLimit}
	if err := q.parse(); err != nil {
		return nil, err
	}
	mu := db.getMutex(q.contract)
	mu.RLock()
	defer mu.RUnlock()
	db.lookup(q)
	if len(q.winEntries) == 0 {
		return
	}
	sort.Slice(q.winEntries[:], func(i, j int) bool {
		return q.winEntries[i].seq > q.winEntries[j].seq
	})
	invalidCount := 0
	start := 0
	limit := q.Limit
	if len(q.winEntries) < int(q.Limit) {
		limit = len(q.winEntries)
	}
	for {
		for _, we := range q.winEntries[start:limit] {
			err = func() error {
				if we.seq == 0 {
					return nil
				}
				e, err := db.readEntry(we.topicHash, we.seq)
				if err != nil {
					if err == errMsgIdDeleted {
						invalidCount++
						return nil
					}
					logger.Error().Err(err).Str("context", "db.readEntry")
					return err
				}
				id, val, err := db.data.readMessage(e)
				if err != nil {
					logger.Error().Err(err).Str("context", "data.readMessage")
					return err
				}
				msgId := message.ID(id)
				if !msgId.EvalPrefix(q.Contract, q.cutoff) {
					invalidCount++
					return nil
				}

				if msgId.IsEncrypted() {
					val, err = db.mac.Decrypt(nil, val)
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
				db.meter.OutBytes.Inc(int64(e.valueSize))
				return nil
			}()
			if err != nil {
				return items, err
			}
		}

		if invalidCount == 0 || len(items) >= int(q.Limit) || cap(q.winEntries) == limit {
			break
		}

		if cap(q.winEntries) < int(q.Limit+invalidCount) {
			start = limit
			limit = cap(q.winEntries)
		} else {
			start = limit
			limit = limit + invalidCount
		}
	}
	db.meter.Gets.Inc(int64(len(items)))
	db.meter.OutMsgs.Inc(int64(len(items)))
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

	q.opts = &QueryOptions{DefaultQueryLimit: db.opts.DefaultQueryLimit, MaxQueryLimit: db.opts.MaxQueryLimit}
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
	db.meter.Leased.Inc(1)
	return message.NewID(db.nextSeq(), false)
}

// Put puts entry into DB. It uses default Contract to put entry into DB.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (db *DB) Put(topic, payload []byte) error {
	return db.PutEntry(NewEntry(topic).WithPayload(payload))
}

// PutEntry puts entry into the DB, if Contract is not specified then it uses master Contract.
// It is safe to modify the contents of the argument after PutEntry returns but not
// before.
func (db *DB) PutEntry(e *Entry) error {
	if err := db.ok(); err != nil {
		return err
	}
	// The write happen synchronously.
	db.writeLockC <- struct{}{}
	defer func() {
		<-db.writeLockC
	}()
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
	var t *message.Topic
	var ttl uint32
	var err error
	if !e.parsed {
		if e.Contract == 0 {
			e.Contract = message.MasterContract
		}
		t, ttl, err = db.parseTopic(e)
		if err != nil {
			return err
		}
		t.AddContract(e.Contract)
		e.topic.data = t.Marshal()
		e.topic.size = uint16(len(e.topic.data))
		e.contract = message.Contract(t.Parts)
		e.topic.hash = t.GetHash(e.contract)
		// fmt.Println("db.PutEntry: contact, topicHash ", e.contract, e.topic.hash)
		if ok := db.trie.add(topic{hash: e.topic.hash}, t.Parts, t.Depth); !ok {
			return errBadRequest
		}
		e.parsed = true
	}
	if err := db.setEntry(e, ttl); err != nil {
		return err
	}
	// Encryption.
	if db.encryption == 1 {
		e.val = db.mac.Encrypt(nil, e.val)
	}
	if err := db.timeWindow.add(e.topic.hash, winEntry{seq: e.seq, expiresAt: e.ExpiresAt}); err != nil {
		return err
	}
	data, err := db.packEntry(e)
	if err != nil {
		return err
	}
	memseq := db.cacheID ^ e.seq
	if err := db.mem.Set(e.topic.hash, memseq, data); err != nil {
		return err
	}
	var scratch [4]byte
	binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(data)+4))

	if _, err := db.tinyBatch.buffer.Write(scratch[:]); err != nil {
		return err
	}
	if _, err := db.tinyBatch.buffer.Write(data); err != nil {
		return err
	}

	db.tinyBatch.incount()
	e.reset()
	return nil
}

// Delete sets entry for deletion.
// It is safe to modify the contents of the argument after Delete returns but not
// before.
func (db *DB) Delete(id, topic []byte) error {
	return db.DeleteEntry(NewEntry(topic).WithID(id))
}

// DeleteEntry deletes an entry from DB. you must provide an ID to delete an entry.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (db *DB) DeleteEntry(e *Entry) error {
	switch {
	case db.flags.Immutable:
		return errImmutable
	case len(e.ID) == 0:
		return errMsgIdEmpty
	case len(e.Topic) == 0:
		return errTopicEmpty
	case len(e.Topic) > maxTopicLength:
		return errTopicTooLarge
	}
	// message ID is the database key
	id := message.ID(e.ID)
	topic, _, err := db.parseTopic(e)
	if err != nil {
		return err
	}
	if e.Contract == 0 {
		e.Contract = message.MasterContract
	}
	topic.AddContract(e.Contract)
	e.contract = message.Contract(topic.Parts)
	if err := db.delete(topic.GetHash(e.contract), message.ID(id).Seq()); err != nil {
		return err
	}
	return nil
}

// Sync syncs entries into DB. Sync happens synchronously.
// Sync write window entries into summary file and write index, and data to respective index and data files.
// In case of any error during sync operation recovery is performed on log file (write ahead log).
func (db *DB) Sync() error {
	// start := time.Now()
	if ok := db.syncHandle.status(); ok {
		// sync is in-progress
		return nil
	}

	// Sync happens synchronously
	db.syncLockC <- struct{}{}
	db.closeW.Add(1)
	defer func() {
		<-db.syncLockC
		db.closeW.Done()
		// db.meter.TimeSeries.AddTime(time.Since(start))
	}()

	if ok := db.syncHandle.startSync(); !ok {
		return nil
	}
	defer func() {
		db.syncHandle.finish()
	}()
	return db.syncHandle.Sync()
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

// Count returns the number of items in the DB.
func (db *DB) Count() int64 {
	return atomic.LoadInt64(&db.count)
}
