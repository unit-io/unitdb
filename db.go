package tracedb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"github.com/unit-io/bpool"
	"github.com/unit-io/tracedb/crypto"
	fltr "github.com/unit-io/tracedb/filter"
	"github.com/unit-io/tracedb/fs"
	"github.com/unit-io/tracedb/message"
	"github.com/unit-io/tracedb/wal"
)

const (
	entriesPerIndexBlock = 150 // (4096 i.e blocksize/26 i.e entry size)
	seqsPerWindowBlock   = 508 // ((4096 i.e. blocksize - 26 fixed)/8 i.e. window entry size)
	// MaxBlocks       = math.MaxUint32
	nShards       = 16
	indexPostfix  = ".index"
	dataPostfix   = ".data"
	windowPostfix = ".win"
	logPostfix    = ".log"
	leasePostfix  = ".lease"
	lockPostfix   = ".lock"
	idSize        = 24
	filterPostfix = ".filter"
	version       = 1 // file format version

	// maxExpDur expired keys are deleted from DB after durType*maxExpDur.
	// For example if durType is Minute and maxExpDur then
	// all expired keys are deleted from db in 1 minutes
	maxExpDur = 1

	// MaxTopicLength is the maximum size of a topic in bytes.
	MaxTopicLength = 1 << 16

	// MaxValueLength is the maximum size of a value in bytes.
	MaxValueLength = 1 << 30

	// MaxKeys is the maximum numbers of keys in the DB.
	MaxKeys = math.MaxInt64

	// MaxSeq is the maximum number of seq supported.
	MaxSeq = uint64(1<<56 - 1)

	// Maximum number of records to return
	// maxResults = 100000
)

type (
	dbInfo struct {
		encryption   uint8
		seq          uint64
		logSeq       uint64
		count        int64
		blockIdx     int32
		windowIdx    int32
		freeblockOff int64
		cacheID      uint64
	}

	// DB represents the message storage for topic->keys-values.
	// All DB methods are safe for concurrent use by multiple goroutines.
	DB struct {
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
		once       Once

		//batchdb
		*batchdb
		//trie
		trie *trie
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
)

// Open opens or creates a new DB.
func Open(path string, opts *Options) (*DB, error) {
	opts = opts.copyWithDefaults()
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
	timeOptions := &timeOptions{expDurationType: time.Minute, maxExpDurations: maxExpDur, backgroundKeyExpiry: opts.BackgroundKeyExpiry}
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
			blockIdx:     -1,
			freeblockOff: -1,
		},
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
	db.encryption = 0
	if opts.Encryption {
		db.encryption = 1
	}

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

	if opts.BackgroundKeyExpiry {
		db.startExpirer(time.Minute, maxExpDur)
	}
	return db, nil
}

func (db *DB) writeHeader(writeFreeList bool) error {
	if writeFreeList {
		db.lease.defrag()
		if err := db.lease.write(); err != nil {
			logger.Error().Err(err).Str("context", "db.writeHeader")
			return err
		}
	}
	h := header{
		signature: signature,
		version:   version,
		dbInfo: dbInfo{
			encryption:   db.encryption,
			seq:          atomic.LoadUint64(&db.seq),
			count:        atomic.LoadInt64(&db.count),
			blockIdx:     db.blocks(),
			windowIdx:    db.timeWindow.windowIndex(),
			freeblockOff: db.freeblockOff,
			cacheID:      db.cacheID,
		},
	}
	return db.index.writeMarshalableAt(h, 0)
}

func (db *DB) readHeader() error {
	h := &header{}
	if err := db.index.readUnmarshalableAt(h, headerSize, 0); err != nil {
		logger.Error().Err(err).Str("context", "db.readHeader")
		return err
	}
	// if !bytes.Equal(h.signature[:], signature[:]) {
	// 	return errCorrupted
	// }
	db.dbInfo = h.dbInfo
	db.timeWindow.setWindowIndex(db.dbInfo.windowIdx)
	if err := db.lease.read(); err != nil {
		if err == io.EOF {
			return nil
		}
		logger.Error().Err(err).Str("context", "db.readHeader")
		return err
	}
	return nil
}

// loadTopicHash loads topic and offset from window file
func (db *DB) loadTrie() error {
	err := db.timeWindow.foreachWindowBlock(func(curw windowHandle) (bool, error) {
		w := &curw
		wOff, ok := db.trie.getOffset(w.topicHash)
		if !ok || wOff < w.offset {
			if ok := db.trie.setOffset(w.topicHash, w.offset); !ok {
				if w.entryIdx == 0 {
					return false, nil
				}
				seq := w.winEntries[w.entryIdx-1].seq
				off := blockOffset(startBlockIndex(seq))
				b := blockHandle{file: db.index, offset: off}
				if err := b.read(); err != nil {
					if err == io.EOF {
						fmt.Println("db.loadTopicHash: eof ", w.topicHash, off, seq)
						return false, nil
					}
					return true, err
				}
				entryIdx := -1
				for i := 0; i < entriesPerIndexBlock; i++ {
					e := b.entries[i]
					if e.seq == seq { //record exist in db
						entryIdx = i
						break
					}
				}
				if entryIdx == -1 {
					fmt.Println("db.loadTopicHash: topicHash, off seq ", w.topicHash, off, seq)
					// return false, errors.New("db.loadTopicHash: unable to get topic from db.")
					return false, nil
				}
				e := b.entries[entryIdx]
				t, err := db.data.readTopic(e)
				if err != nil {
					return true, err
				}
				topic := new(message.Topic)
				err = topic.Unmarshal(t)
				if err != nil {
					return true, err
				}
				if ok := db.trie.add(message.Contract(topic.Parts), w.topicHash, topic.Parts, topic.Depth); ok {
					if ok := db.trie.setOffset(w.topicHash, w.offset); !ok {
						return true, errors.New("db.loadTopicHash: unable to set topic offset to topic trie")
					}
				}
			}
		}
		return false, nil
	})
	return err
}

// Close closes the DB.
func (db *DB) Close() error {
	if !db.setClosed() {
		return errClosed
	}

	// Signal all goroutines.
	close(db.closeC)
	time.Sleep(db.opts.TinyBatchWriteInterval)

	// Acquire lock.
	db.writeLockC <- struct{}{}
	db.syncLockC <- struct{}{}

	// Wait for all goroutines to exit.
	db.closeW.Wait()

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

func (db *DB) readEntry(contract uint64, seq uint64) (entry, error) {
	cacheKey := db.cacheID ^ seq
	e := entry{}
	if data, _ := db.mem.Get(contract, cacheKey); data != nil {
		e.UnmarshalBinary(data[:entrySize])
		e.cacheBlock = make([]byte, len(data[entrySize:]))
		copy(e.cacheBlock, data[entrySize:])
		return e, nil
	}

	off := blockOffset(startBlockIndex(seq))
	bh := blockHandle{file: db.index, offset: off}
	if err := bh.read(); err != nil {
		return entry{}, err
	}

	for i := 0; i < entriesPerIndexBlock; i++ {
		e := bh.entries[i]
		if e.seq == seq {
			return e, nil
		}
	}
	// return entry{}, errMsgIdDoesNotExist
	return entry{}, nil
}

// Get return items matching the query paramater
func (db *DB) Get(q *Query) (items [][]byte, err error) {
	if err := db.ok(); err != nil {
		return nil, err
	}
	switch {
	case len(q.Topic) == 0:
		return nil, errTopicEmpty
	case len(q.Topic) > MaxTopicLength:
		return nil, errTopicTooLarge
	}
	// // CPU profiling by default
	// defer profile.Start().Stop()
	topic := new(message.Topic)
	if q.Contract == 0 {
		q.Contract = message.MasterContract
	}
	//Parse the Key
	topic.ParseKey(q.Topic)
	// Parse the topic
	topic.Parse(q.Contract, false)
	if topic.TopicType == message.TopicInvalid {
		return nil, errBadRequest
	}
	// // Get should only have static topic strings
	// if topic.TopicType != message.TopicStatic {
	// 	return errForbidden
	// }
	topic.AddContract(q.Contract)
	q.parts = topic.Parts
	q.contract = message.Contract(q.parts)

	// In case of last, include it to the query
	if from, limit, ok := topic.Last(); ok {
		q.cutoff = from.Unix()
		switch {
		case (q.Limit == 0 && limit == 0):
			q.Limit = db.opts.DefaultQueryLimit
		case q.Limit > db.opts.MaxQueryLimit || limit > db.opts.MaxQueryLimit:
			q.Limit = db.opts.MaxQueryLimit
		case limit > q.Limit:
			q.Limit = limit
		}
	}

	mu := db.getMutex(q.contract)
	mu.RLock()
	defer mu.RUnlock()
	// lookups are performed in following order
	// ilookup lookups in memory entries from timeWindow those are not yet sync
	// lookup lookups persisted entries if fanout is true
	// lookup gets most recent entries without need for sorting.
	topics, topicOffsets := db.trie.lookup(q.contract, q.parts, q.Limit)
	for i, topicHash := range topics {
		var wEntries []winEntry
		nextOff := int64(0)
		q.winEntries = db.timeWindow.ilookup(topicHash, q.Limit)
		if len(q.winEntries) < q.Limit {
			limit := q.Limit - len(q.winEntries)
			wEntries, nextOff = db.timeWindow.lookup(topicHash, topicOffsets[i], len(q.winEntries), limit)
			q.winEntries = append(q.winEntries, wEntries...)
		}
		if len(q.winEntries) == 0 {
			return
		}

		expiryCount := 0
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
					e, err := db.readEntry(q.contract, we.seq)
					if err != nil {
						logger.Error().Err(err).Str("context", "db.readEntry")
						return err
					}
					if e.isExpired() {
						expiryCount++
						if err := db.timeWindow.addExpiry(e); err != nil {
							logger.Error().Err(err).Str("context", "timeWindow.addExpiry")
							return err
						}
						// if id is expired it does not return an error but continue the iteration
						return nil
					}
					id, val, err := db.data.readMessage(e)
					if err != nil {
						logger.Error().Err(err).Str("context", "data.readMessage")
						return err
					}
					msgId := message.ID(id)
					if !msgId.EvalPrefix(q.contract, q.cutoff) {
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

			if cap(q.winEntries) == limit && cap(q.winEntries) < q.Limit && nextOff > 0 {
				limit := q.Limit - len(items)
				wEntries, nextOff = db.timeWindow.lookup(topicHash, nextOff, 0, limit)
				if len(wEntries) == 0 {
					break
				}
				q.winEntries = append(q.winEntries, wEntries...)
			}

			if expiryCount == 0 || len(items) >= int(q.Limit) || cap(q.winEntries) == limit {
				break
			}

			if cap(q.winEntries) < int(q.Limit+expiryCount) {
				start = limit
				limit = cap(q.winEntries)
			} else {
				start = limit
				limit = limit + expiryCount
			}
		}
		db.meter.Gets.Inc(int64(len(items)))
		db.meter.OutMsgs.Inc(int64(len(items)))
	}
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
	case len(q.Topic) > MaxTopicLength:
		return nil, errTopicTooLarge
	}
	topic := new(message.Topic)
	if q.Contract == 0 {
		q.Contract = message.MasterContract
	}
	//Parse the Key
	topic.ParseKey(q.Topic)
	// Parse the topic
	topic.Parse(q.Contract, false)
	if topic.TopicType == message.TopicInvalid {
		return nil, errBadRequest
	}
	// // Iterator should only have static topic strings
	// if topic.TopicType != message.TopicStatic {
	// 	return errForbidden
	// }
	topic.AddContract(q.Contract)
	q.parts = topic.Parts
	q.contract = message.Contract(q.parts)

	// In case of ttl, include it to the query
	if from, limit, ok := topic.Last(); ok {
		q.cutoff = from.Unix()
		switch {
		case (q.Limit == 0 && limit == 0):
			q.Limit = db.opts.DefaultQueryLimit
		case q.Limit > db.opts.MaxQueryLimit || limit > db.opts.MaxQueryLimit:
			q.Limit = db.opts.MaxQueryLimit
		case limit > q.Limit:
			q.Limit = limit
		}
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

// newBlock adds new block to DB and it returns block offset
func (db *DB) newBlock() (int64, error) {
	off, err := db.index.extend(blockSize)
	db.addBlocks(1)
	return off, err
}

// newBlock adds new block to DB and it returns block offset
func (db *DB) extendBlocks(nBlocks int32) error {
	if _, err := db.index.extend(uint32(nBlocks) * blockSize); err != nil {
		return err
	}
	db.addBlocks(nBlocks)
	return nil
}

func (db *DB) parseTopic(e *Entry) (*message.Topic, int64, error) {
	topic := new(message.Topic)
	if e.Contract == 0 {
		e.Contract = message.MasterContract
	}
	//Parse the Key
	topic.ParseKey(e.Topic)
	// Parse the topic
	topic.Parse(e.Contract, true)
	if topic.TopicType == message.TopicInvalid {
		return nil, 0, errBadRequest
	}
	topic.AddContract(e.Contract)
	// In case of ttl, add ttl to the msg and store to the db
	if ttl, ok := topic.TTL(); ok {
		return topic, ttl, nil
	}
	return topic, 0, nil
}

func (db *DB) setEntry(e *Entry, ttl int64) error {
	//message ID is the database key
	var id message.ID
	var seq uint64
	encryption := db.encryption == 1 || e.encryption
	if e.ExpiresAt == 0 && ttl > 0 {
		e.ExpiresAt = uint32(time.Now().Add(time.Duration(ttl)).Unix())
	}
	if e.ID != nil {
		id = message.ID(e.ID)
		if encryption {
			id.SetEncryption()
		}
		id.AddContract(e.contract)
		seq = id.Seq()
	} else {
		if ok, s := db.data.lease.getSlot(e.contract); ok {
			db.meter.Leased.Inc(1)
			seq = s
		} else {
			seq = db.nextSeq()
		}
		id = message.NewID(seq, encryption)
		id.AddContract(e.contract)
	}
	val := snappy.Encode(nil, e.Payload)
	if seq == 0 {
		panic("db.setEntry: seq is zero")
	}
	e.seq = seq
	e.id = id
	e.val = val
	return nil
}

// Put puts entry into DB. It uses default Contract to put entry into DB.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (db *DB) Put(topic, value []byte) error {
	return db.PutEntry(NewEntry(topic, value))
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
	case len(e.Topic) > MaxTopicLength:
		return errTopicTooLarge
	case len(e.Payload) == 0:
		return errValueEmpty
	case len(e.Payload) > MaxValueLength:
		return errValueTooLarge
	}
	var topic *message.Topic
	var ttl int64
	var err error
	if !e.parsed {
		topic, ttl, err = db.parseTopic(e)
		if err != nil {
			return err
		}
		e.topic = topic.Marshal()
		e.contract = message.Contract(topic.Parts)
		e.topicHash = topic.GetHash(e.contract)
		if ok := db.trie.add(e.contract, e.topicHash, topic.Parts, topic.Depth); !ok {
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
	we := winEntry{
		contract: e.contract,
		seq:      e.seq,
	}
	if err := db.timeWindow.add(e.topicHash, we); err != nil {
		return err
	}
	data, err := db.packEntry(e)
	if err != nil {
		return err
	}
	memseq := db.cacheID ^ e.seq
	if err := db.mem.Set(e.contract, memseq, data); err != nil {
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

// SetEntry sets payload to the provided entry and out the entry into the DB, if Contract is not specified then it uses master Contract.
// It is safe to modify the contents of the argument after PutEntry returns but not
// before.
func (db *DB) SetEntry(e *Entry, payload []byte) error {
	if err := db.ok(); err != nil {
		return err
	}
	e.SetPayload(payload)
	return db.PutEntry(e)
}

// packEntry marshal entry and message data
func (db *DB) packEntry(e *Entry) ([]byte, error) {
	if db.Count() == MaxKeys {
		return nil, errFull
	}
	e1 := entry{
		seq:       e.seq,
		topicSize: uint16(len(e.topic)),
		valueSize: uint32(len(e.val)),
		expiresAt: e.ExpiresAt,

		// topicOffset: e.topicOffset,
	}
	data, _ := e1.MarshalBinary()
	mLen := idSize + len(e.topic) + len(e.val)
	m := make([]byte, mLen)
	copy(m, e.id)
	copy(m[idSize:], e.topic)
	copy(m[len(e.topic)+idSize:], e.val)
	data = append(data, m...)

	return data, nil
}

// tinyCommit commits tiny batch to write ahead log
func (db *DB) tinyCommit() error {
	if err := db.ok(); err != nil {
		return err
	}

	if db.tinyBatch.count() == 0 {
		return nil
	}

	logWriter, err := db.wal.NewWriter()
	if err != nil {
		return err
	}
	// commit writes batches into write ahead log. The write happen synchronously.
	db.closeW.Add(1)
	db.writeLockC <- struct{}{}
	defer func() {
		db.tinyBatch.buffer.Reset()
		<-db.writeLockC
		db.closeW.Done()
	}()
	offset := uint32(0)
	buf := db.tinyBatch.buffer.Bytes()
	for i := uint32(0); i < db.tinyBatch.count(); i++ {
		dataLen := binary.LittleEndian.Uint32(buf[offset : offset+4])
		data := buf[offset+4 : offset+dataLen]
		if err := <-logWriter.Append(data); err != nil {
			return err
		}
		offset += dataLen
	}

	db.meter.Puts.Inc(int64(db.tinyBatch.count()))
	db.setLogSeq(db.Seq())
	if err := <-logWriter.SignalInitWrite(db.LogSeq()); err != nil {
		return err
	}
	db.tinyBatch.reset()
	return nil
}

// commit commits batches to write ahead log
func (db *DB) commit(l int, buf *bpool.Buffer) error {
	if err := db.ok(); err != nil {
		return err
	}

	// commit writes batches into write ahead log. The write happen synchronously.
	db.writeLockC <- struct{}{}
	db.closeW.Add(1)
	defer func() {
		buf.Reset()
		<-db.writeLockC
		db.closeW.Done()
	}()

	logWriter, err := db.wal.NewWriter()
	if err != nil {
		return err
	}

	offset := uint32(0)
	data := buf.Bytes()
	for i := 0; i < l; i++ {
		dataLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		if err := <-logWriter.Append(data[offset+4 : offset+dataLen]); err != nil {
			return err
		}
		offset += dataLen
	}

	db.meter.Puts.Inc(int64(l))
	db.setLogSeq(db.Seq())
	return <-logWriter.SignalInitWrite(db.LogSeq())
}

// Delete sets entry for deletion.
// It is safe to modify the contents of the argument after Delete returns but not
// before.
func (db *DB) Delete(id, topic []byte) error {
	return db.DeleteEntry(&Entry{ID: id, Topic: topic})
}

// DeleteEntry deletes an entry from DB. you must provide an ID to delete an entry.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (db *DB) DeleteEntry(e *Entry) error {
	switch {
	case len(e.ID) == 0:
		return errMsgIdEmpty
	case len(e.Topic) == 0:
		return errTopicEmpty
	case len(e.Topic) > MaxTopicLength:
		return errTopicTooLarge
	}
	// message ID is the database key
	id := message.ID(e.ID)
	if err := db.delete(message.ID(id).Seq()); err != nil {
		return err
	}
	return nil
}

// delete deletes the given key from the DB.
func (db *DB) delete(seq uint64) error {
	//// Test filter block for the message id presence
	if !db.filter.Test(seq) {
		return nil
	}
	db.meter.Dels.Inc(1)
	blockIdx := startBlockIndex(seq)
	if blockIdx > db.blocks() {
		return nil // no record to delete
	}
	blockWriter := newBlockWriter(&db.index, nil)
	e, err := blockWriter.del(seq)
	if err != nil {
		return err
	}
	db.lease.free(e.seq, e.msgOffset, e.mSize())
	db.decount(1)
	if db.syncWrites {
		return db.sync()
	}
	return nil
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

func (db *DB) Seq() uint64 {
	return atomic.LoadUint64(&db.seq)
}

func (db *DB) nextSeq() uint64 {
	return atomic.AddUint64(&db.seq, 1)
}

func (db *DB) LogSeq() uint64 {
	return atomic.LoadUint64(&db.logSeq)
}

func (db *DB) setLogSeq(logSeq uint64) error {
	atomic.StoreUint64(&db.logSeq, logSeq)
	return nil
}

// Count returns the number of items in the DB.
func (db *DB) Count() int64 {
	return atomic.LoadInt64(&db.count)
}

// blocks returns the total blocks in the DB.
func (db *DB) blocks() int32 {
	return atomic.LoadInt32(&db.blockIdx)
}

// addBlock adds new block to the DB.
func (db *DB) addBlocks(nBlocks int32) int32 {
	return atomic.AddInt32(&db.blockIdx, nBlocks)
}

func (db *DB) incount(count int64) int64 {
	return atomic.AddInt64(&db.count, count)
}

func (db *DB) decount(count int64) int64 {
	return atomic.AddInt64(&db.count, -count)
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

// Set closed flag; return true if not already closed.
func (db *DB) setClosed() bool {
	return atomic.CompareAndSwapUint32(&db.closed, 0, 1)
}

// Check whether DB was closed.
func (db *DB) isClosed() bool {
	return atomic.LoadUint32(&db.closed) != 0
}

// Check read ok status.
func (db *DB) ok() error {
	if db.isClosed() {
		return errors.New("wal is closed.")
	}
	return nil
}
