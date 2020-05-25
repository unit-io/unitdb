package unitdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
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
	"github.com/unit-io/unitdb/fs"
	"github.com/unit-io/unitdb/memdb"
	"github.com/unit-io/unitdb/message"
	"github.com/unit-io/unitdb/wal"
)

const (
	entriesPerIndexBlock = 185 // (4096 i.e blocksize/26 i.e entry size)
	seqsPerWindowBlock   = 335 // ((4096 i.e. blocksize - 26 fixed)/12 i.e. window entry size)
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
		encryption   int8
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
		opts       *Options
		flags      *Flags
		mem        *memdb.DB

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
func Open(path string, flags *Flags, opts *Options) (*DB, error) {
	opts = opts.copyWithDefaults()
	flags = flags.copyWithDefaults()
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
	timeOptions := &timeOptions{expDurationType: time.Minute, maxExpDurations: maxExpDur, backgroundKeyExpiry: flags.BackgroundKeyExpiry == 1}
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
	db.encryption = flags.Encryption

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

	if flags.BackgroundKeyExpiry == 1 {
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
				we := w.entries[w.entryIdx-1]
				off := blockOffset(startBlockIndex(we.Seq()))
				b := blockHandle{file: db.index, offset: off}
				if err := b.read(); err != nil {
					if err == io.EOF {
						return false, nil
					}
					return true, err
				}
				entryIdx := -1
				for i := 0; i < entriesPerIndexBlock; i++ {
					e := b.entries[i]
					if e.seq == we.Seq() { //record exist in db
						entryIdx = i
						break
					}
				}
				if entryIdx == -1 {
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
				if ok := db.trie.add(w.topicHash, topic.Parts, topic.Depth); ok {
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

func (db *DB) readEntry(topicHash uint64, seq uint64) (entry, error) {
	cacheKey := db.cacheID ^ seq
	e := entry{}
	data, err := db.mem.Get(topicHash, cacheKey)
	if err != nil {
		return entry{}, errMsgIdDeleted
	}
	if data != nil {
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
	q.db = db
	q.opts = &QueryOptions{DefaultQueryLimit: db.opts.DefaultQueryLimit, MaxQueryLimit: db.opts.MaxQueryLimit}
	if err := q.parse(); err != nil {
		return nil, err
	}
	mu := db.getMutex(q.contract)
	mu.RLock()
	defer mu.RUnlock()
	q.lookup()
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
	case len(q.Topic) > MaxTopicLength:
		return nil, errTopicTooLarge
	}

	q.db = db
	q.opts = &QueryOptions{DefaultQueryLimit: db.opts.DefaultQueryLimit, MaxQueryLimit: db.opts.MaxQueryLimit}
	if err := q.parse(); err != nil {
		return nil, err
	}

	return &ItemIterator{query: q}, nil
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

func (db *DB) parseTopic(e *Entry) (*message.Topic, uint32, error) {
	topic := new(message.Topic)

	//Parse the Key
	topic.ParseKey(e.Topic)
	// Parse the topic
	topic.Parse(e.Contract, true)
	if topic.TopicType == message.TopicInvalid {
		return nil, 0, errBadRequest
	}
	// In case of ttl, add ttl to the msg and store to the db
	if ttl, ok := topic.TTL(); ok {
		return topic, ttl, nil
	}
	return topic, 0, nil
}

func (db *DB) setEntry(e *Entry, ttl uint32) error {
	//message ID is the database key
	var id message.ID
	var seq uint64
	e.encryption = db.encryption == 1 || e.encryption
	if e.ExpiresAt == 0 && ttl > 0 {
		e.ExpiresAt = ttl
	}
	if e.ID != nil {
		id = message.ID(e.ID)
		if e.encryption {
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
		id = message.NewID(seq, e.encryption)
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
	var ttl uint32
	var err error
	if !e.parsed {
		if e.Contract == 0 {
			e.Contract = message.MasterContract
		}
		topic, ttl, err = db.parseTopic(e)
		if err != nil {
			return err
		}
		topic.AddContract(e.Contract)
		e.topic.data = topic.Marshal()
		e.topic.size = uint16(len(e.topic.data))
		e.contract = message.Contract(topic.Parts)
		e.topic.hash = topic.GetHash(e.contract)
		// fmt.Println("db.PutEntry: contact, topicHash ", e.contract, e.topic.hash)
		if ok := db.trie.add(e.topic.hash, topic.Parts, topic.Depth); !ok {
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
		topicSize: e.topic.size,
		valueSize: uint32(len(e.val)),
		expiresAt: e.ExpiresAt,

		// topicOffset: e.topicOffset,
	}
	data, _ := e1.MarshalBinary()
	mLen := idSize + int(e.topic.size) + len(e.val)
	m := make([]byte, mLen)
	copy(m, e.id)
	copy(m[idSize:], e.topic.data)
	copy(m[int(e.topic.size)+idSize:], e.val)
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
	case db.flags.Immutable == 1:
		return errImmutable
	case len(e.ID) == 0:
		return errMsgIdEmpty
	case len(e.Topic) == 0:
		return errTopicEmpty
	case len(e.Topic) > MaxTopicLength:
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

// delete deletes the given key from the DB.
func (db *DB) delete(topicHash, seq uint64) error {
	if db.flags.Immutable == 1 {
		return nil
	}
	// Test filter block for the message id presence
	if !db.filter.Test(seq) {
		return nil
	}
	db.meter.Dels.Inc(1)
	memseq := db.cacheID ^ seq
	if err := db.mem.Remove(topicHash, memseq); err != nil {
		return err
	}
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

// Seq current sequence of the DB.
func (db *DB) Seq() uint64 {
	return atomic.LoadUint64(&db.seq)
}

func (db *DB) nextSeq() uint64 {
	return atomic.AddUint64(&db.seq, 1)
}

// LogSeq current log sequence of the DB.
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
