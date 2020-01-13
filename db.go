package tracedb

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/allegro/bigcache"
	"github.com/golang/snappy"
	"github.com/unit-io/tracedb/collection"
	"github.com/unit-io/tracedb/crypto"
	fltr "github.com/unit-io/tracedb/filter"
	"github.com/unit-io/tracedb/fs"
	"github.com/unit-io/tracedb/hash"
	"github.com/unit-io/tracedb/message"
	"github.com/unit-io/tracedb/wal"
)

const (
	entriesPerBlock = 150
	loadFactor      = 0.7
	// MaxBlocks       = math.MaxUint32
	nShards       = 16 // TODO implelemt sharding based on total Contracts in db
	indexPostfix  = ".index"
	dataPostfix   = ".data"
	logPostfix    = ".log"
	lockPostfix   = ".lock"
	idSize        = 24
	filterPostfix = ".filter"
	version       = 1 // file format version

	// keyExpirationMaxDur expired keys are deleted from db after durType*keyExpirationMaxDur.
	// For example if durType is Minute and keyExpirationMaxDur then
	// all expired keys are deleted from db in 5 minutes
	keyExpirationMaxDur = 1

	// MaxTopicLength is the maximum size of a topic in bytes.
	MaxTopicLength = 1 << 16

	// MaxValueLength is the maximum size of a value in bytes.
	MaxValueLength = 1 << 30

	// MaxKeys is the maximum numbers of keys in the DB.
	MaxKeys = math.MaxUint64

	// Maximum number of records to return
	maxResults = 100000
)

var bufPool = collection.NewBufferPool()

type (
	dbInfo struct {
		encryption   uint8
		seq          uint64
		count        uint64
		nBlocks      uint32
		blockIndex   uint32
		freeblockOff int64
		cacheID      uint64
		hashSeed     uint32
	}

	log struct {
		contract uint64
		seq      uint64
	}

	// DB represents the message storage for topic->keys-values.
	// All DB methods are safe for concurrent use by multiple goroutines.
	DB struct {
		// Need 64-bit alignment.
		mu sync.RWMutex
		mutex
		mac            *crypto.MAC
		writeLockC     chan struct{}
		commitLockC    chan struct{}
		syncLockC      chan struct{}
		commitLogQueue sync.Map
		expiryLockC    chan struct{}
		// consistent     *hash.Consistent
		filter     Filter
		index      table
		data       dataTable
		lock       fs.LockFile
		wal        *wal.WAL
		syncWrites bool
		freeslots  freeslots
		dbInfo
		timeWindow timeWindowBucket

		//batchdb
		*batchdb
		//trie
		trie *message.Trie
		// The db start time
		start time.Time
		// The metircs to measure timeseries on message events
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
	// fileFlag := os.O_CREATE | os.O_RDWR
	fs := opts.FileSystem
	fileMode := os.FileMode(0666)
	lock, needsRecovery, err := fs.CreateLockFile(path+lockPostfix, fileMode)
	if err != nil {
		if err == os.ErrExist {
			err = errLocked
		}
		return nil, err
	}

	index, err := newTable(fs, path+indexPostfix)
	if err != nil {
		return nil, err
	}
	data, err := newTable(fs, path+dataPostfix)
	if err != nil {
		return nil, err
	}
	filter, err := newTable(fs, path+filterPostfix)
	if err != nil {
		return nil, err
	}
	cache, err := bigcache.NewBigCache(config)
	if err != nil {
		return nil, err
	}
	cacheID := uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
	db := &DB{
		mutex:       newMutex(),
		index:       index,
		data:        dataTable{table: data, fb: newFreeBlocks(opts.MinimumFreeBlocksSize)},
		timeWindow:  newTimeWindowBucket(time.Minute, keyExpirationMaxDur),
		filter:      Filter{table: filter, cache: cache, cacheID: cacheID, filterBlock: fltr.NewFilterGenerator()},
		lock:        lock,
		writeLockC:  make(chan struct{}, 1),
		commitLockC: make(chan struct{}, 1),
		syncLockC:   make(chan struct{}, 1),
		expiryLockC: make(chan struct{}, 1),
		freeslots:   newFreeSlots(),
		dbInfo: dbInfo{
			nBlocks:      1,
			freeblockOff: -1,
		},
		batchdb: &batchdb{},
		trie:    message.NewTrie(),
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
		// memcache
		db.cacheID = uint64(rand.Uint32())<<32 + uint64(rand.Uint32())

		seed, err := hash.RandSeed()
		if err != nil {
			return nil, err
		}
		db.hashSeed = seed
		if _, err = db.index.extend(headerSize + blockSize); err != nil {
			return nil, err
		}
		if _, err = db.data.extend(headerSize); err != nil {
			return nil, err
		}
		if err := db.writeHeader(false); err != nil {
			return nil, err
		}
	} else {
		if err := db.readHeader(!needsRecovery); err != nil {
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

	if needsRecovery {
		if err := db.recover(); err != nil {
			return nil, err
		}
	}

	logOpts := wal.Options{Path: path + logPostfix, TargetSize: opts.LogSize}
	wal, needLogRecovery, err := wal.New(logOpts)
	if err != nil {
		wal.Close()
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

	// Create a new MAC from the key.
	if db.mac, err = crypto.New(opts.EncryptionKey); err != nil {
		return nil, err
	}

	// set ecnryption flag to encrypt messages
	db.encryption = 0
	if opts.Encryption {
		db.encryption = 1
	}

	//initbatchdb
	if err = db.initbatchdb(opts); err != nil {
		return nil, err
	}

	// loadTrie loads topic into trie on opening an existing database file.
	db.loadTrie()

	db.startSyncer(opts.BackgroundSyncInterval)

	if opts.BackgroundKeyExpiry {
		db.startExpirer(time.Minute, keyExpirationMaxDur)
	}
	return db, nil
}

func blockOffset(idx uint32) int64 {
	return int64(headerSize) + (int64(blockSize) * int64(idx))
}

func (db *DB) startSyncer(interval time.Duration) {
	logsyncTicker := time.NewTicker(interval)
	go func() {
		defer func() {
			logsyncTicker.Stop()
		}()
		for {
			select {
			case <-db.closeC:
				return
			case <-logsyncTicker.C:
				if err := db.Sync(); err != nil {
					logger.Error().Err(err).Str("context", "startSyncer").Msg("Error syncing to db")
				}
			}
		}
	}()
}

func (db *DB) startExpirer(durType time.Duration, maxDur int) {
	expirerTicker := time.NewTicker(durType * time.Duration(maxDur))
	go func() {
		for {
			select {
			case <-expirerTicker.C:
				db.ExpireOldEntries()
			case <-db.closeC:
				expirerTicker.Stop()
				return
			}
		}
	}()
}

func (db *DB) writeHeader(writeFreeList bool) error {
	if writeFreeList {
		db.data.fb.defrag()
		freeblockOff, err := db.data.fb.write(db.data.table)
		if err != nil {
			return err
		}
		db.dbInfo.freeblockOff = freeblockOff
	}
	h := header{
		signature: signature,
		version:   version,
		dbInfo:    db.dbInfo,
	}
	return db.index.writeMarshalableAt(h, 0)
}

func (db *DB) readHeader(readFreeList bool) error {
	h := &header{}
	if err := db.index.readUnmarshalableAt(h, headerSize, 0); err != nil {
		return err
	}
	// if !bytes.Equal(h.signature[:], signature[:]) {
	// 	return errCorrupted
	// }
	db.dbInfo = h.dbInfo
	if readFreeList {
		if err := db.data.fb.read(db.data.table, db.dbInfo.freeblockOff); err != nil {
			return err
		}
	}
	db.dbInfo.freeblockOff = -1
	return nil
}

// Close closes the DB.
func (db *DB) Close() error {
	if !db.setClosed() {
		return errClosed
	}

	// Signal all goroutines.
	close(db.closeC)

	// Acquire writer lock.
	db.writeLockC <- struct{}{}
	db.commitLockC <- struct{}{}
	db.syncLockC <- struct{}{}

	// Wait for all gorotines to exit.
	db.closeW.Wait()

	if err := db.writeHeader(true); err != nil {
		return err
	}
	if err := db.data.Close(); err != nil {
		return err
	}
	if err := db.index.Close(); err != nil {
		return err
	}
	if err := db.lock.Unlock(); err != nil {
		return err
	}
	if err := db.filter.close(); err != nil {
		return err
	}

	// Clear memdbs.
	db.clearMems()

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

func startBlockIndex(seq uint64) uint32 {
	return uint32(float64(seq-1) / float64(entriesPerBlock))
}

func (db *DB) hash(data []byte) uint32 {
	return hash.WithSalt(data, db.hashSeed)
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
	b := blockHandle{table: db.index.FileManager, offset: off}
	if err := b.read(); err != nil {
		return entry{}, err
	}

	for i := 0; i < entriesPerBlock; i++ {
		e := b.entries[i]
		if e.seq == seq {
			return e, nil
		}
	}
	return entry{}, errMsgIdDoesNotExist
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
	topic.Parse(q.Contract, true)
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
	if from, until, limit, ok := topic.Last(); ok {
		q.prefix = message.GenPrefix(q.contract, until.Unix())
		q.cutoff = from.Unix()
		q.Limit = limit
		if q.Limit == 0 {
			q.Limit = maxResults // Maximum number of records to return
		}
	}

	mu := db.getMutex(q.contract)
	mu.RLock()
	defer mu.RUnlock()
	q.seqs = db.trie.Lookup(q.contract, q.parts)
	if len(q.seqs) == 0 {
		return
	}
	if len(q.seqs) > int(q.Limit) {
		q.seqs = q.seqs[:q.Limit]
	}

	for _, seq := range q.seqs {
		err = func() error {
			e, err := db.readEntry(q.contract, seq)
			if err != nil {
				return err
			}
			if e.isExpired() {
				if ok := db.trie.Remove(q.contract, q.parts, seq); ok {
					db.timeWindow.add(e)
				}
				// if id is expired it does not return an error but continue the iteration
				return nil
			}
			id, val, err := db.data.readMessage(e)
			if err != nil {
				return err
			}
			_id := message.ID(id)
			if !_id.EvalPrefix(q.contract, q.cutoff) {
				return nil
			}

			if _id.IsEncrypted() {
				val, err = db.mac.Decrypt(nil, val)
				if err != nil {
					return err
				}
			}
			var entry Entry
			var buffer []byte
			val, err = snappy.Decode(buffer, val)
			if err != nil {
				return err
			}
			err = entry.Unmarshal(val)
			if err != nil {
				return err
			}
			items = append(items, entry.Payload)
			db.meter.OutBytes.Inc(int64(e.valueSize))
			return nil
		}()
		if err != nil {
			return items, err
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
	topic := new(message.Topic)
	if q.Contract == 0 {
		q.Contract = message.MasterContract
	}
	//Parse the Key
	topic.ParseKey(q.Topic)
	// Parse the topic
	topic.Parse(q.Contract, true)
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
	if from, until, limit, ok := topic.Last(); ok {
		q.prefix = message.GenPrefix(q.contract, until.Unix())
		q.cutoff = from.Unix()
		q.Limit = limit
		if q.Limit == 0 {
			q.Limit = maxResults // Maximum number of records to return
		}
	}

	return &ItemIterator{db: db, query: q}, nil
}

func (db *DB) sync() error {
	// writeHeader information to persist correct seq information to disk, also sync freeblocks to disk
	if err := db.writeHeader(false); err != nil {
		return err
	}
	if err := db.data.Sync(); err != nil {
		return err
	}
	if err := db.index.Sync(); err != nil {
		return err
	}
	return nil
}

// Sync commits the contents of the database to the backing FileSystem; this is effectively a noop for an in-memory database. It must only be called while the database is opened.
func (db *DB) Sync() error {
	// write to db happens synchronously
	db.syncLockC <- struct{}{}
	db.closeW.Add(1)
	defer func() {
		<-db.syncLockC
		db.closeW.Done()
	}()

	var needRecovery bool
	seqs, err := db.wal.Scan()
	if err != nil {
		return err
	}
	if len(seqs) == 0 {
		return nil
	}
	if err := db.extendBlocks(); err != nil {
		return err
	}
	for _, s := range seqs {
		err := func() error {
			qlogs, ok := db.commitLogQueue.Load(s)
			if !ok {
				return nil
			}
			logs := qlogs.([]log)
			for _, log := range logs {
				memdata, err := db.mem.Get(log.contract, log.seq)
				if err != nil {
					return err
				}
				e := entry{}
				if err = e.UnmarshalBinary(memdata[:entrySize]); err != nil {
					return err
				}
				startBlockIdx := startBlockIndex(e.seq)
				off := blockOffset(startBlockIdx)
				b := &blockHandle{table: db.index, offset: off}
				if err := b.read(); err != nil {
					return err
				}
				entryIdx := 0
				for i := 0; i < entriesPerBlock; i++ {
					ie := b.entries[i]
					if ie.seq == e.seq { //record exist in db
						entryIdx = -1
						break
					}
				}
				if entryIdx == -1 {
					continue
				}
				db.count++
				if e.mOffset, err = db.data.writeRaw(memdata[entrySize:]); err != nil {
					return err
				}
				db.meter.InBytes.Inc(int64(e.valueSize))
				b.entries[b.entryIdx] = e
				if b.entries[b.entryIdx].expiresAt > 0 {
					db.timeWindow.add(b.entries[b.entryIdx])
				}
				b.entryIdx++
				if err := b.write(); err != nil {
					return err
				}

				db.filter.Append(e.seq)
			}
			db.meter.Puts.Inc(int64(len(logs)))
			db.mem.Free(logs[0].contract, logs[0].seq)
			return db.sync()
		}()
		db.commitLogQueue.Delete(s)
		if err == nil {
			if err := db.wal.SignalLogApplied(s); err != nil {
				return err
			}
		} else {
			needRecovery = true
		}
	}

	if needRecovery {
		// run db recovery if an error occur with the db sync
		if err := db.recoverLog(); err != nil {
			// if unable to recover db then close db
			panic(fmt.Sprintf("Unable to recover db on sync error %v. Closing db...", err))
		}
	}
	return nil
}

// ExpireOldEntries run expirer to delete entries from db if ttl was set on entries and it has expired
func (db *DB) ExpireOldEntries() {
	// expiry happens synchronously
	db.expiryLockC <- struct{}{}
	defer func() {
		<-db.expiryLockC
	}()
	expiredEntries := db.timeWindow.expireOldEntries()
	// fmt.Println("db.ExpireOldEntries: expiry count ", len(expiredEntries))
	for _, expiredEntry := range expiredEntries {
		entry := expiredEntry.(entry)
		/// Test filter block if message hash presence
		if !db.filter.Test(entry.seq) {
			continue
		}
		etopic, err := db.data.readTopic(entry)
		if err != nil {
			continue
		}
		topic := new(message.Topic)
		topic.Unmarshal(etopic)
		contract := message.Contract(topic.Parts)
		mu := db.getMutex(contract)
		mu.Lock()
		mu.Unlock()
		db.delete(entry.seq)
		db.count--
	}
	if db.syncWrites {
		db.sync()
	}
}

// loadTrie loads topics to the trie from data file
func (db *DB) loadTrie() error {
	it := &TopicIterator{db: db}
	for it.First(); it.Valid(); it.Next() {
		err := it.Error()
		if err != nil {
			logger.Error().Err(err).Str("context", "db.loadTrie")
			return err
		}
		db.trie.Add(message.Contract(it.Topic().Parts()), it.Topic().Parts(), it.Topic().Depth(), it.Topic().Seq())
	}
	return nil
}

// NewContract generates a new Contract.
func (db *DB) NewContract() (uint32, error) {
	raw := make([]byte, 4)
	rand.Read(raw)

	contract := uint32(binary.BigEndian.Uint32(raw[:4]))
	return contract, nil
}

// NewID generates new ID that is later used client program to EntryPut or EntryDelete.
func (db *DB) NewID() []byte {
	return message.NewID(db.nextSeq(), false)
}

// newBlock adds new block to db table and return block offset
func (db *DB) newBlock() (int64, error) {
	off, err := db.index.extend(blockSize)
	db.nBlocks++
	db.blockIndex++
	return off, err
}

// extendBlocks adds new blocks to db table for the batch write
func (db *DB) extendBlocks() error {
	nBlocks := uint32(float64(db.seq-1) / float64(entriesPerBlock))
	for nBlocks > db.blockIndex {
		if _, err := db.newBlock(); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) parseTopic(e *Entry) (*message.Topic, error) {
	topic := new(message.Topic)
	if e.Contract == 0 {
		e.Contract = message.MasterContract
	}
	//Parse the Key
	topic.ParseKey(e.Topic)
	e.Topic = topic.Topic
	// Parse the topic
	topic.Parse(e.Contract, true)
	if topic.TopicType == message.TopicInvalid {
		return nil, errBadRequest
	}
	// In case of ttl, add ttl to the msg and store to the db
	if ttl, ok := topic.TTL(); ok {
		//1410065408 10 sec
		e.ExpiresAt = uint32(time.Now().Add(time.Duration(ttl)).Unix())
	}
	topic.AddContract(e.Contract)
	return topic, nil
}

func (db *DB) setEntry(e *Entry) error {
	//message ID is the database key
	var id message.ID
	var ok bool
	var seq uint64
	if e.ID != nil {
		id = message.ID(e.ID)
		id.AddContract(e.contract)
		seq = id.Seq()
	} else {
		ok, seq = db.freeslots.get(e.contract)
		if !ok {
			seq = db.nextSeq()
		}
		id = message.NewID(seq, db.encryption == 1)
		id.AddContract(e.contract)
	}
	m, err := e.Marshal()
	if err != nil {
		return err
	}
	val := snappy.Encode(nil, m)
	e.id = id
	e.seq = seq
	e.val = val
	return nil
}

// Put sets the entry for the given message. It uses default Contract to put entry into db.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (db *DB) Put(topic, value []byte) error {
	return db.PutEntry(NewEntry(topic, value))
}

// PutEntry sets the entry for the given message.
// It is safe to modify the contents of the argument after PutEntry returns but not
// before.
func (db *DB) PutEntry(e *Entry) error {
	// start := time.Now()
	// defer log.Printf("db.Put %d", time.Since(start).Nanoseconds())
	// The write happen synchronously.
	db.writeLockC <- struct{}{}
	defer func() {
		<-db.writeLockC
	}()
	if err := db.ok(); err != nil {
		return err
	}
	switch {
	case len(e.Topic) == 0:
		return errTopicEmpty
	case len(e.Topic) > MaxTopicLength:
		return errTopicTooLarge
	case len(e.Payload) > MaxValueLength:
		return errValueTooLarge
	}
	topic, err := db.parseTopic(e)
	if err != nil {
		return err
	}
	e.topic = topic.Marshal()
	e.contract = message.Contract(topic.Parts)
	db.setEntry(e)
	// Encryption.
	if db.encryption == 1 {
		e.val = db.mac.Encrypt(nil, e.val)
	}
	data, err := db.packEntry(e)
	if err != nil {
		return err
	}
	memseq := db.cacheID ^ e.seq
	if err := db.mem.Set(e.contract, memseq, data); err != nil {
		return err
	}
	if ok := db.trie.Add(e.contract, topic.Parts, topic.Depth, e.seq); ok {
		if err != nil {
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
		db.tinyBatch.logs = append(db.tinyBatch.logs, log{contract: e.contract, seq: memseq})
		db.tinyBatch.entryCount++
	}

	return nil
}

// entryData marshal entry along with message data
func (db *DB) packEntry(e *Entry) ([]byte, error) {
	if db.count == MaxKeys {
		return nil, errFull
	}

	e1 := entry{
		seq:       e.seq,
		topicSize: uint16(len(e.topic)),
		valueSize: uint32(len(e.val)),
		expiresAt: e.ExpiresAt,
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

// tinyCommit commits tinyBatch with size less than entriesPerBlock
func (db *DB) tinyCommit(entryCount uint16, logs []log, tinyBatchData []byte) error {
	if err := db.ok(); err != nil {
		return err
	}
	// commit writes batches into write ahead log. The write happen synchronously.
	db.writeLockC <- struct{}{}
	db.closeW.Add(1)
	defer func() {
		db.closeW.Done()
		<-db.writeLockC
	}()

	logWriter, err := db.wal.NewWriter()
	if err != nil {
		return err
	}

	offset := uint32(0)
	for i := uint16(0); i < entryCount; i++ {
		dataLen := binary.LittleEndian.Uint32(tinyBatchData[offset : offset+4])
		if err := <-logWriter.Append(tinyBatchData[offset+4 : offset+dataLen]); err != nil {
			return err
		}
		offset += dataLen
	}

	db.meter.InMsgs.Inc(int64(entryCount))
	logSeq := db.wal.NextSeq()
	if err := <-logWriter.SignalInitWrite(logSeq); err != nil {
		return err
	}
	if err := db.writeHeader(false); err != nil {
		return err
	}
	db.commitLogQueue.Store(logSeq, logs)
	return db.tinyBatch.reset()
}

func (db *DB) commit(logs []log) error {
	// // CPU profiling by default
	// defer profile.Start().Stop()
	if err := db.ok(); err != nil {
		return err
	}

	// commit writes batches into write ahead log. The write happen synchronously.
	db.commitLockC <- struct{}{}
	db.closeW.Add(1)
	defer func() {
		db.closeW.Done()
		<-db.commitLockC
	}()

	logWriter, err := db.wal.NewWriter()
	if err != nil {
		return err
	}

	for _, log := range logs {
		memdata, err := db.mem.Get(log.contract, log.seq)
		if err != nil {
			return err
		}
		e := entry{}
		if err = e.UnmarshalBinary(memdata[:entrySize]); err != nil {
			return err
		}

		if err := <-logWriter.Append(memdata); err != nil {
			return err
		}
	}

	db.meter.InMsgs.Inc(int64(len(logs)))
	logSeq := db.wal.NextSeq()
	if err := <-logWriter.SignalInitWrite(logSeq); err != nil {
		return err
	}
	if err := db.writeHeader(false); err != nil {
		return err
	}
	db.commitLogQueue.Store(logSeq, logs)
	return db.tinyBatch.reset()
}

// Put sets the entry for the given message. It uses default Contract to put entry into db.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (db *DB) Delete(id, topic []byte) error {
	return db.DeleteEntry(&Entry{ID: id, Topic: topic})
}

// DeleteEntry delets an entry from database. you must provide an ID to delete message.
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
	// db.mu.Lock()
	// defer db.mu.Unlock()
	topic := new(message.Topic)
	if e.Contract == 0 {
		e.Contract = message.MasterContract
	}
	//Parse the Key
	topic.ParseKey(e.Topic)
	e.Topic = topic.Topic
	// Parse the topic
	topic.Parse(e.Contract, true)
	if topic.TopicType == message.TopicInvalid {
		return errBadRequest
	}

	topic.AddContract(e.Contract)
	// message ID is the database key
	id := message.ID(e.ID)
	contract := message.Contract(topic.Parts)
	id.AddContract(contract)
	mu := db.getMutex(contract)
	mu.Lock()
	defer mu.Unlock()
	if ok := db.trie.Remove(contract, topic.Parts, id.Seq()); ok {
		err := db.delete(message.ID(id).Seq())
		if err != nil {
			return err
		}
	}
	return nil
}

// delete deletes the given key from the DB.
func (db *DB) delete(seq uint64) error {
	// seq := message.ID(id).Seq()
	/// Test filter block for the message id presence
	if !db.filter.Test(seq) {
		return nil
	}
	db.meter.Dels.Inc(1)
	startBlockIdx := startBlockIndex(seq)
	off := blockOffset(startBlockIdx)
	b := &blockHandle{table: db.index, offset: off}
	if err := b.read(); err != nil {
		return err
	}
	entryIdx := -1
	for i := 0; i < entriesPerBlock; i++ {
		e := b.entries[i]
		if seq == e.seq {
			entryIdx = i
			break
		}
	}
	if entryIdx == -1 {
		return nil // no entry in db to delete
	}

	// e := b.entries[entryIdx]
	b.del(entryIdx)
	b.entryIdx--
	if err := b.write(); err != nil {
		return err
	}
	// db.freeslots.free(e.seq)
	// db.data.free(e.mSize(), e.mOffset)
	db.count--
	if db.syncWrites {
		return db.sync()
	}
	return nil
}

// Count returns the number of items in the DB.
func (db *DB) Count() uint64 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.count
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

func (db *DB) getSeq() uint64 {
	return atomic.LoadUint64(&db.seq)
}

func (db *DB) nextSeq() uint64 {
	return atomic.AddUint64(&db.seq, 1)
}
