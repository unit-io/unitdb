package tracedb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/allegro/bigcache"
	"github.com/golang/snappy"
	"github.com/saffat-in/tracedb/collection"
	"github.com/saffat-in/tracedb/crypto"
	fltr "github.com/saffat-in/tracedb/filter"
	"github.com/saffat-in/tracedb/fs"
	"github.com/saffat-in/tracedb/hash"
	"github.com/saffat-in/tracedb/message"
	"github.com/saffat-in/tracedb/wal"
)

const (
	entriesPerBlock = 155
	loadFactor      = 0.7
	// MaxBlocks       = math.MaxUint32
	indexPostfix  = ".index"
	dataPostfix   = ".data"
	logPostfix    = ".log"
	lockPostfix   = ".lock"
	idSize        = 20
	filterPostfix = ".filter"
	version       = 1 // file format version

	// keyExpirationMaxDur expired keys are deleted from db after durType*keyExpirationMaxDur.
	// For example if durType is Minute and keyExpirationMaxDur then
	// all expired keys are deleted from db in 5 minutes
	keyExpirationMaxDur = 1

	// MaxKeyLength is the maximum size of a key in bytes.
	MaxTopicLength = 1 << 16

	// MaxValueLength is the maximum size of a value in bytes.
	MaxValueLength = 1 << 30

	// MaxKeys is the maximum numbers of keys in the DB.
	MaxKeys = math.MaxUint32

	// Maximum number of records to return
	maxResults = 100000
)

var bufPool = collection.NewBufferPool()

type (
	dbInfo struct {
		encryption   uint8
		seq          uint64
		count        uint32
		nBlocks      uint32
		blockIndex   uint32
		freeblockOff int64
		cacheID      uint64
		hashSeed     uint32
	}

	// DB represents the message storage for topic->keys-values.
	// All DB methods are safe for concurrent use by multiple goroutines.
	DB struct {
		// Need 64-bit alignment.
		mu         sync.RWMutex
		mac        *crypto.MAC
		writeLockC chan struct{}
		logLockC   chan struct{}
		// consistent   *hash.Consistent
		filter       Filter
		index        table
		data         dataTable
		lock         fs.LockFile
		wal          *wal.WAL
		metrics      Metrics
		cancelSyncer context.CancelFunc
		syncWrites   bool
		freeseq      freesequence
		dbInfo
		timeWindow timeWindowBucket

		//batchdb
		*batchdb
		//trie
		trie *message.Trie
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
		log.Fatal(err)
	}
	cacheID := uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
	db := &DB{
		index:      index,
		data:       dataTable{table: data},
		timeWindow: newTimeWindowBucket(time.Minute, keyExpirationMaxDur),
		filter:     Filter{table: filter, cache: cache, cacheID: cacheID, filterBlock: fltr.NewFilterGenerator()},
		lock:       lock,
		writeLockC: make(chan struct{}, 1),
		logLockC:   make(chan struct{}, 1),
		metrics:    newMetrics(),
		dbInfo: dbInfo{
			nBlocks:      1,
			freeblockOff: -1,
		},
		batchdb: &batchdb{},
		trie:    message.NewTrie(),
		// Close
		closeC: make(chan struct{}),
	}

	// Create a new MAC from the key.
	if db.mac, err = crypto.New(opts.EncryptionKey); err != nil {
		return nil, err
	}

	//initbatchdb
	if err = db.initbatchdb(); err != nil {
		return nil, err
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

	// db.consistent = hash.InitConsistent(int(MaxBlocks/blockSize), int(db.nBlocks))

	if needsRecovery {
		if err := db.recover(); err != nil {
			return nil, err
		}
	}

	logOpts := wal.Options{Path: path + logPostfix, TargetSize: opts.LogSize}
	wal, err := wal.New(logOpts)
	if err != nil {
		errors.New("Error creating WAL")
	}
	db.wal = wal

	// loadTrie loads topic into trie on opening an existing database file.
	db.loadTrie()

	db.logSyncer(opts.LogSyncInterval)

	if opts.BackgroundSyncInterval > 0 {
		db.startSyncer(opts.BackgroundSyncInterval)
	} else if opts.BackgroundSyncInterval == -1 {
		db.syncWrites = true
	}

	if opts.BackgroundKeyExpiry {
		db.startExpirer(time.Minute, keyExpirationMaxDur)
	}
	return db, nil
}

func blockOffset(idx uint32) int64 {
	return int64(headerSize) + (int64(blockSize) * int64(idx))
}

func (db *DB) logSyncer(interval time.Duration) {
	logsyncTicker := time.NewTicker(interval)
	go func() {
		for {
			select {
			default:
				if err := db.logSync(); err != nil {
					logger.Error().Err(err).Str("context", "logSyncer").Msg("Error committing write ahead log to db")
				}
				<-logsyncTicker.C
			case <-db.closeC:
				logsyncTicker.Stop()
				if err := db.wal.Close(); err != nil {
					logger.Error().Err(err).Str("context", "logSyncer").Msg("Error closing wal")
				}
				return
			}
		}
	}()
}

func (db *DB) startSyncer(interval time.Duration) {
	ctx, cancel := context.WithCancel(context.Background())
	db.cancelSyncer = cancel
	syncTicker := time.NewTicker(interval)
	go func() {
		var lastModifications int64
		for {
			select {
			case <-ctx.Done():
				syncTicker.Stop()
				return
			default:
				modifications := db.metrics.Puts.Value() + db.metrics.Dels.Value()
				if modifications != lastModifications {
					if err := db.Sync(); err != nil {
						logger.Error().Err(err).Str("context", "startSyncer").Msg("Error synchronizing database")
					}
					lastModifications = modifications
				}
				<-syncTicker.C
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
				db.expireOldEntries()
			case <-db.closeC:
				expirerTicker.Stop()
				return
			}
		}
	}()
}

func (db *DB) readEntry(seq uint64) (entry, error) {
	cacheKey := db.cacheID ^ seq
	e := entry{}
	if data, _ := db.mem.Get(cacheKey); data != nil {
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
	return entry{}, errIdDoesNotExist
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
	db.mu.Lock()
	defer db.mu.Unlock()

	if db.cancelSyncer != nil {
		db.cancelSyncer()
	}

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

	var err error
	// Signal all goroutines.
	close(db.closeC)

	if db.closer != nil {
		if err1 := db.closer.Close(); err == nil {
			err = err1
		}
		db.closer = nil
	}

	// Clear memdbs.
	db.clearMems()

	return err
}

func startBlockIndex(seq uint64) uint32 {
	return uint32(float64(seq-1) / float64(entriesPerBlock))
}

func (db *DB) hash(data []byte) uint32 {
	return hash.WithSalt(data, db.hashSeed)
}

// Get return items matching the query paramater
func (db *DB) Get(q *Query) (items [][]byte, err error) {
	// // CPU profiling by default
	// defer profile.Start().Stop()
	db.mu.RLock()
	defer db.mu.RUnlock()
	topic := new(message.Topic)
	if q.Contract == 0 {
		q.Contract = message.Contract
	}
	//Parse the Key
	topic.ParseKey(q.Topic)
	// Parse the topic
	topic.Parse(q.Contract, true)
	if topic.TopicType == message.TopicInvalid {
		return nil, errBadRequest
	}

	topic.AddContract(q.Contract)
	q.parts = topic.Parts

	// In case of ttl, include it to the query
	if from, until, limit, ok := topic.Last(); ok {
		q.prefix = message.GenPrefix(q.parts, until.Unix())
		q.cutoff = from.Unix()
		q.Limit = limit
		if q.Limit == 0 {
			q.Limit = maxResults // Maximum number of records to return
		}
	}
	q.seqs = db.trie.Lookup(q.parts)
	if len(q.seqs) == 0 {
		return
	}
	if len(q.seqs) > int(q.Limit) {
		q.seqs = q.seqs[:q.Limit]
	}
	for _, seq := range q.seqs {
		err = func() error {
			e, err := db.readEntry(seq)
			if err != nil {
				return err
			}
			if e.isExpired() {
				val, err := db.data.readTopic(e)
				if err != nil {
					return err
				}
				topic := new(message.Topic)
				topic.Unmarshal(val)
				db.trie.Remove(topic.Parts, seq)
				// free expired keys
				db.data.free(e.mSize(), e.mOffset)
				db.count--
				// if id is expired it does not return an error but continue the iteration
				return nil
			}
			id, val, err := db.data.readMessage(e)
			if err != nil {
				return err
			}
			_id := message.ID(id)
			if !_id.EvalPrefix(q.parts, q.cutoff) {
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
			return nil
			// 	}
			// }
			return nil
		}()
		if err != nil {
			return items, err
		}
		db.metrics.Gets.Add(1)
	}
	return items, nil
}

// Items returns a new ItemIterator.
func (db *DB) Items(q *Query) (*ItemIterator, error) {
	topic := new(message.Topic)
	if q.Contract == 0 {
		q.Contract = message.Contract
	}
	//Parse the Key
	topic.ParseKey(q.Topic)
	// Parse the topic
	topic.Parse(q.Contract, true)
	if topic.TopicType == message.TopicInvalid {
		return nil, errBadRequest
	}

	topic.AddContract(q.Contract)
	q.parts = topic.Parts

	// In case of ttl, include it to the query
	if from, until, limit, ok := topic.Last(); ok {
		q.prefix = message.GenPrefix(q.parts, until.Unix())
		q.cutoff = from.Unix()
		q.Limit = limit
		if q.Limit == 0 {
			q.Limit = maxResults // Maximum number of records to return
		}
	}

	return &ItemIterator{db: db, query: q}, nil
}

func (db *DB) sync() error {
	// writeHeader information to persist correct seq information to disk
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
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.sync()
}

func (db *DB) expireOldEntries() {
	expiredEntries := db.timeWindow.expireOldEntries()
	for _, expiredEntry := range expiredEntries {
		entry := expiredEntry.(entry)
		/// Test filter block if message hash presence
		if !db.filter.Test(entry.seq) {
			continue
		}
		db.metrics.Dels.Add(1)
		db.mu.Lock()
		defer db.mu.Unlock()
		e, err := db.readEntry(entry.seq)
		if err != nil {
			continue
		}
		etopic, err := db.data.readTopic(e)
		if err != nil {
			continue
		}
		topic := new(message.Topic)
		topic.Unmarshal(etopic)
		if ok := db.trie.Remove(topic.Parts, entry.seq); ok {
			db.data.free(e.mSize(), e.mOffset)
			db.count--
		}

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
		db.trie.Add(it.Topic().Parts(), it.Topic().Depth(), it.Topic().Seq())
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

// PutEntry sets the entry for the given message. It updates the value for the existing message id.
func (db *DB) PutEntry(e *Entry) error {
	// start := time.Now()
	// defer log.Printf("db.Put %d", time.Since(start).Nanoseconds())
	db.mu.Lock()
	defer db.mu.Unlock()
	topic := new(message.Topic)
	if e.Contract == 0 {
		e.Contract = message.Contract
	}
	//Parse the Key
	topic.ParseKey(e.Topic)
	e.Topic = topic.Topic
	// Parse the topic
	topic.Parse(e.Contract, true)
	if topic.TopicType == message.TopicInvalid {
		return errBadRequest
	}
	// In case of ttl, add ttl to the msg and store to the db
	if ttl, ok := topic.TTL(); ok {
		//1410065408 10 sec
		e.ExpiresAt = uint32(time.Now().Add(time.Duration(ttl)).Unix())
	}
	topic.AddContract(e.Contract)
	//message ID is the database key
	var id message.ID
	var ok bool
	var seq uint64
	if e.ID != nil {
		id = message.ID(e.ID)
		id.AddContract(topic.Parts)
		seq = id.Seq()
	} else {
		ok, seq = db.freeseq.get()
		if !ok {
			seq = db.nextSeq()
		}
		id = message.NewID(seq, false)
		id.AddContract(topic.Parts)
	}
	m, err := e.Marshal()
	if err != nil {
		return err
	}
	val := snappy.Encode(nil, m)
	switch {
	case len(topic.Topic) > MaxTopicLength:
		return errIdTooLarge
	case len(val) > MaxValueLength:
		return errValueTooLarge
	}
	logWriter, err := db.wal.NewWriter()
	if err != nil {
		return err
	}
	data, err := db.entryData(seq, id, topic.Marshal(), val, e.ExpiresAt)
	if err != nil {
		return err
	}
	memseq := db.cacheID ^ seq
	if err := db.mem.Set(memseq, data); err != nil {
		return err
	}
	if ok := db.trie.Add(topic.Parts, topic.Depth, seq); ok {
		if err != nil {
			return err
		}
		if err := <-logWriter.Append(data); err != nil {
			return err
		}
	}

	return <-logWriter.SignalInitWrite()
}

// entryData marshal entry along with message data
func (db *DB) entryData(seq uint64, id, topic, value []byte, expiresAt uint32) ([]byte, error) {
	if db.count == MaxKeys {
		return nil, errFull
	}

	e := entry{
		seq:       seq,
		topicSize: uint16(len(topic)),
		valueSize: uint32(len(value)),
		// mOffset:   offset,
		expiresAt: expiresAt,
	}
	data, _ := e.MarshalBinary()
	mLen := idSize + len(topic) + len(value)
	m := make([]byte, mLen)
	copy(m, id)
	copy(m[idSize:], topic)
	copy(m[len(topic)+idSize:], value)
	data = append(data, m...)

	return data, nil
}

// tinyCommit commits tinyBatch with size less than entriesPerBlock
func (db *DB) tinyCommit(entryCount uint16, tinyBatchData []byte) <-chan error {
	// commit writes batches into write ahead log. The write happen synchronously.
	db.logLockC <- struct{}{}
	defer func() {
		<-db.logLockC
	}()
	db.closeW.Add(1)
	defer db.closeW.Done()

	done := make(chan error, 1)
	logWriter, err := db.wal.NewWriter()
	if err != nil {
		done <- err
		return done
	}

	offset := uint32(0)
	for i := uint16(0); i < entryCount; i++ {
		dataLen := binary.LittleEndian.Uint32(tinyBatchData[offset : offset+4])
		if err := <-logWriter.Append(tinyBatchData[offset+4 : offset+dataLen]); err != nil {
			done <- err
			return done
		}
		offset += dataLen
	}

	if err := db.writeHeader(false); err != nil {
		done <- err
		return done
	}
	return logWriter.SignalInitWrite()
}

func (db *DB) commit(batchSeqs []uint64) <-chan error {
	// // CPU profiling by default
	// defer profile.Start().Stop()
	// commit writes batches into write ahead log. The write happen synchronously.
	db.logLockC <- struct{}{}
	defer func() {
		<-db.logLockC
	}()
	db.closeW.Add(1)
	defer db.closeW.Done()

	done := make(chan error, 1)
	logWriter, err := db.wal.NewWriter()
	if err != nil {
		done <- err
		return done
	}

	for _, seq := range batchSeqs {
		db.metrics.Puts.Add(1)
		memdata, err := db.mem.Get(seq)
		if err != nil {
			done <- err
			return done
		}
		e := entry{}
		if err = e.UnmarshalBinary(memdata[:entrySize]); err != nil {
			done <- err
			return done
		}

		if err := <-logWriter.Append(memdata); err != nil {
			done <- err
			return done
		}
	}

	if err := db.writeHeader(false); err != nil {
		done <- err
		return done
	}
	return logWriter.SignalInitWrite()
}

func (db *DB) logSync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.closeW.Add(1)
	defer db.closeW.Done()
	seqs, err := db.wal.Scan()
	if err != nil {
		return err
	}
	for _, s := range seqs {
		it, err := db.wal.Read(s)
		if err != nil {
			return err
		}
		for {
			logData, ok := it.Next()
			if !ok {
				break
			}
			entryData, data := logData[:entrySize], logData[entrySize:]
			e := entry{}
			e.UnmarshalBinary(entryData)
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
			moffset := e.mSize()
			m := data[:moffset]
			if e.mOffset, err = db.data.writeRaw(m); err != nil {
				db.freeseq.free(e.seq)
				return err
			}

			b.entries[b.entryIdx] = e
			if b.entries[b.entryIdx].expiresAt > 0 {
				db.timeWindow.add(b.entries[b.entryIdx])
			}
			b.entryIdx++
			if err := b.write(); err != nil {
				db.freeseq.free(e.seq)
				return err
			}
			db.filter.Append(e.seq)
		}
		if err := db.sync(); err != nil {
			return err
		}
		if err := db.wal.SignalLogApplied(s); err != nil {
			return err
		}
	}

	return nil
}

// DeleteEntry delets an entry from database. you must provide an ID to delete message.
// It is safe to modify the contents of the argument after Delete returns but
// not before.
func (db *DB) DeleteEntry(e *Entry) error {
	if e.ID == nil {
		return errIdEmpty
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	topic := new(message.Topic)
	if e.Contract == 0 {
		e.Contract = message.Contract
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
	id.AddContract(topic.Parts)

	if ok := db.trie.Remove(topic.Parts, id.Seq()); ok {
		err := db.delete(id)
		if err != nil {
			return err
		}
	}
	return nil
}

// delete deletes the given key from the DB.
func (db *DB) delete(id []byte) error {
	seq := message.ID(id).Seq()
	/// Test filter block for the message id presence
	if !db.filter.Test(seq) {
		return nil
	}
	db.metrics.Dels.Add(1)
	db.freeseq.free(seq)
	startBlockIdx := startBlockIndex(seq)
	off := blockOffset(startBlockIdx)
	b := &blockHandle{table: db.index, offset: off}
	if err := b.read(); err != nil {
		db.freeseq.free(seq)
		return err
	}
	entryIdx := -1
	for i := 0; i < entriesPerBlock; i++ {
		e := b.entries[i]
		if seq == e.seq {
			_id, err := db.data.readId(e)
			if err != nil {
				return err
			}
			if bytes.Equal(id, _id) {
				entryIdx = i
				break
			}
		}
	}
	if entryIdx == -1 {
		return nil // no entry in db to delete
	}

	e := b.entries[entryIdx]
	b.del(entryIdx)
	b.entryIdx--
	if err := b.write(); err != nil {
		return err
	}
	db.data.free(e.mSize(), e.mOffset)
	db.count--
	if db.syncWrites {
		return db.sync()
	}
	return nil
}

// Count returns the number of items in the DB.
func (db *DB) Count() uint32 {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return db.count
}

// Metrics returns the DB metrics.
func (db *DB) Metrics() Metrics {
	return db.metrics
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
