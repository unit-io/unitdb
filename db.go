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
	"github.com/unit-io/tracedb/crypto"
	fltr "github.com/unit-io/tracedb/filter"
	"github.com/unit-io/tracedb/fs"
	"github.com/unit-io/tracedb/hash"
	"github.com/unit-io/tracedb/message"
	"github.com/unit-io/tracedb/wal"
)

const (
	entriesPerBlock    = 150 // (4096 i.e blocksize/26 i.e entry slot size)
	seqsPerWindowBlock = 500 // ((4096 i.e. blocksize - 26 fixed)/8 i.e. timeentry size)
	// MaxBlocks       = math.MaxUint32
	nShards       = 16
	indexPostfix  = ".index"
	dataPostfix   = ".data"
	windowPostfix = ".summary"
	logPostfix    = ".log"
	lockPostfix   = ".lock"
	idSize        = 24
	filterPostfix = ".filter"
	version       = 1 // file format version

	// maxExpDur expired keys are deleted from db after durType*maxExpDur.
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
	maxResults = 1000000
)

type (
	dbInfo struct {
		encryption    uint8
		seq           uint64
		count         int64
		nBlocks       uint32
		blockIndex    uint32
		timeWindowIdx int32
		freeblockOff  int64
		cacheID       uint64
		hashSeed      uint32
	}

	// DB represents the message storage for topic->keys-values.
	// All DB methods are safe for concurrent use by multiple goroutines.
	DB struct {
		// Need 64-bit alignment.
		mu sync.RWMutex
		mutex
		mac         *crypto.MAC
		writeLockC  chan struct{}
		commitLockC chan struct{}
		syncLockC   chan struct{}
		expiryLockC chan struct{}
		// consistent     *hash.Consistent
		filter     Filter
		index      file
		data       dataTable
		lock       fs.LockFile
		wal        *wal.WAL
		syncWrites bool
		freeslots  freeslots
		dbInfo
		timeWindow *timeWindowBucket

		//batchdb
		*batchdb
		//trie
		trie *trie
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

	index, err := newFile(fs, path+indexPostfix)
	if err != nil {
		return nil, err
	}
	data, err := newFile(fs, path+dataPostfix)
	if err != nil {
		return nil, err
	}
	timeOptions := &timeOptions{expDurationType: time.Minute, maxExpDurations: maxExpDur}
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
		index:       index,
		data:        dataTable{file: data, fb: newFreeBlocks(opts.MinimumFreeBlocksSize)},
		timeWindow:  newTimeWindowBucket(timewindow, timeOptions),
		filter:      Filter{file: filter, filterBlock: fltr.NewFilterGenerator()},
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
		trie:    newTrie(uint32(opts.CacheCap)),
		start:   time.Now(),
		meter:   NewMeter(),
		// Close
		closeC: make(chan struct{}),
	}

	db.filter.cache = db.mem
	db.filter.cacheID = db.cacheID

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

	if err := db.loadTopicHash(); err != nil {
		return nil, err
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
	if err := db.loadTrie(); err != nil {
		return nil, err
	}

	db.startSyncer(opts.BackgroundSyncInterval)

	if opts.BackgroundKeyExpiry {
		db.startExpirer(time.Minute, maxExpDur)
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
		freeblockOff, err := db.data.fb.write(db.data.file)
		if err != nil {
			return err
		}
		db.dbInfo.freeblockOff = freeblockOff
	}
	h := header{
		signature: signature,
		version:   version,
		dbInfo: dbInfo{
			encryption:    db.encryption,
			seq:           atomic.LoadUint64(&db.seq),
			count:         atomic.LoadInt64(&db.count),
			nBlocks:       atomic.LoadUint32(&db.nBlocks),
			blockIndex:    db.blockIndex,
			timeWindowIdx: db.timeWindow.getTimeWindowIdx(),
			freeblockOff:  db.freeblockOff,
			cacheID:       db.cacheID,
			hashSeed:      db.hashSeed,
		},
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
	db.timeWindow.setTimeWindowIdx(db.dbInfo.timeWindowIdx)
	if readFreeList {
		if err := db.data.fb.read(db.data.file, db.dbInfo.freeblockOff); err != nil {
			return err
		}
	}
	db.dbInfo.freeblockOff = -1
	return nil
}

// initLoad loads topic and offset from timewindow files
func (db *DB) loadTopicHash() error {
	err := db.timeWindow.foreachWindowBlock(func(curb windowHandle) (bool, error) {
		b := &curb
		pOff := b.offset
		sOff, ok := db.trie.getOffset(b.topicHash)
		if !ok || sOff < pOff {
			if ok := db.trie.setOffset(b.topicHash, pOff); !ok {
				if b.entryIdx == 0 {
					return false, nil
				}
				seq := b.winEntries[b.entryIdx-1].seq
				off := blockOffset(startBlockIndex(seq))
				lb := blockHandle{file: db.index, offset: off}
				if err := lb.read(); err != nil {
					if err == io.EOF {
						return false, nil
					}
					return true, err
				}
				entryIdx := -1
				for i := 0; i < entriesPerBlock; i++ {
					e := lb.entries[i]
					if e.seq == seq { //record exist in db
						entryIdx = i
						break
					}
				}
				if entryIdx == -1 {
					return false, nil
				}
				e := lb.entries[entryIdx]
				t, err := db.data.readTopic(e)
				if err != nil {
					return true, err
				}
				topic := new(message.Topic)
				err = topic.Unmarshal(t)
				if err != nil {
					return true, err
				}
				if ok := db.trie.addTopic(message.Contract(topic.Parts), b.topicHash, topic.Parts, topic.Depth); ok {
					if ok := db.trie.setOffset(b.topicHash, pOff); !ok {
						return true, errors.New("topic_trie loading error: unable to set topic offset to topic trie")
					}
				}
			}
		}
		return false, nil
	})
	return err
}

// loadTrie loads topics to the trie for current time block. It loads recent messages seq per cache cap defined in the Options
func (db *DB) loadTrie() error {
	it := &TopicIterator{db: db}
	for {
		it.Next()
		if !it.Valid() {
			return nil
		}
		err := it.Error()
		if err != nil {
			logger.Error().Err(err).Str("context", "db.loadTrie")
			return err
		}
		we := winEntry{
			contract: it.Topic().Contract(),
			seq:      it.Topic().Seq(),
		}
		db.trie.add(it.Topic().Hash(), we)
	}
}

func (db *DB) sync() error {
	// writeHeader information to persist correct seq information to disk, also sync freeblocks to disk
	if err := db.writeHeader(false); err != nil {
		return err
	}
	if err := db.index.Sync(); err != nil {
		return err
	}
	if err := db.data.Sync(); err != nil {
		return err
	}
	return nil
}

func (db *DB) Sync() error {
	// start := time.Now()
	// write to db happens synchronously
	db.syncLockC <- struct{}{}
	db.closeW.Add(1)
	defer func() {
		<-db.syncLockC
		db.closeW.Done()
		// db.meter.TimeSeries.AddTime(time.Since(start))
	}()

	seqs, err := db.wal.Scan()
	if err != nil {
		return err
	}
	if len(seqs) == 0 {
		return nil
	}

	err = db.timeWindow.foreachTimeWindow(true, func(windowEntries map[uint64]windowEntries) (bool, error) {
		if err := db.extendBlocks(); err != nil {
			return false, err
		}

		var wEntry winEntry
		for h, wEntries := range windowEntries {
			topicOff, ok := db.trie.getOffset(h)
			if !ok {
				return true, errors.New("db.Sync: timeWindow sync error: unbale to get topic offset from trie")
			}
			wb, err := db.timeWindow.getWindowBlockHandle(h, topicOff)
			if err != nil {
				return true, err
			}
			var b *blockHandle
			for _, we := range wEntries {
				if we.Seq() == 0 {
					continue
				}
				wEntry = we.(winEntry)
				if ok := db.trie.add(h, wEntry); !ok {
					return true, errors.New("db:Sync: unbale to add entry to trie")
				}
				newOff, err := db.timeWindow.write(&wb, we)
				if err != nil {
					return true, err
				}
				if newOff > topicOff {
					if ok := db.trie.setOffset(h, newOff); !ok {
						return true, errors.New("db:Sync: timeWindow sync error: unbale to set topic offset in trie")
					}
				}
				mseq := db.cacheID ^ wEntry.seq
				memdata, err := db.mem.Get(wEntry.contract, mseq)
				if err != nil {
					return true, err
				}
				e := entry{}
				if err = e.UnmarshalBinary(memdata[:entrySize]); err != nil {
					return true, err
				}

				startTimeIdx := startBlockIndex(e.seq)
				off := blockOffset(startTimeIdx)
				if off != newOff {
					newOff = off
					b = &blockHandle{file: db.index, offset: off}
					if err := b.read(); err != nil {
						return true, err
					}
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
				db.incount()
				if e.mOffset, err = db.data.write(memdata[entrySize:]); err != nil {
					return true, err
				}

				db.meter.InBytes.Inc(int64(e.valueSize))
				b.entries[b.entryIdx] = entry{
					seq:       e.seq,
					topicSize: e.topicSize,
					valueSize: e.valueSize,
					mOffset:   e.mOffset,
					expiresAt: e.expiresAt,
				}
				b.entryIdx++
				if err := b.write(); err != nil {
					return true, err
				}

				db.filter.Append(e.seq)
				db.meter.InMsgs.Inc(1)
			}
		}
		db.mem.Free(wEntry.contract, wEntry.seq)
		return false, nil
	})
	if err := db.timeWindow.Sync(); err != nil {
		return err
	}
	if err := db.sync(); err != nil {
		return err
	}
	if err == nil {
		for _, s := range seqs {
			if err := db.wal.SignalLogApplied(s); err != nil {
				return err
			}
		}
	} else {
		// run db recovery if an error occur with the db sync
		if err := db.recoverLog(); err != nil {
			// if unable to recover db then close db
			panic(fmt.Sprintf("db.Sync: Unable to recover db on sync error %v. Closing db...", err))
		}
	}
	return nil
}

// ExpireOldEntries run expirer to delete entries from db if ttl was set on entries and it has expired
func (db *DB) ExpireOldEntries() {
	// expiry happens synchronously
	db.syncLockC <- struct{}{}
	defer func() {
		<-db.syncLockC
	}()
	expiredEntries := db.timeWindow.expireOldEntries(maxResults)
	for _, expiredEntry := range expiredEntries {
		e := expiredEntry.(entry)
		/// Test filter block if message hash presence
		if !db.filter.Test(e.seq) {
			continue
		}
		etopic, err := db.data.readTopic(e)
		if err != nil {
			continue
		}
		topic := new(message.Topic)
		topic.Unmarshal(etopic)
		contract := message.Contract(topic.Parts)
		db.trie.remove(topic.GetHash(contract), expiredEntry.(winEntry))
		db.freeslots.free(e.seq)
		db.data.free(e.mSize(), e.mOffset)
		db.decount()
	}
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
	b := blockHandle{file: db.index.FileManager, offset: off}
	if err := b.read(); err != nil {
		return entry{}, err
	}

	for i := 0; i < entriesPerBlock; i++ {
		e := b.entries[i]
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
		if q.Limit == 0 && limit == 0 {
			q.Limit = maxResults // Maximum number of records to return
		}
		if limit > q.Limit {
			q.Limit = limit
		}
	}

	mu := db.getMutex(q.contract)
	mu.RLock()
	defer mu.RUnlock()
	// lookups are performed in following order
	// ilookup lookups in memory entries from timeWindow those are not yet sync
	// trie lookup lookups in memory entries from trie as trie cachse recent entries at the time of timeWindow sync
	// lookup lookups persisted entries if fanout is true
	// lookup gets most recent entries without need for sorting. This is an art and not technological solution:)
	wEntries, topicHss, topicOffsets, fanout := db.trie.lookup(q.contract, q.parts, q.Limit)
	for i, topicHash := range topicHss {
		nextOff := int64(0)
		if q.winEntries, fanout = db.timeWindow.ilookup(topicHash, q.Limit); fanout {
			q.winEntries = append(q.winEntries, wEntries...)
		}
		if fanout {
			limit := q.Limit - uint32(len(wEntries))
			wEntries, nextOff = db.timeWindow.lookup(topicHash, topicOffsets[i], len(wEntries), limit)
			q.winEntries = append(q.winEntries, wEntries...)
		}
		if len(q.winEntries) == 0 {
			return
		}

		expiryCount := 0
		start := uint32(0)
		limit := q.Limit
		if len(q.winEntries) < int(q.Limit) {
			limit = uint32(len(q.winEntries))
		}
		for {
			for _, we := range q.winEntries[start:limit] {
				err = func() error {
					if we.seq == 0 {
						return nil
					}
					e, err := db.readEntry(q.contract, we.seq)
					if err != nil {
						return err
					}
					if e.isExpired() {
						expiryCount++
						if ok := db.trie.remove(topicHash, we); ok {
							db.timeWindow.addExpiry(e)
						}
						// if id is expired it does not return an error but continue the iteration
						return nil
					}
					var id message.ID
					id, val, err := db.data.readMessage(e)
					if err != nil {
						return err
					}
					id = message.ID(id)
					if !id.EvalPrefix(q.contract, q.cutoff) {
						return nil
					}

					if id.IsEncrypted() {
						val, err = db.mac.Decrypt(nil, val)
						if err != nil {
							return err
						}
					}
					var buffer []byte
					val, err = snappy.Decode(buffer, val)
					if err != nil {
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

			if uint32(cap(q.winEntries)) == limit && cap(q.winEntries) < int(q.Limit) && nextOff > 0 {
				limit := q.Limit - uint32(len(items))
				wEntries, nextOff = db.timeWindow.lookup(topicHash, nextOff, 0, limit)
				if len(wEntries) == 0 {
					break
				}
				q.winEntries = append(q.winEntries, wEntries...)
			}

			if len(items) >= int(q.Limit) || uint32(cap(q.winEntries)) == limit {
				break
			}

			if cap(q.winEntries) < int(q.Limit+uint32(expiryCount)) {
				start = limit
				limit = uint32(cap(q.winEntries))
			} else {
				start = limit
				limit = limit + uint32(expiryCount)
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
		if q.Limit == 0 && limit == 0 {
			q.Limit = maxResults // Maximum number of records to return
		}
		if limit > q.Limit {
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
// New entry is later stored in first time block. ID generated using NewID expires after maxBlockDur.
func (db *DB) NewID() []byte {
	return message.NewID(db.nextSeq(), false)
}

// newBlock adds new block to db table and return block offset
func (db *DB) newBlock() (int64, error) {
	off, err := db.index.extend(blockSize)
	db.addblock()
	db.blockIndex++
	return off, err
}

// extendBlocks adds new blocks to db table for the batch write
func (db *DB) extendBlocks() error {
	nBlocks := uint32(float64(db.getSeq()) / float64(entriesPerBlock))
	for nBlocks > db.blockIndex {
		if _, err := db.newBlock(); err != nil {
			return err
		}
	}
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
		//1410065408 10 sec
		return topic, ttl, nil
	}
	return topic, 0, nil
}

func (db *DB) setEntry(e *Entry, ttl int64) error {
	//message ID is the database key
	var id message.ID
	var ok bool
	var seq uint64
	encryption := db.encryption == 1 || e.encryption
	if ttl > 0 {
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
		ok, seq = db.freeslots.get(e.contract)
		if !ok {
			seq = db.nextSeq()
		}
		id = message.NewID(seq, encryption)
		id.AddContract(e.contract)
	}
	val := snappy.Encode(nil, e.Payload)
	e.seq = seq
	e.id = id
	e.val = val
	return nil
}

// Put puts entry to db. It uses default Contract to put entry into db.
// It is safe to modify the contents of the argument after Put returns but not
// before.
func (db *DB) Put(topic, value []byte) error {
	return db.PutEntry(NewEntry(topic, value))
}

// PutEntry puts entry to the db, if contract is not specifies then it uses master contract.
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
	case len(e.Payload) > MaxValueLength:
		return errValueTooLarge
	}
	topic, ttl, err := db.parseTopic(e)
	if err != nil {
		return err
	}
	e.topic = topic.Marshal()
	e.contract = message.Contract(topic.Parts)
	e.topicHash = topic.GetHash(e.contract)
	if err := db.setEntry(e, ttl); err != nil {
		return err
	}
	// Encryption.
	if db.encryption == 1 {
		e.val = db.mac.Encrypt(nil, e.val)
	}

	var ok bool
	we := winEntry{
		contract: e.contract,
		seq:      e.seq,
	}
	if ok = db.trie.addTopic(e.contract, e.topicHash, topic.Parts, topic.Depth); ok {
		if err := db.timeWindow.add(e.topicHash, we); err != nil {
			return err
		}
		e.topicOffset, _ = db.trie.getOffset(e.topicHash)
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
	}

	return nil
}

// packentry marshal entry along with message data
func (db *DB) packEntry(e *Entry) ([]byte, error) {
	if db.Count() == MaxKeys {
		return nil, errFull
	}
	e1 := entry{
		seq:       e.seq,
		topicSize: uint16(len(e.topic)),
		valueSize: uint32(len(e.val)),
		expiresAt: e.ExpiresAt,

		topicOffset: e.topicOffset,
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
	// commit writes batches into write ahead log. The write happen synchronously.
	db.writeLockC <- struct{}{}
	db.closeW.Add(1)
	defer func() {
		db.closeW.Done()
		<-db.writeLockC
	}()

	if db.tinyBatch.count() == 0 {
		return nil
	}

	logWriter, err := db.wal.NewWriter()
	if err != nil {
		return err
	}

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
	logSeq := db.wal.NextSeq()
	db.wal.SetBlocks(db.blocks())
	if err := <-logWriter.SignalInitWrite(logSeq); err != nil {
		return err
	}
	db.tinyBatch.reset()
	db.bufPool.Put(db.tinyBatch.buffer)
	return nil
}

// commit commits batches to write ahead log
func (db *DB) commit(l int, data []byte) (uint64, <-chan error) {
	done := make(chan error, 1)
	if err := db.ok(); err != nil {
		done <- err
		return 0, done
	}
	db.closeW.Add(1)
	defer db.closeW.Done()

	logWriter, err := db.wal.NewWriter()
	if err != nil {
		done <- err
		return 0, done
	}

	offset := uint32(0)
	for i := 0; i < l; i++ {
		dataLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		if err := <-logWriter.Append(data[offset+4 : offset+dataLen]); err != nil {
			done <- err
			return 0, done
		}
		offset += dataLen
	}

	db.meter.Puts.Inc(int64(l))
	logSeq := db.wal.NextSeq()
	db.wal.SetBlocks(db.blocks())
	return logSeq, logWriter.SignalInitWrite(logSeq)
}

// Delete sets entry for deletion.
// It is safe to modify the contents of the argument after Delete returns but not
// before.
func (db *DB) Delete(id, topic []byte) error {
	return db.DeleteEntry(&Entry{ID: id, Topic: topic})
}

// DeleteEntry delets an entry from db. you must provide an ID to delete an entry.
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
	we := winEntry{
		contract: contract,
		seq:      message.ID(id).Seq(),
	}
	if ok := db.trie.remove(topic.GetHash(contract), we); ok {
		err := db.delete(message.ID(id).Seq())
		if err != nil {
			return err
		}
	}
	return nil
}

// delete deletes the given key from the DB.
func (db *DB) delete(seq uint64) error {
	/// Test filter block for the message id presence
	if !db.filter.Test(seq) {
		return nil
	}
	db.meter.Dels.Inc(1)
	startBlockIdx := startBlockIndex(seq)
	off := blockOffset(startBlockIdx)
	b := &blockHandle{file: db.index, offset: off}
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

	e := b.entries[entryIdx]
	b.del(entryIdx)
	b.entryIdx--
	if err := b.write(); err != nil {
		return err
	}
	db.freeslots.free(e.seq)
	db.data.free(e.mSize(), e.mOffset)
	db.decount()
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

func (db *DB) getSeq() uint64 {
	return atomic.LoadUint64(&db.seq)
}

func (db *DB) nextSeq() uint64 {
	return atomic.AddUint64(&db.seq, 1)
}

// Count returns the number of items in the DB.
func (db *DB) Count() int64 {
	return atomic.LoadInt64(&db.count)
}

// Count returns the number of items in the DB.
func (db *DB) blocks() uint32 {
	return atomic.LoadUint32(&db.nBlocks)
}

// Count returns the number of items in the DB.
func (db *DB) addblock() uint32 {
	return atomic.AddUint32(&db.nBlocks, 1)
}

func (db *DB) incount() int64 {
	return atomic.AddInt64(&db.count, 1)
}

func (db *DB) decount() int64 {
	return atomic.AddInt64(&db.count, -11)
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
