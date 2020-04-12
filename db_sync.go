package tracedb

import (
	"errors"
	"fmt"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/tracedb/message"
)

type (
	internal struct {
		startBlockIdx int32
		upperSeq      uint64
		lastSyncSeq   uint64
		syncStatusOk  bool
		inBytes       int64
		count         int64

		rawWindow *bpool.Buffer
		rawBlock  *bpool.Buffer
		rawData   *bpool.Buffer
	}
	syncHandle struct {
		internal
		*DB

		windowWriter *windowWriter
		blockWriter  *blockWriter
		dataWriter   *dataWriter
	}
)

func (db *syncHandle) startSync() bool {
	if db.lastSyncSeq == db.Seq() {
		db.syncStatusOk = false
		return db.syncStatusOk
	}
	db.startBlockIdx = db.blocks()
	db.lastSyncSeq = db.Seq()

	db.rawWindow = db.bufPool.Get()
	db.rawBlock = db.bufPool.Get()
	db.rawData = db.bufPool.Get()

	db.windowWriter = newWindowWriter(db.timeWindow, db.rawWindow)
	db.blockWriter = newBlockWriter(&db.index, db.rawBlock)
	db.dataWriter = newDataWriter(&db.data, db.rawData)
	db.syncStatusOk = true

	return db.syncStatusOk
}

func (db *syncHandle) finish() error {
	if !db.syncStatusOk {
		return nil
	}

	db.bufPool.Put(db.rawWindow)
	db.bufPool.Put(db.rawBlock)
	db.bufPool.Put(db.rawData)
	db.syncStatusOk = false
	return nil
}

func (db *syncHandle) status() (ok bool) {
	return db.syncStatusOk
}

func (in *internal) reset() error {
	in.count = 0
	in.inBytes = 0
	in.rawWindow.Reset()
	in.rawBlock.Reset()
	in.rawData.Reset()
	return nil
}

func (db *DB) startSyncer(interval time.Duration) {
	syncTicker := time.NewTicker(interval)
	syncHandle := syncHandle{DB: db, internal: internal{}}
	go func() {
		defer func() {
			syncTicker.Stop()
		}()
		for {
			select {
			case <-db.closeC:
				return
			case <-syncTicker.C:
				if ok := syncHandle.status(); ok {
					// sync is in-progress
					continue
				}
				if err := syncHandle.Sync(); err != nil {
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

func (db *DB) sync() error {
	// writeHeader information to persist correct seq information to disk, also sync freeblocks to disk
	if err := db.writeHeader(false); err != nil {
		return err
	}
	if err := db.timeWindow.Sync(); err != nil {
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

// Sync syncs entries into DB. Sync happens synchronously.
// Sync write window entries into summary file and write index, and data to respective index and data files.
// In case of any error during sync operation recovery is performed on log file (write ahead log).
func (db *syncHandle) Sync() error {
	// start := time.Now()
	// Sync happens synchronously
	db.syncLockC <- struct{}{}
	db.closeW.Add(1)
	defer func() {
		<-db.syncLockC
		db.closeW.Done()
		// db.meter.TimeSeries.AddTime(time.Since(start))
	}()

	if ok := db.startSync(); !ok {
		return nil
	}
	defer func() {
		db.internal.reset()
		db.finish()
	}()

	err := db.timeWindow.foreachTimeWindow(true, func(last bool, windowEntries map[uint64]windowEntries) (bool, error) {
		var wEntry winEntry
		for h, wEntries := range windowEntries {
			topicOff, ok := db.trie.getOffset(h)
			if !ok {
				return true, errors.New("db.Sync: timeWindow sync error: unable to get topic offset from trie")
			}
			wOff, err := db.windowWriter.append(h, topicOff, wEntries)
			if err != nil {
				return true, err
			}
			if ok := db.trie.setOffset(h, wOff); !ok {
				return true, errors.New("db:Sync: timeWindow sync error: unable to set topic offset in trie")
			}
			for _, we := range wEntries {
				if we.Seq() == 0 {
					continue
				}
				wEntry = we.(winEntry)
				mseq := db.cacheID ^ wEntry.seq
				memdata, err := db.mem.Get(wEntry.contract, mseq)
				if err != nil {
					return true, err
				}
				memEntry := entry{}
				if err = memEntry.UnmarshalBinary(memdata[:entrySize]); err != nil {
					return true, err
				}

				if memEntry.msgOffset, err = db.dataWriter.writeMessage(memdata[entrySize:]); err != nil {
					return true, err
				}
				exists, err := db.blockWriter.append(memEntry, db.startBlockIdx)
				if err != nil {
					return true, err
				}
				if exists {
					continue
				}

				db.filter.Append(wEntry.seq)
				db.internal.count++
				db.internal.inBytes += int64(memEntry.valueSize)
			}

			if db.upperSeq < wEntry.seq {
				db.upperSeq = wEntry.seq
			}

			if db.rawData.Size() > db.opts.BufferSize {
				if db.blockWriter.UpperSeq() == 0 {
					return false, nil
				}
				nBlocks := int32((db.blockWriter.UpperSeq() - 1) / entriesPerIndexBlock)
				if nBlocks > db.blocks() {
					// fmt.Println("db.startSync: startBlockIdx, nBlocks ", db.startBlockIdx, nBlocks)
					if err := db.extendBlocks(nBlocks - db.blocks()); err != nil {
						return true, err
					}
				}
				if err := db.windowWriter.write(); err != nil {
					return true, err
				}
				if err := db.blockWriter.write(); err != nil {
					return true, err
				}
				if _, err := db.dataWriter.write(); err != nil {
					return true, err
				}
				if err := db.sync(); err != nil {
					return true, err
				}
				db.incount(db.internal.count)
				db.meter.Syncs.Inc(db.internal.count)
				db.meter.InMsgs.Inc(db.internal.count)
				db.meter.InBytes.Inc(db.internal.inBytes)

				db.internal.reset()
				if err := db.wal.SignalLogApplied(db.upperSeq); err != nil {
					return true, err
				}
			}

			db.mem.Free(wEntry.contract, db.cacheID^wEntry.seq)
		}

		if last {
			if db.blockWriter.UpperSeq() == 0 {
				return false, nil
			}
			nBlocks := int32((db.blockWriter.UpperSeq() - 1) / entriesPerIndexBlock)
			if nBlocks > db.blocks() {
				// fmt.Println("db.startSync: startBlockIdx, nBlocks ", db.startBlockIdx, nBlocks)
				if err := db.extendBlocks(nBlocks - db.blocks()); err != nil {
					return true, err
				}
			}
			if _, err := db.dataWriter.write(); err != nil {
				return true, err
			}
			if err := db.blockWriter.write(); err != nil {
				return true, err
			}
			if err := db.windowWriter.write(); err != nil {
				return true, err
			}
			if err := db.sync(); err != nil {
				return true, err
			}
			if err := db.wal.SignalLogApplied(db.upperSeq); err != nil {
				return true, err
			}
			db.incount(db.internal.count)
			db.meter.Syncs.Inc(db.internal.count)
			db.meter.InMsgs.Inc(db.internal.count)
			db.meter.InBytes.Inc(db.internal.inBytes)

			db.internal.reset()
		}

		return false, nil
	})

	if err != nil {
		// run db recovery if an error occur with the db sync
		if err := db.startRecovery(); err != nil {
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
		db.data.free(e)
		db.decount()
	}
}
