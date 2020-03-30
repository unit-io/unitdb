package tracedb

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/unit-io/tracedb/message"
)

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

// Sync syncs entries into DB. Sync happens synchronously.
// Sync write window entries into summary file and write index, and data to respective index and data files.
// In case of any error during sync operation recovery is performed on log file (write ahead log).
func (db *DB) Sync() error {
	// start := time.Now()
	// Sync happens synchronously
	db.syncLockC <- struct{}{}
	db.closeW.Add(1)
	defer func() {
		<-db.syncLockC
		db.closeW.Done()
		// db.meter.TimeSeries.AddTime(time.Since(start))
	}()

	blockIdx := db.blocks()
	var (
		upperSeq uint64
		wEntry   winEntry
	)
	block := db.bufPool.Get()
	data := db.bufPool.Get()

	blockWriter := newBlockWriter(db.index, block, blockOffset(blockIdx))
	dataWriter := newDataWriter(&db.data, data)
	defer func() {
		db.bufPool.Put(block)
		db.bufPool.Put(data)
	}()
	err := db.timeWindow.foreachTimeWindow(true, func(last bool, windowEntries map[uint64]windowEntries) (bool, error) {
		if len(windowEntries) == 0 {
			return false, nil
		}

		for h, wEntries := range windowEntries {
			topicOff, ok := db.trie.getOffset(h)
			if !ok {
				return true, errors.New("db.Sync: timeWindow sync error: unable to get topic offset from trie")
			}
			wOff, err := db.timeWindow.sync(h, topicOff, wEntries)
			if err != nil {
				return true, err
			}
			if ok := db.trie.setOffset(h, wOff); !ok {
				return true, errors.New("db:Sync: timeWindow sync error: unable to set topic offset in trie")
			}
			sort.Slice(wEntries[:], func(i, j int) bool {
				return wEntries[i].Seq() < wEntries[j].Seq()
			})
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

				db.meter.Syncs.Inc(1)
				if memEntry.msgOffset, err = dataWriter.writeMessage(memdata[entrySize:]); err != nil {
					return true, err
				}
				blockWriter.append(memEntry, startBlockIndex(memEntry.seq) < blockIdx)

				db.filter.Append(wEntry.seq)
				db.incount()
				db.meter.InMsgs.Inc(1)
				db.meter.InBytes.Inc(int64(memEntry.valueSize))
			}

			if upperSeq < wEntry.seq {
				upperSeq = wEntry.seq
			}

			if last || data.Size() > db.opts.BufferSize {
				nBlocks := blockWriter.Count()
				for i := 0; i < nBlocks; i++ {
					if _, err := db.newBlock(); err != nil {
						return false, err
					}
				}
				if err := blockWriter.write(); err != nil {
					return true, err
				}
				if _, err := dataWriter.write(); err != nil {
					return true, err
				}

				if err := db.sync(); err != nil {
					return true, err
				}

				if err := db.wal.SignalLogApplied(upperSeq); err != nil {
					return true, err
				}
			}

			db.mem.Free(wEntry.contract, db.cacheID^wEntry.seq)
		}

		return false, nil
	})

	if err != nil {
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
		db.data.free(e)
		db.decount()
	}
}
