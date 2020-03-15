package tracedb

import (
	"errors"
	"fmt"
	"time"

	"github.com/unit-io/tracedb/bpool"
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

	// var bufPool *bpool.BufferPool
	bufSize := int64(1 << 27)
	// db.once.Do(func() {
	// 	bufPool = bpool.NewBufferPool(bufSize)
	// })

	// defer db.once.Reset()

	err := db.timeWindow.foreachTimeWindow(true, func(windowEntries map[uint64]windowEntries) (bool, uint64, error) {
		// var m runtime.MemStats
		// runtime.ReadMemStats(&m)
		// log.Println(float64(m.Sys) / 1024 / 1024)
		// log.Println(float64(m.HeapAlloc) / 1024 / 1024)

		var (
			rawIndex, rawData *bpool.Buffer
			idxBlocks         []blockHandle
		)
		blockIdx := db.blocks()

		idxBlockOff := int64(0)
		dataBlockOff := int64(0)

		rawIndex = db.bufPool.Get()
		rawData = db.bufPool.Get()
		defer func() {
			db.bufPool.Put(rawData)
			db.bufPool.Put(rawIndex)
		}()

		write := func() error {
			if err := db.extendBlocks(); err != nil {
				return err
			}
			for _, idxBlock := range idxBlocks {
				rawIndex.Write(idxBlock.MarshalBinary())
			}
			if _, err := db.index.WriteAt(rawIndex.Bytes(), idxBlockOff); err != nil {
				return err
			}
			if _, err := db.data.WriteAt(rawData.Bytes(), dataBlockOff); err != nil {
				return err
			}

			// reset blocks
			idxBlockOff = int64(0)
			dataBlockOff = int64(0)
			idxBlocks = idxBlocks[:0]
			rawIndex.Reset()
			rawData.Reset()
			return nil
		}

		var wEntry winEntry
		for h, wEntries := range windowEntries {
			topicOff, ok := db.trie.getOffset(h)
			if !ok {
				return true, wEntry.seq, errors.New("db.Sync: timeWindow sync error: unbale to get topic offset from trie")
			}
			wOff, err := db.timeWindow.sync(h, topicOff, wEntries)
			if err != nil {
				return true, wEntry.seq, err
			}
			if ok := db.trie.setOffset(h, wOff); !ok {
				return true, wEntry.seq, errors.New("db:Sync: timeWindow sync error: unbale to set topic offset in trie")
			}
			var off int64
			var bh, leasedBh blockHandle
			for _, we := range wEntries {
				if we.Seq() == 0 {
					continue
				}
				wEntry = we.(winEntry)
				mseq := db.cacheID ^ wEntry.seq
				memdata, err := db.mem.Get(wEntry.contract, mseq)
				if err != nil {
					return true, wEntry.seq, err
				}
				memEntry := entry{}
				if err = memEntry.UnmarshalBinary(memdata[:entrySize]); err != nil {
					fmt.Println("db.Sync: mem entry read error ", err)
					return true, wEntry.seq, err
				}
				startBlockIdx := startBlockIndex(wEntry.seq)
				newOff := blockOffset(startBlockIdx)
				bh = blockHandle{file: db.index, offset: newOff}
				// any seq less than current db count is leased seq
				if startBlockIdx < blockIdx {
					if newOff != off {
						off = newOff
						// write previous block
						if err := leasedBh.write(); err != nil {
							return true, wEntry.seq, err
						}
						if err := bh.read(); err != nil {
							return true, wEntry.seq, err
						}
					}
					entryIdx := 0
					for i := 0; i < entriesPerIndexBlock; i++ {
						e := bh.entries[i]
						if e.seq == wEntry.seq { //record exist in db
							entryIdx = -1
							break
						}
					}
					if entryIdx == -1 {
						continue
					}
					leasedBh = blockHandle{file: db.index, offset: bh.offset}
				} else {
					if idxBlockOff == 0 {
						idxBlockOff = newOff
					}
					idxBlocks = append(idxBlocks, bh)
				}
				db.incount()
				if memEntry.msgOffset, err = db.data.write(memdata[entrySize:]); err != nil {
					if err != errLeasedBlock {
						return true, wEntry.seq, err
					}
				} else {
					if dataBlockOff == 0 {
						dataBlockOff = memEntry.msgOffset
					}
					rawData.Write(memdata[entrySize:])
				}

				bh.entries[bh.entryIdx] = memEntry
				bh.entryIdx++

				db.filter.Append(wEntry.seq)
				db.meter.InMsgs.Inc(1)
				db.meter.InBytes.Inc(int64(memEntry.valueSize))
			}

			db.mem.Free(wEntry.contract, db.cacheID^wEntry.seq)

			// write any pending entries
			if err := leasedBh.write(); err != nil {
				return true, wEntry.seq, err
			}

			if rawData.Size() > bufSize {
				if err := write(); err != nil {
					return true, wEntry.seq, err
				}

				if err := db.sync(); err != nil {
					return true, wEntry.seq, err
				}

				if err := db.wal.SignalLogApplied(wEntry.seq); err != nil {
					return true, wEntry.seq, err
				}
			}
		}

		if err := write(); err != nil {
			return true, wEntry.seq, err
		}

		if err := db.sync(); err != nil {
			return true, wEntry.seq, err
		}

		if err := db.wal.SignalLogApplied(wEntry.seq); err != nil {
			return true, wEntry.seq, err
		}
		return false, wEntry.seq, nil
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
		db.freeslots.free(e.seq)
		db.data.free(e.mSize(), e.msgOffset)
		db.decount()
	}
}
