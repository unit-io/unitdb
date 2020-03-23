package tracedb

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/unit-io/bpool"
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

	bufSize := int64(1 << 27)
	off := int64(-1)
	var (
		upperSeq uint64
		wEntry   winEntry
		b        blockHandle
		rawData  *bpool.Buffer
	)
	rawData = db.bufPool.Get()
	defer func() {
		db.bufPool.Put(rawData)
	}()

	if err := db.extendBlocks(); err != nil {
		return err
	}

	write := func() error {
		if _, err := db.data.write(rawData.Bytes()); err != nil {
			return err
		}
		// reset blocks
		rawData.Reset()
		return nil
	}

	err := db.timeWindow.foreachTimeWindow(true, func(windowEntries map[uint64]windowEntries) (bool, uint64, error) {
		// var m runtime.MemStats
		// runtime.ReadMemStats(&m)
		// log.Println(float64(m.Sys) / 1024 / 1024)
		// log.Println(float64(m.HeapAlloc) / 1024 / 1024)
		if len(windowEntries) == 0 {
			return false, 0, nil
		}

		for h, wEntries := range windowEntries {
			topicOff, ok := db.trie.getOffset(h)
			if !ok {
				return true, wEntry.seq, errors.New("db.Sync: timeWindow sync error: unable to get topic offset from trie")
			}
			wOff, err := db.timeWindow.sync(h, topicOff, wEntries)
			if err != nil {
				return true, wEntry.seq, err
			}
			if ok := db.trie.setOffset(h, wOff); !ok {
				return true, wEntry.seq, errors.New("db:Sync: timeWindow sync error: unable to set topic offset in trie")
			}
			sort.Slice(wEntries[:], func(i, j int) bool {
				return wEntries[i].Seq() < wEntries[j].Seq()
			})
			for _, we := range wEntries {
				if we.Seq() == 0 {
					continue
				}
				wEntry = we.(winEntry)
				if ok := db.trie.add(h, wEntry); !ok {
					return true, wEntry.seq, errors.New("db:Sync: unable to add entry to trie")
				}
				mseq := db.cacheID ^ wEntry.seq
				memdata, err := db.mem.Get(wEntry.contract, mseq)
				if err != nil {
					return true, wEntry.seq, err
				}
				memEntry := entry{}
				if err = memEntry.UnmarshalBinary(memdata[:entrySize]); err != nil {
					return true, wEntry.seq, err
				}
				startBlockIdx := startBlockIndex(wEntry.seq)
				newOff := blockOffset(startBlockIdx)
				if off != newOff {
					off = newOff
					// write previous block
					if err := b.write(); err != nil {
						return true, wEntry.seq, err
					}
					b = blockHandle{file: db.index, offset: newOff}
					if err := b.read(); err != nil {
						return true, wEntry.seq, err
					}
				}
				entryIdx := 0
				for i := 0; i < int(b.entryIdx); i++ {
					e := b.entries[i]
					if e.seq == wEntry.seq { //record exist in db
						entryIdx = -1
						break
					}
				}
				if entryIdx == -1 {
					continue
				}
				db.incount()
				if memEntry.msgOffset, err = db.data.writeMessage(memdata[entrySize:]); err != nil {
					if err != errLeasedBlock {
						return true, wEntry.seq, err
					}
				} else {
					dataLen := align(uint32(len(memdata[entrySize:])))
					if off, err := rawData.Extend(int64(dataLen)); err == nil {
						_, err = rawData.WriteAt(memdata[entrySize:], off)
					}
					if err != nil {
						return true, wEntry.seq, err
					}
				}
				if err := b.append(memEntry); err != nil {
					return true, wEntry.seq, err
				}

				db.filter.Append(wEntry.seq)
				db.meter.InMsgs.Inc(1)
				db.meter.InBytes.Inc(int64(memEntry.valueSize))
			}

			if upperSeq < wEntry.seq {
				upperSeq = wEntry.seq
			}

			// write any pending block
			if err := b.write(); err != nil {
				return true, wEntry.seq, err
			}

			if rawData.Size() > bufSize {
				if err := write(); err != nil {
					return true, wEntry.seq, err
				}

				if err := db.sync(); err != nil {
					return true, wEntry.seq, err
				}

				if err := db.wal.SignalLogApplied(upperSeq); err != nil {
					return true, wEntry.seq, err
				}
			}

			db.mem.Free(wEntry.contract, db.cacheID^wEntry.seq)
		}

		return false, wEntry.seq, nil
	})

	if err := write(); err != nil {
		return err
	}

	if err := db.sync(); err != nil {
		return err
	}

	if err := db.wal.SignalLogApplied(upperSeq); err != nil {
		return err
	}
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

func uniqueBlocks(blocks []*blockHandle) ([]*blockHandle, error) {
	if len(blocks) == 0 {
		return blocks, nil
	}
	sort.Slice(blocks[:], func(i, j int) bool {
		return blocks[i].offset < blocks[j].offset
	})
	j := 0
	for i := 1; i < len(blocks); i++ {
		if blocks[j].offset == blocks[i].offset {
			continue
		}
		j++
		// preserve the original data
		// in[i], in[j] = in[j], in[i]
		// only set what is required
		blocks[j] = blocks[i]
	}

	return blocks[:j+1], nil
}
