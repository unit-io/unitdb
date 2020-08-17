/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unitdb

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
)

type (
	internal struct {
		*DB

		startBlockIdx int32
		lastSyncSeq   uint64
		upperSeq      uint64
		syncStatusOk  bool
		syncComplete  bool
		inBytes       int64
		count         int64
		errCount      uint64

		rawWindow *bpool.Buffer
		rawBlock  *bpool.Buffer
		rawData   *bpool.Buffer

		// offsets for rollback in case of sync error
		winOff   int64
		blockOff int64
		dataOff  int64
	}
	syncHandle struct {
		internal

		windowWriter *windowWriter
		blockWriter  *blockWriter
		dataWriter   *dataWriter
	}
)

func (db *syncHandle) startSync() bool {
	if db.lastSyncSeq == db.seq() {
		db.syncStatusOk = false
		return db.syncStatusOk
	}
	db.startBlockIdx = db.blocks()

	db.rawWindow = db.bufPool.Get()
	db.rawBlock = db.bufPool.Get()
	db.rawData = db.bufPool.Get()

	db.windowWriter = newWindowWriter(db.timeWindow, db.rawWindow)
	db.blockWriter = newBlockWriter(&db.index, db.rawBlock)
	db.dataWriter = newDataWriter(&db.data, db.rawData)

	db.winOff = db.timeWindow.currSize()
	db.blockOff = db.index.currSize()
	db.dataOff = db.data.currSize()
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
	in.lastSyncSeq = in.upperSeq
	in.count = 0
	in.inBytes = 0
	in.upperSeq = 0
	in.startBlockIdx = in.blocks()

	in.rawWindow.Reset()
	in.rawBlock.Reset()
	in.rawData.Reset()

	in.winOff = in.timeWindow.currSize()
	in.blockOff = in.index.currSize()
	in.dataOff = in.data.currSize()

	return nil
}

func (db *syncHandle) abort() error {
	defer db.internal.reset()
	if db.syncComplete {
		return nil
	}
	// rollback blocks
	// fmt.Println("syncHandle.abort: abort blocks sync")
	db.data.truncate(db.dataOff)
	db.index.truncate(db.blockOff)
	db.timeWindow.truncate(db.winOff)
	atomic.StoreInt32(&db.blockIdx, db.startBlockIdx)
	db.decount(uint64(db.count))

	if err := db.dataWriter.rollback(); err != nil {
		return err
	}
	if err := db.blockWriter.rollback(); err != nil {
		return err
	}
	if err := db.windowWriter.rollback(); err != nil {
		return err
	}

	db.lease.defrag()
	if err := db.lease.write(); err != nil {
		logger.Error().Err(err).Str("context", "db.abort")
		return err
	}

	return nil
}

func (db *DB) startSyncer(interval time.Duration) {
	syncTicker := time.NewTicker(interval)
	go func() {
		defer func() {
			syncTicker.Stop()
		}()
		for {
			select {
			case <-db.closeC:
				return
			case <-syncTicker.C:
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
				db.expireEntries()
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

func (db *syncHandle) sync(recovery bool) error {
	if db.internal.upperSeq == 0 {
		return nil
	}
	db.syncComplete = false
	defer db.abort()

	if _, err := db.dataWriter.write(); err != nil {
		logger.Error().Err(err).Str("context", "data.write")
		return err
	}

	nBlocks := int32((db.internal.upperSeq - 1) / entriesPerIndexBlock)
	if nBlocks > db.blocks() {
		if err := db.extendBlocks(nBlocks - db.blocks()); err != nil {
			logger.Error().Err(err).Str("context", "db.extendBlocks")
			return err
		}
	}
	if err := db.windowWriter.write(); err != nil {
		logger.Error().Err(err).Str("context", "timeWindow.write")
		return err
	}
	if err := db.blockWriter.write(); err != nil {
		logger.Error().Err(err).Str("context", "block.write")
		return err
	}

	if err := db.DB.sync(); err != nil {
		return err
	}
	db.incount(uint64(db.internal.count))
	if recovery {
		db.meter.Recovers.Inc(db.internal.count)
	}
	db.meter.Syncs.Inc(db.internal.count)
	db.meter.InMsgs.Inc(db.internal.count)
	db.meter.InBytes.Inc(db.internal.inBytes)
	db.syncComplete = true
	return nil
}

// Sync syncs entries into DB. Sync happens synchronously.
// Sync write window entries into summary file and write index, and data to respective index and data files.
// In case of any error during sync operation recovery is performed on log file (write ahead log).
func (db *syncHandle) Sync() error {
	// // CPU profiling by default
	// defer profile.Start().Stop()
	var err1 error
	baseSeq := db.internal.lastSyncSeq
	err := db.timeWindow.foreachTimeWindow(func(timeID int64, wEntries windowEntries) (bool, error) {
		winEntries := make(map[uint64]windowEntries)
		for _, we := range wEntries {
			if we.Seq() == 0 {
				db.errCount++
				continue
			}
			if we.Seq() < baseSeq {
				baseSeq = we.Seq()
			}
			if we.Seq() > db.internal.upperSeq {
				db.internal.upperSeq = we.Seq()
			}
			blockID := startBlockIndex(we.Seq())
			mseq := db.cacheID ^ uint64(we.Seq())
			memdata, err := db.mem.Get(uint64(blockID), mseq)
			if err != nil || memdata == nil {
				logger.Error().Err(err).Str("context", "mem.Get")
				err1 = err
				break
			}
			var e entry
			if err = e.UnmarshalBinary(memdata[:entrySize]); err != nil {
				err1 = err
			}
			s := slot{
				seq:       e.seq,
				topicSize: e.topicSize,
				valueSize: e.valueSize,

				cacheBlock: memdata[entrySize:],
			}
			if s.msgOffset, err = db.dataWriter.append(s.cacheBlock); err != nil {
				err1 = err
				break
			}
			if exists, err := db.blockWriter.append(s, db.startBlockIdx); exists || err != nil {
				if err != nil {
					err1 = err
					break
				}
				db.errCount++
				continue
			}

			if _, ok := winEntries[e.topicHash]; ok {
				winEntries[e.topicHash] = append(winEntries[e.topicHash], we)
			} else {
				winEntries[e.topicHash] = windowEntries{we}
			}

			db.filter.Append(we.Seq())
			db.internal.count++
			db.internal.inBytes += int64(e.valueSize)
		}
		for h := range winEntries {
			topicOff, ok := db.trie.getOffset(h)
			if !ok {
				return true, errors.New("db.Sync: timeWindow sync error: unable to get topic offset from trie")
			}
			wOff, err := db.windowWriter.append(h, topicOff, winEntries[h])
			if err != nil {
				return true, err
			}
			if ok := db.trie.setOffset(topic{hash: h, offset: wOff}); !ok {
				return true, errors.New("db:Sync: timeWindow sync error: unable to set topic offset in trie")
			}
		}
		blockID := startBlockIndex(baseSeq)
		db.mem.Free(uint64(blockID), db.cacheID^baseSeq)
		if err1 != nil {
			return false, err1
		}

		if err := db.sync(false); err != nil {
			return false, err
		}
		if db.syncComplete {
			if err := db.wal.SignalLogApplied(timeID); err != nil {
				logger.Error().Err(err).Str("context", "wal.SignalLogApplied")
				return false, err
			}
		}
		return false, nil
	})
	if err != nil || err1 != nil {
		db.syncComplete = false
		db.abort()
		// run db recovery if an error occur with the db sync
		if err := db.startRecovery(); err != nil {
			// if unable to recover db then close db
			panic(fmt.Sprintf("db.Sync: Unable to recover db on sync error %v. Closing db...", err))
		}
	}

	return db.sync(false)
}

// expireEntries run expirer to delete entries from db if ttl was set on entries and that has expired
func (db *DB) expireEntries() error {
	// sync happens synchronously
	db.syncLockC <- struct{}{}
	defer func() {
		<-db.syncLockC
	}()
	expiredEntries := db.timeWindow.getExpiredEntries(db.opts.defaultQueryLimit)
	for _, expiredEntry := range expiredEntries {
		we := expiredEntry.(winEntry)
		/// Test filter block if message hash presence
		if !db.filter.Test(we.seq) {
			continue
		}
		off := blockOffset(startBlockIndex(we.Seq()))
		b := blockHandle{file: db.index, offset: off}
		if err := b.read(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
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
			return nil
		}
		e := b.entries[entryIdx]
		db.lease.free(e.seq, e.msgOffset, e.mSize())
		db.decount(1)
	}

	return nil
}
