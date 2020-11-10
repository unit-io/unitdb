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
	"sync/atomic"
	"time"

	"github.com/unit-io/bpool"
)

type (
	_SyncInfo struct {
		startBlockIdx  int32
		lastSyncSeq    uint64
		upperSeq       uint64
		syncStatusOk   bool
		syncComplete   bool
		inBytes        int64
		count          int64
		entriesInvalid uint64
	}
	_SyncHandle struct {
		syncInfo _SyncInfo
		*DB

		windowWriter *_WindowWriter
		blockWriter  *_BlockWriter
		dataWriter   *_DataWriter

		rawWindow *bpool.Buffer
		rawBlock  *bpool.Buffer
		rawData   *bpool.Buffer

		// offsets for rollback in case of sync error.
		winOff   int64
		blockOff int64
		dataOff  int64
	}
)

func (db *_SyncHandle) startSync() bool {
	if db.syncInfo.lastSyncSeq == db.seq() {
		db.syncInfo.syncStatusOk = false
		return db.syncInfo.syncStatusOk
	}
	db.syncInfo.startBlockIdx = db.blocks()

	db.rawWindow = db.internal.bufPool.Get()
	db.rawBlock = db.internal.bufPool.Get()
	db.rawData = db.internal.bufPool.Get()

	db.windowWriter = newWindowWriter(db.internal.timeWindow, db.rawWindow)
	db.blockWriter = newBlockWriter(&db.internal.index, db.rawBlock)
	db.dataWriter = newDataWriter(&db.internal.data, db.rawData)

	db.winOff = db.internal.timeWindow.file.currSize()
	db.blockOff = db.internal.index.currSize()
	db.dataOff = db.internal.data.file.currSize()
	db.syncInfo.syncStatusOk = true

	return db.syncInfo.syncStatusOk
}

func (db *_SyncHandle) finish() error {
	if !db.syncInfo.syncStatusOk {
		return nil
	}

	db.internal.bufPool.Put(db.rawWindow)
	db.internal.bufPool.Put(db.rawBlock)
	db.internal.bufPool.Put(db.rawData)

	db.syncInfo.syncStatusOk = false
	return nil
}

func (db *_SyncHandle) status() (ok bool) {
	return db.syncInfo.syncStatusOk
}

func (db *_SyncHandle) reset() error {
	db.syncInfo.lastSyncSeq = db.syncInfo.upperSeq
	db.syncInfo.count = 0
	db.syncInfo.inBytes = 0
	db.syncInfo.upperSeq = 0
	db.syncInfo.startBlockIdx = db.blocks()

	db.rawWindow.Reset()
	db.rawBlock.Reset()
	db.rawData.Reset()

	db.winOff = db.internal.timeWindow.file.currSize()
	db.blockOff = db.internal.index.currSize()
	db.dataOff = db.internal.data.file.currSize()

	return nil
}

func (db *_SyncHandle) abort() error {
	defer db.reset()
	if db.syncInfo.syncComplete {
		return nil
	}
	// rollback blocks.
	db.internal.data.file.truncate(db.dataOff)
	db.internal.index.truncate(db.blockOff)
	db.internal.timeWindow.file.truncate(db.winOff)
	atomic.StoreInt32(&db.internal.dbInfo.blockIdx, db.syncInfo.startBlockIdx)
	db.decount(uint64(db.syncInfo.count))

	if err := db.dataWriter.rollback(); err != nil {
		return err
	}
	if err := db.blockWriter.rollback(); err != nil {
		return err
	}
	if err := db.windowWriter.rollback(); err != nil {
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
			case <-db.internal.closeC:
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
			case <-db.internal.closeC:
				expirerTicker.Stop()
				return
			}
		}
	}()
}

func (db *DB) sync() error {
	// writeInfo information to persist correct seq information to disk.
	if err := db.writeInfo(); err != nil {
		return err
	}
	if err := db.fs.sync(); err != nil {
		return nil
	}

	return nil
}

func (db *_SyncHandle) sync(recovery bool) error {
	if db.syncInfo.upperSeq == 0 {
		return nil
	}
	db.syncInfo.syncComplete = false
	defer db.abort()

	if _, err := db.dataWriter.write(); err != nil {
		logger.Error().Err(err).Str("context", "data.write")
		return err
	}

	nBlocks := int32((db.syncInfo.upperSeq - 1) / entriesPerIndexBlock)
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
	db.incount(uint64(db.syncInfo.count))
	if recovery {
		db.internal.meter.Recovers.Inc(db.syncInfo.count)
	}
	db.internal.meter.Syncs.Inc(db.syncInfo.count)
	db.internal.meter.InMsgs.Inc(db.syncInfo.count)
	db.internal.meter.InBytes.Inc(db.syncInfo.inBytes)
	db.syncInfo.syncComplete = true
	return nil
}

// Sync syncs entries into DB. Sync happens synchronously.
// Sync write window entries into summary file and write index, and data to respective index and data files.
// In case of any error during sync operation recovery is performed on log file (write ahead log).
func (db *_SyncHandle) Sync() error {
	// // CPU profiling by default
	// defer profile.Start().Stop()
	var err1 error
	baseSeq := db.syncInfo.lastSyncSeq
	timeRelease := db.internal.timeWindow.release()
	err := db.internal.mem.ForEachBlock(db.opts.syncDurationType*time.Duration(db.opts.maxSyncDurations), func(timeID int64, seqs []uint64) (bool, error) {
		winEntries := make(map[uint64]_WindowEntries)
		for _, seq := range seqs {
			if seq < baseSeq {
				baseSeq = seq
			}
			if seq > db.syncInfo.upperSeq {
				db.syncInfo.upperSeq = seq
			}
			memdata, err := db.internal.mem.Lookup(timeID, seq)
			if err != nil || memdata == nil {
				db.syncInfo.entriesInvalid++
				logger.Error().Err(err).Str("context", "mem.Get")
				err1 = err
				continue
			}
			var e _Entry
			if err = e.UnmarshalBinary(memdata[:entrySize]); err != nil {
				db.syncInfo.entriesInvalid++
				err1 = err
				continue
			}
			s := _Slot{
				seq:       e.seq,
				topicSize: e.topicSize,
				valueSize: e.valueSize,

				cache: memdata[entrySize:],
			}

			if s.msgOffset, err = db.dataWriter.append(s.cache); err != nil {
				return true, err
			}
			if exists, err := db.blockWriter.append(s, db.syncInfo.startBlockIdx); exists || err != nil {
				if err != nil {
					return true, err
				}
				db.internal.freeList.free(s.seq, s.msgOffset, s.mSize())
				db.syncInfo.entriesInvalid++
				continue
			}
			we := newWinEntry(seq, e.expiresAt)
			if _, ok := winEntries[e.topicHash]; ok {
				winEntries[e.topicHash] = append(winEntries[e.topicHash], we)
			} else {
				winEntries[e.topicHash] = _WindowEntries{we}
			}
			db.internal.filter.Append(we.seq())
			db.syncInfo.count++
			db.syncInfo.inBytes += int64(e.valueSize)
		}
		for h := range winEntries {
			topicOff, ok := db.internal.trie.getOffset(h)
			if !ok {
				return true, errors.New("db.Sync: timeWindow sync error: unable to get topic offset from trie")
			}
			wOff, err := db.windowWriter.append(h, topicOff, winEntries[h])
			if err != nil {
				return true, err
			}
			if ok := db.internal.trie.setOffset(_Topic{hash: h, offset: wOff}); !ok {
				return true, errors.New("db:Sync: timeWindow sync error: unable to set topic offset in trie")
			}
		}
		if err1 != nil {
			return true, err1
		}

		if err := db.sync(false); err != nil {
			return true, err
		}
		if db.syncInfo.syncComplete {
			if err := timeRelease(timeID); err != nil {
				return false, err
			}
			if err := db.internal.mem.Free(timeID); err != nil {
				return true, err
			}
			db.internal.freeList.releaseLease(timeID)
		}

		return false, nil
	})
	if err != nil || err1 != nil {
		db.syncInfo.syncComplete = false
		db.abort()
	}

	return db.sync(false)
}

// expireEntries run expirer to delete entries from db if ttl was set on entries and that has expired.
func (db *DB) expireEntries() error {
	// sync happens synchronously.
	db.internal.syncLockC <- struct{}{}
	defer func() {
		<-db.internal.syncLockC
	}()
	expiredEntries := db.internal.timeWindow.expiryWindowBucket.getExpiredEntries(db.opts.queryOptions.defaultQueryLimit)
	for _, expiredEntry := range expiredEntries {
		we := expiredEntry.(_WinEntry)
		/// Test filter block if message hash presence.
		if !db.internal.filter.Test(we.seq()) {
			continue
		}
		r := newBlockReader(&db.internal.index)
		s, err := r.read(we.seq())
		if err != nil {
			return err
		}
		db.internal.freeList.free(s.seq, s.msgOffset, s.mSize())
		db.decount(1)
	}

	return nil
}
