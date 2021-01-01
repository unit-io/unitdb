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
	"sort"
	"time"

	"github.com/unit-io/bpool"
)

type (
	_SyncInfo struct {
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

		rawWindow *bpool.Buffer
		rawBlock  *bpool.Buffer
	}
)

func (db *_SyncHandle) startSync() bool {
	if db.syncInfo.lastSyncSeq == db.seq() {
		db.syncInfo.syncStatusOk = false
		return db.syncInfo.syncStatusOk
	}

	db.rawWindow = db.internal.bufPool.Get()
	db.rawBlock = db.internal.bufPool.Get()

	var err error
	db.windowWriter, err = newWindowWriter(db.fs, db.rawWindow)
	if err != nil {
		logger.Error().Err(err).Str("context", "startSync").Msg("Error syncing to db")
		return false
	}
	db.blockWriter, err = newBlockWriter(db.fs, db.internal.freeList, db.rawBlock)
	if err != nil {
		logger.Error().Err(err).Str("context", "startSync").Msg("Error syncing to db")
		return false
	}
	db.syncInfo.syncStatusOk = true

	return db.syncInfo.syncStatusOk
}

func (db *_SyncHandle) finish() error {
	if !db.syncInfo.syncStatusOk {
		return nil
	}

	db.internal.bufPool.Put(db.rawWindow)
	db.internal.bufPool.Put(db.rawBlock)

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

	if err := db.windowWriter.reset(); err != nil {
		return err
	}
	if err := db.blockWriter.reset(); err != nil {
		return err
	}

	return nil
}

func (db *_SyncHandle) abort() error {
	defer db.reset()
	if db.syncInfo.syncComplete {
		return nil
	}
	// Rollback.
	if err := db.windowWriter.abort(); err != nil {
		return err
	}

	if err := db.blockWriter.abort(); err != nil {
		return err
	}

	db.decount(uint64(db.syncInfo.count))

	return nil
}

func (db *DB) startSyncer(interval time.Duration) {
	db.internal.closeW.Add(1)
	defer db.internal.closeW.Done()
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
					panic(err)
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

	if _, err := db.blockWriter.extend(db.syncInfo.upperSeq); err != nil {
		logger.Error().Err(err).Str("context", "db.extendBlocks")
		return err
	}
	if err := db.windowWriter.write(); err != nil {
		logger.Error().Err(err).Str("context", "timeWindow.write")
		return err
	}
	if err := db.blockWriter.write(); err != nil {
		logger.Error().Err(err).Str("context", "block.write")
		return err
	}

	db.incount(uint64(db.syncInfo.count))
	if err := db.DB.sync(); err != nil {
		return err
	}
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
	timeRelease := db.internal.timeWindow.release()
	err := db.internal.mem.ForEachBlock(func(timeID int64, seqs []uint64) (bool, error) {
		winEntries := make(map[uint64]_WindowEntries)
		sort.Slice(seqs[:], func(i, j int) bool {
			return seqs[i] < seqs[j]
		})
		if seqs[len(seqs)-1] > db.syncInfo.upperSeq {
			db.syncInfo.upperSeq = seqs[len(seqs)-1]
		}
		for _, seq := range seqs {
			memdata, err := db.internal.mem.Lookup(timeID, seq)
			if err != nil || memdata == nil {
				db.syncInfo.entriesInvalid++
				logger.Error().Err(err).Str("context", "mem.Get")
				err1 = err
				continue
			}
			var m _Entry
			if err = m.UnmarshalBinary(memdata[:entrySize]); err != nil {
				db.syncInfo.entriesInvalid++
				err1 = err
				continue
			}
			e := _IndexEntry{
				seq:       m.seq,
				topicSize: m.topicSize,
				valueSize: m.valueSize,

				cache: memdata[entrySize:],
			}
			if err := db.blockWriter.append(e); err != nil {
				if err == errEntryExist {
					continue
				}
				return true, err
			}

			we := newWinEntry(seq, m.expiresAt)
			if _, ok := winEntries[m.topicHash]; ok {
				winEntries[m.topicHash] = append(winEntries[m.topicHash], we)
			} else {
				winEntries[m.topicHash] = _WindowEntries{we}
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
			fmt.Println("db.sync: error ", err1)
			return true, err1
		}

		if err := db.sync(false); err != nil {
			fmt.Println("db.sync: sync error ", err)
			return true, err
		}
		if db.syncInfo.syncComplete {
			if err := timeRelease(timeID); err != nil {
				return false, err
			}
			if err := db.internal.mem.Free(timeID); err != nil {
				return true, err
			}
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
		e, err := db.internal.reader.readEntry(we.seq())
		if err != nil {
			return err
		}
		db.internal.freeList.free(e.seq, e.msgOffset, e.mSize())
		db.decount(1)
	}

	return nil
}
