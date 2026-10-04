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
	"sort"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/unitdb/memdb"
	"github.com/unit-io/unitdb/message"
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
		// counted is set once count is added to the DB's.
		counted bool
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

	return db.initSync()
}

// initSync prepares the window and block writers for a sync.
func (db *_SyncHandle) initSync() bool {
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

func (db *_SyncHandle) reset() error {
	db.syncInfo.lastSyncSeq = db.syncInfo.upperSeq
	db.syncInfo.count = 0
	db.syncInfo.counted = false
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

	// Uncount the entries only if they were counted: a sync that fails
	// before writing them counts none.
	if db.syncInfo.counted {
		db.decount(uint64(db.syncInfo.count))
	}

	return nil
}

// startSyncer syncs every interval until the DB closes. A sync that fails
// is logged and tried again: it panicked, taking the process down, and a
// tick racing Close fails with errClosed. Close waits for the syncer: it
// was counted done as it started.
func (db *DB) startSyncer(interval time.Duration) {
	db.internal.closeW.Add(1)
	syncTicker := time.NewTicker(interval)
	go func() {
		defer db.internal.closeW.Done()
		defer syncTicker.Stop()
		for {
			select {
			case <-db.internal.closeC:
				return
			case <-syncTicker.C:
				if err := db.Sync(); errors.Is(err, errClosed) {
					return
				} else if err != nil {
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
		return err
	}

	return nil
}

// testHookBeforeDecount, if set, runs once a delete has written its
// tombstone and before it writes the count without the entry.
var testHookBeforeDecount func()

// testHookBeforeCount, if set, runs once a sync has written its entries and
// before it writes their count: the crash tests stop the process there.
var testHookBeforeCount func()

// sync writes the entries appended since the last sync, of the memdb block
// timeID (0 for none), and counts them.
func (db *_SyncHandle) sync(recovery bool, timeID int64) error {
	if db.syncInfo.upperSeq == 0 {
		return nil
	}
	db.syncInfo.syncComplete = false
	defer db.abort()

	// Record the block before writing its entries: if the process stops
	// before the count below is written, the recovery counts the entries of
	// this block it finds already written.
	if timeID != 0 && db.syncInfo.count > 0 {
		db.internal.dbInfo.syncing = timeID
		if err := db.writeInfo(); err != nil {
			return err
		}
		if err := db.internal.info.Sync(); err != nil {
			return err
		}
	}

	if _, err := db.blockWriter.extend(db.syncInfo.upperSeq); err != nil {
		logger.Error().Err(err).Str("context", "db.extendBlocks")
		return err
	}
	if err := db.windowWriter.write(); err != nil {
		logger.Error().Err(err).Str("context", "timeWindow.write")
		return err
	}
	// Write the filter first so it covers every entry in the index.
	if err := db.internal.filter.write(); err != nil {
		logger.Error().Err(err).Str("context", "filter.write")
		return err
	}
	if err := db.blockWriter.write(); err != nil {
		logger.Error().Err(err).Str("context", "block.write")
		return err
	}

	// The entries reach the disk before the count that includes them.
	if err := db.fs.sync(); err != nil {
		return err
	}
	if testHookBeforeCount != nil {
		testHookBeforeCount()
	}
	db.incount(uint64(db.syncInfo.count))
	db.syncInfo.counted = true
	db.internal.dbInfo.syncing = 0
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
	timeRelease := db.internal.timeWindow.release()
	pending := make(map[uint64]_WindowEntries)
	err := db.internal.mem.BlockIterator(func(timeID int64, seqs []uint64) (bool, error) {
		return db.syncBlock(timeID, seqs, false, pending, timeRelease)
	})
	if err == nil {
		err = db.writePending(pending)
	}
	if err != nil {
		db.syncInfo.syncComplete = false
		db.abort()
	}

	return db.sync(false, 0)
}

// expireEntries run expirer to delete entries from db if ttl was set on entries and that has expired.
func (db *DB) expireEntries() error {
	// sync happens synchronously.
	db.internal.syncLockC <- struct{}{}
	defer func() {
		<-db.internal.syncLockC
	}()
	expiredEntries := db.internal.timeWindow.expiryWindowBucket.getExpiredEntries(db.opts.queryOptions.defaultQueryLimit)
	expired := false
	for _, expiredEntry := range expiredEntries {
		we := expiredEntry.(_WinEntry)
		/// Test filter block if message hash presence.
		if !db.internal.filter.Test(we.seq()) {
			continue
		}
		e, err := db.internal.reader.readEntry(we.seq())
		if err == errMsgIDDeleted {
			continue
		}
		if err != nil {
			return err
		}
		db.internal.freeList.free(e.seq, e.msgOffset, e.mSize())
		db.decount(1)
		expired = true
	}
	if expired {
		// Persist the count so it is right after a crash.
		return db.writeInfo()
	}

	return nil
}

// syncBlock writes the entries seqs of the memdb block timeID to disk, and
// frees the block once they are. Sync and recovery both write blocks with
// it: recovery kept a copy of it, which differed.
//
// The window entries of a topic not in the trie are held in pending, for
// writePending once a later block names the topic: in recovery, a topic is
// named by its first entry, which may be in a later block. A recovered
// entry naming its topic adds it to the trie, and its sequence is the DB's
// at least.
func (db *_SyncHandle) syncBlock(timeID int64, seqs []uint64, recovery bool, pending map[uint64]_WindowEntries, timeRelease func(int64) error) (bool, error) {
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	if seqs[len(seqs)-1] > db.syncInfo.upperSeq {
		db.syncInfo.upperSeq = seqs[len(seqs)-1]
	}
	// New entries must not reuse the sequences of recovered ones.
	db.advanceSeq(seqs[len(seqs)-1])
	// The block a sync was writing when the process stopped: the entries
	// of it already written are not in the count kept, if Open didn't
	// recount (deriveFromIndex).
	countWritten := recovery && !db.internal.recounted && timeID == db.internal.dbInfo.syncing

	winEntries := make(map[uint64]_WindowEntries)
	var err1 error
	for _, seq := range seqs {
		memdata, err := db.internal.mem.Lookup(timeID, seq)
		if errors.Is(err, memdb.ErrNotFound) {
			// Deleted since the block's keys were listed. It failed the
			// sync, which then uncounted entries it had not counted.
			continue
		}
		if err != nil || memdata == nil {
			db.syncInfo.entriesInvalid++
			logger.Error().Err(err).Str("context", "mem.Lookup")
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
				if countWritten && m.valueSize != 0 {
					db.syncInfo.count++
				}
				continue
			}
			return true, err
		}
		if m.topicSize != 0 {
			rawtopic, err := db.internal.reader.readTopic(e)
			if err != nil {
				return true, err
			}
			t := new(message.Topic)
			if err := t.Unmarshal(rawtopic); err != nil {
				return true, err
			}
			db.addTopic(m.topicHash, t.Parts, t.Depth)
		}
		winEntries[m.topicHash] = append(winEntries[m.topicHash], newWinEntry(seq, m.expiresAt))
		db.internal.filter.Append(seq)
		// A tombstone (see delete) is written for its topic, and is not
		// counted.
		if m.valueSize != 0 {
			db.syncInfo.count++
		}
		db.syncInfo.inBytes += int64(e.valueSize)
	}
	if err1 != nil {
		return true, err1
	}

	for h, wEntries := range winEntries {
		if _, ok := db.internal.trie.getOffset(h); !ok {
			pending[h] = append(pending[h], wEntries...)
			delete(winEntries, h)
		}
	}
	if err := db.writePending(winEntries); err != nil {
		return true, err
	}

	if err := db.sync(recovery, timeID); err != nil {
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
}
