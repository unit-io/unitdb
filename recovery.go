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
	"time"

	"github.com/unit-io/unitdb/message"
	// _ "net/http/pprof"
)

func (db *_SyncHandle) recoverWindowBlocks(windowEntries map[uint64]_WindowEntries) error {
	for h, wEntries := range windowEntries {
		topicOff, ok := db.internal.trie.getOffset(h)
		if !ok {
			return errors.New(fmt.Sprintf("recovery.recoverWindowBlocks: timeWindow sync error, unable to get topic offset from trie %d", h))
		}
		wOff, err := db.windowWriter.append(h, topicOff, wEntries)
		if err != nil {
			return err
		}
		if ok := db.internal.trie.setOffset(_Topic{hash: h, offset: wOff}); !ok {
			return errors.New("recovery.recoverWindowBlocks: timeWindow sync error, unable to set topic offset in trie")
		}
	}
	return nil
}

func (db *_SyncHandle) startRecovery() error {
	// p := profile.Start(profile.MemProfile, profile.ProfilePath("."), profile.NoShutdownHook)
	// defer p.Stop()
	db.internal.closeW.Add(1)
	defer func() {
		db.internal.closeW.Done()
	}()
	fmt.Println("db.recoverLog: start recovery")
	if ok := db.startSync(); !ok {
		return nil
	}
	defer func() {
		db.finish()
	}()

	var err1 error
	pendingEntries := make(map[uint64]_WindowEntries)
	dataFile, err := db.fs.getFile(FileDesc{Type: TypeData})
	if err != nil {
		return err
	}
	data := newDataReader(db.internal.data, dataFile)

	err = db.internal.mem.ForEachBlock(db.opts.syncDurationType*time.Duration(db.opts.maxSyncDurations), func(timeID int64, seqs []uint64) (bool, error) {
		winEntries := make(map[uint64]_WindowEntries)
		for _, seq := range seqs {
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
			if e.seq > db.syncInfo.upperSeq {
				db.syncInfo.upperSeq = e.seq
			}
			s := _Slot{
				seq:       e.seq,
				topicSize: e.topicSize,
				valueSize: e.valueSize,

				cache: memdata[entrySize:],
			}
			if s.msgOffset, err = db.dataWriter.append(s.cache); err != nil {
				return false, err
			}
			exists, err := db.blockWriter.append(s, db.syncInfo.startBlockIdx)
			if err != nil {
				return false, err
			}
			if exists {
				db.internal.freeList.free(s.seq, s.msgOffset, s.mSize())
				continue
			}
			if e.topicSize != 0 {
				rawtopic, _ := data.readTopic(s)

				t := new(message.Topic)
				if err := t.Unmarshal(rawtopic); err != nil {
					return false, err
				}
				db.internal.trie.add(newTopic(e.topicHash, 0), t.Parts, t.Depth)
			}
			if _, ok := winEntries[e.topicHash]; ok {
				winEntries[e.topicHash] = append(winEntries[e.topicHash], newWinEntry(e.seq, e.expiresAt))
			} else {
				winEntries[e.topicHash] = _WindowEntries{newWinEntry(e.seq, e.expiresAt)}
			}
			db.internal.filter.Append(e.seq)
			db.syncInfo.count++
			db.syncInfo.inBytes += int64(e.valueSize)
		}
		if err1 != nil {
			return true, err1
		}

		for h := range winEntries {
			_, ok := db.internal.trie.getOffset(h)
			if !ok {
				if _, ok := pendingEntries[h]; ok {
					pendingEntries[h] = append(pendingEntries[h], winEntries[h]...)
				} else {
					pendingEntries[h] = winEntries[h]
				}
				delete(winEntries, h)
			}
		}
		if err := db.recoverWindowBlocks(winEntries); err != nil {
			logger.Error().Err(err).Str("context", "db.recoverWindowBlocks")
			return true, err
		}

		if err := db.sync(true); err != nil {
			return true, err
		}
		if db.syncInfo.syncComplete {
			if err := db.internal.mem.Free(timeID); err != nil {
				return true, err
			}
		}

		return false, nil
	})

	if err != nil || err1 != nil {
		db.syncInfo.syncComplete = false
		db.abort()
		return err1
	}

	if err := db.recoverWindowBlocks(pendingEntries); err != nil {
		logger.Error().Err(err).Str("context", "db.recoverWindowBlocks")
		return err
	}

	return db.sync(true)
}

func (db *DB) recoverLog() error {
	// Sync happens synchronously.
	db.internal.syncLockC <- struct{}{}
	defer func() {
		<-db.internal.syncLockC
	}()

	syncHandle := _SyncHandle{DB: db}
	if err := syncHandle.startRecovery(); err != nil {
		return err
	}

	return nil
}
