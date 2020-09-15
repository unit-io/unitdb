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

	// _ "net/http/pprof"

	"github.com/unit-io/unitdb/message"
)

func (db *syncHandle) recoverWindowBlocks(windowEntries map[uint64]windowEntries) error {
	for h, wEntries := range windowEntries {
		topicOff, ok := db.trie.getOffset(h)
		if !ok {
			return errors.New(fmt.Sprintf("recovery.recoverWindowBlocks: timeWindow sync error, unable to get topic offset from trie %d", h))
		}
		wOff, err := db.windowWriter.append(h, topicOff, wEntries)
		if err != nil {
			return err
		}
		if ok := db.trie.setOffset(topic{hash: h, offset: wOff}); !ok {
			return errors.New("recovery.recoverWindowBlocks: timeWindow sync error, unable to set topic offset in trie")
		}
	}
	return nil
}

func (db *syncHandle) startRecovery() error {
	// p := profile.Start(profile.MemProfile, profile.ProfilePath("."), profile.NoShutdownHook)
	// defer p.Stop()
	db.closeW.Add(1)
	defer func() {
		db.closeW.Done()
	}()
	fmt.Println("db.recoverLog: start recovery")

	if ok := db.startSync(); !ok {
		return nil
	}
	defer func() {
		db.finish()
	}()

	var e entry
	topics := make(map[uint64]*message.Topic) // map[topicHash]*message.Topic
	r, err := db.wal.NewReader()
	if err != nil {
		return err
	}
	err = r.Read(func() (ok bool, err error) {
		l := r.Count()
		winEntries := make(map[uint64]windowEntries)
		for i := uint32(0); i < l; i++ {
			logData, ok, err := r.Next()
			if err != nil {
				return false, err
			}
			if !ok {
				break
			}
			if err := e.UnmarshalBinary(logData[:entrySize]); err != nil {
				return true, err
			}
			if db.freeList.isFree(e.seq) {
				// If seq is present in free list it mean it was deleted but not get released from the WAL.
				continue
			}
			if e.seq > db.internal.upperSeq {
				db.internal.upperSeq = e.seq
			}
			s := slot{
				seq:       e.seq,
				topicSize: e.topicSize,
				valueSize: e.valueSize,

				cacheBlock: logData[entrySize:],
			}
			if s.msgOffset, err = db.dataWriter.append(s.cacheBlock); err != nil {
				return true, err
			}
			exists, err := db.blockWriter.append(s, db.startBlockIdx)
			if err != nil {
				return true, err
			}
			if exists {
				db.freeList.free(s.seq, s.msgOffset, s.mSize())
				continue
			}
			if _, ok := topics[e.topicHash]; !ok && e.topicSize != 0 {
				rawtopic, _ := db.dataWriter.readTopic(s)

				t := new(message.Topic)
				if err := t.Unmarshal(rawtopic); err != nil {
					return true, err
				}
				db.trie.add(newTopic(e.topicHash, 0), t.Parts, t.Depth)
				topics[e.topicHash] = t
			}
			if _, ok := winEntries[e.topicHash]; ok {
				winEntries[e.topicHash] = append(winEntries[e.topicHash], newWinEntry(e.seq, e.expiresAt))
			} else {
				winEntries[e.topicHash] = windowEntries{newWinEntry(e.seq, e.expiresAt)}
			}
			db.filter.Append(e.seq)
			db.internal.count++
			db.internal.inBytes += int64(e.valueSize)
		}

		if err := db.recoverWindowBlocks(winEntries); err != nil {
			logger.Error().Err(err).Str("context", "db.recoverWindowBlocks")
			return true, err
		}

		if err := db.sync(true); err != nil {
			return true, err
		}
		return false, nil
	})
	if err != nil {
		fmt.Println("db.Sync: error ", err)
		db.syncComplete = false
		db.abort()
		return err
	}
	return db.sync(true)
}

func (db *DB) recoverLog() error {
	// Sync happens synchronously.
	db.syncLockC <- struct{}{}
	defer func() {
		<-db.syncLockC
	}()

	syncHandle := syncHandle{internal: internal{DB: db}}
	if err := syncHandle.startRecovery(); err != nil {
		return err
	}

	// reset log on successful recovery.
	return db.wal.Reset()
}
