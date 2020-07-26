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

func (db *syncHandle) recoverWindowBlocks() error {
	err := db.timeWindow.foreachTimeWindow(true, func(windowEntries map[uint64]windowEntries) (bool, error) {
		for h, wEntries := range windowEntries {
			topicOff, ok := db.trie.getOffset(h)
			if !ok {
				return true, errors.New(fmt.Sprintf("recovery.recoverWindowBlocks: timeWindow sync error, unable to get topic offset from trie %d", h))
			}
			wOff, err := db.windowWriter.append(h, topicOff, wEntries)
			if err != nil {
				return true, err
			}
			if ok := db.trie.setOffset(topic{hash: h, offset: wOff}); !ok {
				return true, errors.New("recovery.recoverWindowBlocks: timeWindow sync error, unable to set topic offset in trie")
			}
		}
		return false, nil
	})
	return err
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
	var logSeq uint64
	topics := make(map[uint64]*message.Topic) // map[topicHash]*message.Topic
	r, err := db.wal.NewReader()
	if err != nil {
		return err
	}
	err = r.Read(func(lSeq uint64, last bool) (ok bool, err error) {
		l := r.Count()
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
			s := slot{
				seq:       e.seq,
				topicSize: e.topicSize,
				valueSize: e.valueSize,

				cacheBlock: logData[entrySize:],
			}
			// off := s.mSize()
			// s.cacheBlock = make([]byte, s.mSize())
			// copy(s.cacheBlock, data[:off])
			if s.msgOffset, err = db.dataWriter.append(s.cacheBlock); err != nil {
				return true, err
			}
			exists, err := db.blockWriter.append(s, db.startBlockIdx)
			if err != nil {
				return true, err
			}
			if exists {
				continue
			}
			if _, ok := topics[e.topicHash]; !ok && e.topicSize != 0 {
				rawtopic, _ := db.dataWriter.readTopic(s)

				t := new(message.Topic)
				if err := t.Unmarshal(rawtopic); err != nil {
					return true, err
				}
				db.trie.add(topic{hash: e.topicHash}, t.Parts, t.Depth)
				topics[e.topicHash] = t
			}
			db.timeWindow.add(e.topicHash, winEntry{seq: e.seq, expiresAt: e.expiresAt})
			db.filter.Append(e.seq)
			db.internal.count++
			db.internal.inBytes += int64(e.valueSize)
		}

		if err := db.recoverWindowBlocks(); err != nil {
			logger.Error().Err(err).Str("context", "db.recoverWindowBlocks")
			return true, err
		}

		if err := db.sync(true, false); err != nil {
			return true, err
		}
		logSeq = lSeq
		return false, nil
	})
	if err != nil {
		db.syncComplete = false
		db.abort()
		return err
	}
	if err := db.sync(true, true); err != nil {
		return err
	}
	if err := db.wal.SignalLogApplied(logSeq); err != nil {
		logger.Error().Err(err).Str("context", "wal.SignalLogApplied")
		return err
	}
	return nil
}

func (db *DB) recoverLog() error {
	// Sync happens synchronously
	db.syncLockC <- struct{}{}
	defer func() {
		<-db.syncLockC
	}()

	syncHandle := syncHandle{DB: db, internal: internal{}}
	return syncHandle.startRecovery()
}
