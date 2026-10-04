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
)

// writePending writes window entries of topics, held back until the trie
// named them; the entries of a topic it doesn't name yet are skipped.
func (db *_SyncHandle) writePending(windowEntries map[uint64]_WindowEntries) error {
	for h, wEntries := range windowEntries {
		topicOff, ok := db.internal.trie.getOffset(h)
		if !ok {
			// A topic no entry on disk names: its first entry was deleted
			// before a sync, which older versions allowed. Its entries can't
			// be queried; skip them rather than fail to open.
			logger.Error().Uint64("topic", h).Int("entries", len(wEntries)).Str("context", "db.writePending").Msg("skipped the entries of a topic whose name was lost")
			continue
		}
		// sync writes nothing past its upper sequence, which each block's
		// sync resets.
		for _, we := range wEntries {
			if we.seq() > db.syncInfo.upperSeq {
				db.syncInfo.upperSeq = we.seq()
			}
		}
		wOff, err := db.windowWriter.append(h, topicOff, wEntries)
		if err != nil {
			return err
		}
		if ok := db.internal.trie.setOffset(_Topic{hash: h, offset: wOff}); !ok {
			return errors.New("db.writePending: unable to set topic offset in trie")
		}
	}
	return nil
}

func (db *_SyncHandle) startRecovery() error {
	db.internal.closeW.Add(1)
	defer func() {
		db.internal.closeW.Done()
	}()
	fmt.Println("db.recoverLog: start recovery")
	// Don't use startSync: it skips when the stored sequence has not moved
	// since the last sync, but after a crash the stored sequence is stale and
	// the WAL may hold entries past it.
	if ok := db.initSync(); !ok {
		return nil
	}
	defer func() {
		db.finish()
	}()

	// The blocks are written as a sync writes them; nothing is in the time
	// window to release.
	noRelease := func(int64) error { return nil }
	pending := make(map[uint64]_WindowEntries)
	err := db.internal.mem.All(func(timeID int64, seqs []uint64) (bool, error) {
		return db.syncBlock(timeID, seqs, true, pending, noRelease)
	})
	if err == nil {
		err = db.writePending(pending)
	}
	if err != nil {
		db.syncInfo.syncComplete = false
		db.abort()
		return err
	}

	return db.sync(true, 0)
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
