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
	"fmt"
	"sync/atomic"
)

// Verify checks that the state the DB keeps besides its entries agrees with
// them, and returns the first disagreement found:
//
//   - every block and stored message passes its checksum;
//   - each index entry is in the block of its sequence, once, and at or below
//     the DB's sequence, so a new entry can't reuse it;
//   - the filter holds every entry in the index, or reads would miss it;
//   - Count is the number of entries in the index not deleted (unless
//     background expiry is on: it uncounts expired entries it leaves there);
//   - each topic's window blocks link to older blocks of the topic, the trie
//     knows the topic and points at its newest block, and every entry in the
//     index not deleted is in a window block, or no query finds it;
//   - every topic in the trie is named in the topics file;
//   - the memdb passes its own Verify.
//
// It waits for a sync in progress, and holds off the next one. The tests run
// it after operations; it reads every block, so it is slow on a large DB.
func (db *DB) Verify() error {
	if err := db.ok(); err != nil {
		return err
	}
	select {
	case db.internal.syncLockC <- struct{}{}:
	case <-db.internal.closeC:
		return errClosed
	}
	defer func() {
		<-db.internal.syncLockC
	}()

	if err := db.verifyFiles(); err != nil {
		return err
	}
	winFile, indexFile, _, _, err := db.files()
	if err != nil {
		return err
	}

	seq := db.seq()
	live := make(map[uint64]bool)
	if err := forEachBlock(indexFile, func(off int64, buf []byte) error {
		var b _IndexBlock
		if err := b.unmarshalBinary(buf); err != nil {
			return err
		}
		idx := int32(off / int64(blockSize))
		seen := make(map[uint64]bool)
		for _, e := range b.entries {
			if e.seq == 0 {
				continue
			}
			switch {
			case blockIndex(e.seq) != idx:
				return fmt.Errorf("unitdb: entry %d is in index block %d; its block is %d", e.seq, idx, blockIndex(e.seq))
			case seen[e.seq]:
				return fmt.Errorf("unitdb: entry %d is twice in index block %d", e.seq, idx)
			case e.seq > seq:
				return fmt.Errorf("unitdb: entry %d is past the DB's sequence %d", e.seq, seq)
			case !db.internal.filter.Test(e.seq):
				return fmt.Errorf("unitdb: the filter misses entry %d", e.seq)
			}
			seen[e.seq] = true
			if !e.deleted() {
				live[e.seq] = true
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if count := atomic.LoadUint64(&db.internal.dbInfo.count); !db.opts.flags.backgroundKeyExpiry && count != uint64(len(live)) {
		return fmt.Errorf("unitdb: Count is %d; the index holds %d entries", count, len(live))
	}

	heads := make(map[uint64]int64)
	if err := forEachBlock(winFile, func(off int64, buf []byte) error {
		var b _WinBlock
		if err := b.unmarshalBinary(buf); err != nil {
			return err
		}
		if b.entryIdx == 0 {
			return nil
		}
		if int(b.entryIdx) > entriesPerWindowBlock {
			return fmt.Errorf("unitdb: window block at %d holds %d entries", off, b.entryIdx)
		}
		if b.next != 0 {
			prev, err := db.readWinBlock(winFile, b.next)
			if err != nil {
				return fmt.Errorf("unitdb: window block at %d links to %d: %v", off, b.next, err)
			}
			if b.next >= off || prev.topicHash != b.topicHash {
				return fmt.Errorf("unitdb: window block at %d of topic %d links to the block at %d, of topic %d", off, b.topicHash, b.next, prev.topicHash)
			}
		}
		heads[b.topicHash] = off
		for _, we := range b.entries[:b.entryIdx] {
			if we.seq() == 0 || we.seq() > seq {
				return fmt.Errorf("unitdb: window block at %d holds entry %d; the DB's sequence is %d", off, we.seq(), seq)
			}
			delete(live, we.seq())
		}
		return nil
	}); err != nil {
		return err
	}
	for h, off := range heads {
		got, ok := db.internal.trie.getOffset(h)
		if !ok {
			return fmt.Errorf("unitdb: topic %d has window blocks and no name in the trie", h)
		}
		if got != off {
			return fmt.Errorf("unitdb: the trie points topic %d at window block %d; its newest is at %d", h, got, off)
		}
	}
	for s := range live {
		return fmt.Errorf("unitdb: entry %d is in the index and in no window block", s)
	}
	for _, h := range db.internal.trie.hashes() {
		if _, ok := db.internal.topics.get(h); !ok {
			return fmt.Errorf("unitdb: topic %d is in the trie and not named in the topics file", h)
		}
	}

	return db.internal.mem.Verify()
}
