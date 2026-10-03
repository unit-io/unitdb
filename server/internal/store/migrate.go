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

package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/log"
)

// A store written by v0.6.0 or before keeps the store's own records under
// fixed ids (namespaces.go). Open moves them to where they are kept now,
// before the node serves:
//
//   - the topic index, and for each topic in it the replicas of its messages;
//   - the ids of the replicated messages this node stored.
//
// The cluster moves the hints (Hint.Legacy), as only it can rewrite a hint,
// which holds its own id; and the security state is moved as it is loaded
// (Security.Legacy). Subscriptions are not moved: they are of connections,
// which a restart closes, and the clients and the other nodes (Resync)
// subscribe again; the old records are never read again.
//
// Each record is copied, the copies flushed to the store's log, and then the
// old record deleted. A crash in between leaves both, and the next Open moves
// what is left: a record whose copy is there already is deleted without being
// copied again. An entry of the old topic index is deleted once its topic's
// replicas have moved, so a crash leaves it to be moved again. The move runs
// whenever an old namespace holds records: at the first start of v0.7.0, and
// again if a rollback to v0.6.0 wrote some.

// moveBatch reads up to the store's most records at once.
const moveBatch = "?last=100000"

// errNotDeleted stops a move whose old records stay after they are deleted,
// which would never end.
var errNotDeleted = errors.New("store: an old record is still there after it was deleted")

// crashAfterCopy, set by tests, stops a move once records are copied and
// before the old ones are deleted, as a crash would, when it returns true.
var crashAfterCopy func() bool

// Flush waits for what was stored before it to be written to the store's
// log.
func Flush() error {
	return adp.Flush()
}

// newIDAt returns a new id for a record moved from the one stored under old,
// with the time of old, so that a query for the messages of a last duration
// finds it as before.
func newIDAt(old []byte) ([]byte, error) {
	id, err := adp.NewID()
	if err != nil {
		return nil, err
	}
	if len(old) >= 4 && len(id) >= 4 {
		copy(id[:4], old[:4])
	}
	return id, nil
}

// copyAs returns what a record moved to its new place is stored as, and its
// ttl, or keep false for a record not to move: one that expired.
type copyAs func(b []byte, now time.Time) (out []byte, ttl string, keep bool)

// moveRecords moves the records stored at from to to, and returns how many
// it copied.
func moveRecords(from, to place, as copyAs) (int, error) {
	moved := 0
	deleted := make(map[string]bool)
	for {
		ids, recs, err := adp.GetWithIDs(from.contract, from.topic+moveBatch)
		if err != nil {
			return moved, err
		}
		if len(ids) == 0 {
			return moved, nil
		}
		// The copies there already, of a move a crash interrupted.
		_, have, err := adp.GetWithIDs(to.contract, to.topic+moveBatch)
		if err != nil {
			return moved, err
		}
		there := make(map[string]int, len(have))
		for _, b := range have {
			there[string(b)]++
		}
		now := time.Now()
		// Oldest first, so that the copies keep their order.
		for i := len(recs) - 1; i >= 0; i-- {
			if deleted[string(ids[i])] {
				return moved, errNotDeleted
			}
			b, ttl, keep := as(recs[i], now)
			if !keep {
				continue
			}
			if there[string(b)] > 0 {
				there[string(b)]--
				continue
			}
			id, err := newIDAt(ids[i])
			if err != nil {
				return moved, err
			}
			if err := adp.PutWithID(to.contract, id, to.topic, b, ttl); err != nil {
				return moved, err
			}
			moved++
		}
		if err := adp.Flush(); err != nil {
			return moved, err
		}
		if crashAfterCopy != nil && crashAfterCopy() {
			return moved, errors.New("store: test crash")
		}
		for _, id := range ids {
			if err := adp.Delete(from.contract, id, from.topic); err != nil {
				return moved, err
			}
			deleted[string(id)] = true
		}
	}
}

// asReplica keeps a stored message as it is, until it expires.
func asReplica(b []byte, now time.Time) ([]byte, string, bool) {
	_, expiresAt, known := unwrap(b)
	if !known || expiresAt == 0 {
		return b, "", true
	}
	if expired(expiresAt, now) {
		return nil, "", false
	}
	return b, strconv.FormatInt(expiresAt-now.Unix(), 10), true
}

// asSeen keeps a replicated message's id for as long as the longest: the
// store does not say how long it had left.
func asSeen(b []byte, _ time.Time) ([]byte, string, bool) {
	return b, strconv.FormatInt(int64(maxSeenTTL/time.Second), 10), true
}

// migrate moves the records an older version stored under fixed ids, the
// topic index and the replicas of its topics, and the ids of replicated
// messages, to where they are kept now. Call it with the topic index loaded.
func migrate() error {
	topics, replicas, err := migrateIndex()
	if topics > 0 || replicas > 0 {
		log.ErrLogger.Info().Str("context", "store.migrate").Int("topics", topics).Int("replicas", replicas).Msg("moved the topic index and replicas of an older version to $sys topics")
	}
	if err != nil {
		return fmt.Errorf("store: moving the topic index and replicas of an older version: %w", err)
	}
	seen, err := moveRecords(place{legacySeenStoreId, legacySeenTopic}, place{sysContract, sysTopic(sysSeen, seenTopic)}, asSeen)
	if seen > 0 {
		log.ErrLogger.Info().Str("context", "store.migrate").Int("ids", seen).Msg("moved the replicated message ids of an older version to $sys topics")
	}
	if err != nil {
		return fmt.Errorf("store: moving the replicated message ids of an older version: %w", err)
	}
	return nil
}

// migrateIndex moves the old topic index, and the replicas of each topic in
// it, and returns how many topics and replicas it moved.
//
// The replicas of contract A's topic were kept under the topic in contract
// A XOR an id, which may be a contract B (finding 8): if B's own messages on
// the topic are kept there too, the topic of B is in the index, and the
// records can't be told apart. They are then left where they are, as they
// were read, rather than move B's messages to A; A's owner still has them.
func migrateIndex() (int, int, error) {
	from := place{legacyIndexStoreId, topicIndexTopic}
	done := make(map[TopicRef]bool)
	deleted := make(map[string]bool)
	// Every topic indexed: in the old index, and in the new one, which holds
	// the entries moved before a crash.
	listed := make(map[TopicRef]bool)
	_, all, err := adp.GetWithIDs(from.contract, from.topic+moveBatch)
	if err != nil {
		return 0, 0, err
	}
	for _, b := range all {
		if len(b) > 4 {
			listed[TopicRef{Contract: binary.LittleEndian.Uint32(b[:4]), Topic: string(b[4:])}] = true
		}
	}
	for _, ref := range (&MessageStore{}).Topics() {
		listed[ref] = true
	}
	topicsMoved, replicas := 0, 0
	for {
		ids, recs, err := adp.GetWithIDs(from.contract, from.topic+moveBatch)
		if err != nil {
			return topicsMoved, replicas, err
		}
		if len(ids) == 0 {
			return topicsMoved, replicas, nil
		}
		for i := len(recs) - 1; i >= 0; i-- {
			if deleted[string(ids[i])] {
				return topicsMoved, replicas, errNotDeleted
			}
			b := recs[i]
			if len(b) <= 4 {
				continue
			}
			ref := TopicRef{Contract: binary.LittleEndian.Uint32(b[:4]), Topic: string(b[4:])}
			if done[ref] {
				continue
			}
			shared := TopicRef{Contract: ref.Contract ^ legacyReplicaStoreId, Topic: ref.Topic}
			if listed[shared] {
				log.ErrLogger.Warn().Str("context", "store.migrate").Uint32("contract", ref.Contract).Uint32("shared_with", shared.Contract).Str("topic", ref.Topic).Msg("the replicas of a topic share a namespace with another contract's messages: left where they are")
			} else {
				n, err := moveRecords(
					place{shared.Contract, ref.Topic},
					place{ref.Contract, sysTopic(sysReplicas, ref.Topic)},
					asReplica)
				replicas += n
				if err != nil {
					return topicsMoved, replicas, err
				}
			}
			if err := addToIndex(ref.Contract, ref.Topic); err != nil {
				return topicsMoved, replicas, err
			}
			done[ref] = true
			topicsMoved++
		}
		if err := adp.Flush(); err != nil {
			return topicsMoved, replicas, err
		}
		for _, id := range ids {
			if err := adp.Delete(from.contract, id, from.topic); err != nil {
				return topicsMoved, replicas, err
			}
			deleted[string(id)] = true
		}
	}
}

// Legacy returns the hints a v0.6.0 node kept for node, and the id each is
// stored under, which is the one it holds. The caller stores each again with
// Put, under a new id, then calls Flush, and deletes it with DeleteLegacy.
func (h *HintStore) Legacy(node string) (ids, payloads [][]byte, err error) {
	return adp.GetWithIDs(legacyHintStoreId, legacyHintTopic(node)+moveBatch)
}

// DeleteLegacy deletes a hint a v0.6.0 node kept for node.
func (h *HintStore) DeleteLegacy(node string, id []byte) error {
	return adp.Delete(legacyHintStoreId, id, legacyHintTopic(node))
}
