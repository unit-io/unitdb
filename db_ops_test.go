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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/unit-io/unitdb/filter"
	"github.com/unit-io/unitdb/message"
)

// smallOpts keeps buffer pools small so tests don't reserve GBs of memory.
func smallOpts() []Options {
	return []Options{WithBufferSize(1 << 16), WithMemdbSize(1 << 16), WithFreeBlockSize(1 << 16)}
}

// openTestDB opens a DB in a fresh temp dir and closes it on test cleanup.
func openTestDB(t *testing.T, opts ...Options) (*DB, string) {
	t.Helper()
	dir := t.TempDir()
	db, err := Open(dir, append(smallOpts(), opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db, dir
}

func reopenTestDB(t *testing.T, dir string, opts ...Options) *DB {
	t.Helper()
	db, err := Open(dir, append(smallOpts(), opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

// syncDB waits for memdb to rotate to a new time block, then syncs.
// memdb only hands blocks older than the current one to Sync, and a block
// lasts one second by default, so a Sync straight after Put writes nothing.
func syncDB(t *testing.T, db *DB) {
	t.Helper()
	next := time.Now().Truncate(time.Second).Add(time.Second + 100*time.Millisecond)
	time.Sleep(time.Until(next))
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
}

func testMsg(i int) []byte {
	return []byte(fmt.Sprintf("msg.%d", i))
}

// putMsgs puts n messages to topic and returns their IDs in put order.
func putMsgs(t *testing.T, db *DB, topic []byte, contract uint32, n int) [][]byte {
	t.Helper()
	var ids [][]byte
	for i := 0; i < n; i++ {
		id := db.NewID()
		if err := db.PutEntry(NewEntry(topic, testMsg(i)).WithID(id).WithContract(contract)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

// newestFirst returns the expected Get result for messages 0..n-1.
func newestFirst(n int) [][]byte {
	var msgs [][]byte
	for i := n - 1; i >= 0; i-- {
		msgs = append(msgs, testMsg(i))
	}
	return msgs
}

func get(t *testing.T, db *DB, q *Query) [][]byte {
	t.Helper()
	items, err := db.Get(q)
	if err != nil {
		t.Fatal(err)
	}
	return items
}

func assertMsgs(t *testing.T, want, got [][]byte) {
	t.Helper()
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("expected %d messages %q; got %d messages %q", len(want), want, len(got), got)
	}
}

func TestOpenLocked(t *testing.T) {
	_, dir := openTestDB(t)

	if _, err := Open(dir, smallOpts()...); err != errLocked {
		t.Fatalf("expected %v; got %v", errLocked, err)
	}
}

func TestPutGet(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.putget")

	for i := 0; i < 10; i++ {
		if err := db.Put(topic, testMsg(i)); err != nil {
			t.Fatal(err)
		}
	}

	assertMsgs(t, newestFirst(10), get(t, db, NewQuery(topic)))
	if count := db.Count(); count != 0 {
		// Count reflects entries synced to disk, not entries in memdb.
		t.Logf("count before sync: %d", count)
	}
}

func TestGetUnknownTopic(t *testing.T) {
	db, _ := openTestDB(t)
	putMsgs(t, db, []byte("unit.ops.known"), 0, 3)

	if items := get(t, db, NewQuery([]byte("unit.ops.unknown"))); len(items) != 0 {
		t.Fatalf("expected no messages; got %q", items)
	}
}

func TestPutEntryValidation(t *testing.T) {
	db, _ := openTestDB(t)
	longTopic := bytes.Repeat([]byte("a"), maxTopicLength+1)

	tests := []struct {
		name    string
		entry   *Entry
		wantErr error
	}{
		{"empty topic", NewEntry(nil, []byte("v")), errTopicEmpty},
		{"topic too large", NewEntry(longTopic, []byte("v")), errTopicTooLarge},
		{"empty payload", NewEntry([]byte("unit.ops"), nil), errValueEmpty},
		{"options only", NewEntry([]byte("?"), []byte("v")), errBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := db.PutEntry(tt.entry); err != tt.wantErr {
				t.Fatalf("expected %v; got %v", tt.wantErr, err)
			}
		})
	}
}

func TestGetValidation(t *testing.T) {
	db, _ := openTestDB(t)
	longTopic := bytes.Repeat([]byte("a"), maxTopicLength+1)

	tests := []struct {
		name    string
		topic   []byte
		wantErr error
	}{
		{"empty topic", nil, errTopicEmpty},
		{"topic too large", longTopic, errTopicTooLarge},
		{"options only", []byte("?"), errBadRequest},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := db.Get(NewQuery(tt.topic)); err != tt.wantErr {
				t.Fatalf("expected %v; got %v", tt.wantErr, err)
			}
		})
	}
}

func TestContractIsolation(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.contract")

	c1, _ := db.NewContract()
	c2 := c1 + 1
	putMsgs(t, db, topic, c1, 3)

	assertMsgs(t, newestFirst(3), get(t, db, NewQuery(topic).WithContract(c1)))
	if items := get(t, db, NewQuery(topic).WithContract(c2)); len(items) != 0 {
		t.Fatalf("other contract: expected no messages; got %q", items)
	}
	if items := get(t, db, NewQuery(topic)); len(items) != 0 {
		t.Fatalf("master contract: expected no messages; got %q", items)
	}
}

func TestQueryLimit(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.limit")
	putMsgs(t, db, topic, 0, 20)

	assertMsgs(t, newestFirst(20)[:5], get(t, db, NewQuery(topic).WithLimit(5)))
	// ?last=N limits to the N most recent messages.
	assertMsgs(t, newestFirst(20)[:3], get(t, db, NewQuery(append(topic, []byte("?last=3")...))))
}

func TestDefaultQueryLimit(t *testing.T) {
	db, _ := openTestDB(t, WithDefaultQueryLimit(4))
	topic := []byte("unit.ops.defaultlimit")
	putMsgs(t, db, topic, 0, 10)

	assertMsgs(t, newestFirst(10)[:4], get(t, db, NewQuery(topic)))
}

func TestMaxQueryLimit(t *testing.T) {
	db, _ := openTestDB(t, WithMaxQueryLimit(6))
	topic := []byte("unit.ops.maxlimit")
	putMsgs(t, db, topic, 0, 10)

	// A ?last query asking for more than the max is capped.
	assertMsgs(t, newestFirst(10)[:6], get(t, db, NewQuery(append(topic, []byte("?last=1h")...)).WithLimit(100)))
}

func TestQueryWithLast(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.withlast")
	putMsgs(t, db, topic, 0, 5)

	assertMsgs(t, newestFirst(5), get(t, db, NewQuery(topic).WithLast("1h")))

	// An invalid duration leaves the query unchanged.
	q := NewQuery(topic).WithLast("not-a-duration")
	if q.Limit != 0 || q.internal.cutoff != 0 {
		t.Fatalf("expected unchanged query; got limit=%d cutoff=%d", q.Limit, q.internal.cutoff)
	}
}

func TestDeleteImmutable(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.immutable")
	ids := putMsgs(t, db, topic, 0, 1)

	if err := db.Delete(ids[0], topic); err != errImmutable {
		t.Fatalf("expected %v; got %v", errImmutable, err)
	}
}

func TestDeleteValidation(t *testing.T) {
	db, _ := openTestDB(t, WithMutable())

	if err := db.Delete(nil, []byte("unit.ops")); err != errMsgIDEmpty {
		t.Errorf("empty id: expected %v; got %v", errMsgIDEmpty, err)
	}
	if err := db.Delete(db.NewID(), nil); err != errTopicEmpty {
		t.Errorf("empty topic: expected %v; got %v", errTopicEmpty, err)
	}
}

func TestDeleteBeforeSync(t *testing.T) {
	db, _ := openTestDB(t, WithMutable())
	topic := []byte("unit.ops.delete.mem")
	ids := putMsgs(t, db, topic, 0, 6)

	for i := 0; i < 6; i += 2 {
		if err := db.Delete(ids[i], topic); err != nil {
			t.Fatal(err)
		}
	}

	assertMsgs(t, [][]byte{testMsg(5), testMsg(3), testMsg(1)}, get(t, db, NewQuery(topic)))
}

func TestDeleteAfterSync(t *testing.T) {
	db, dir := openTestDB(t, WithMutable())
	topic := []byte("unit.ops.delete.disk")
	ids := putMsgs(t, db, topic, 0, 6)
	syncDB(t, db)
	if count := db.Count(); count != 6 {
		t.Fatalf("expected count 6 after sync; got %d", count)
	}

	for i := 0; i < 6; i += 2 {
		if err := db.Delete(ids[i], topic); err != nil {
			t.Fatal(err)
		}
	}

	assertMsgs(t, [][]byte{testMsg(5), testMsg(3), testMsg(1)}, get(t, db, NewQuery(topic)))
	if count := db.Count(); count != 3 {
		t.Fatalf("expected count 3 after delete; got %d", count)
	}

	// Deletes must be persisted, not just applied in memory.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenTestDB(t, dir, WithMutable())
	assertMsgs(t, [][]byte{testMsg(5), testMsg(3), testMsg(1)}, get(t, db, NewQuery(topic)))
	if count := db.Count(); count != 3 {
		t.Fatalf("expected count 3 after reopen; got %d", count)
	}
}

func TestDeleteNotOnDiskAcrossIndexBlocks(t *testing.T) {
	db, _ := openTestDB(t, WithMutable(), WithBufferSize(1<<20), WithMemdbSize(1<<24))
	topic := []byte("unit.ops.delete.blocks")
	// Fill exactly 4 index blocks on disk, so the next seq falls in an index
	// block that has not been written yet.
	n := 4*entriesPerIndexBlock - 1
	putMsgs(t, db, topic, 0, n)
	deadline := time.Now().Add(10 * time.Second)
	for db.Count() < uint64(n) {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d synced; got %d", n, db.Count())
		}
		time.Sleep(100 * time.Millisecond)
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
	}

	ids := putMsgs(t, db, topic, 0, 2)
	for _, id := range ids {
		if err := db.Delete(id, topic); err != nil {
			t.Fatal(err)
		}
	}
	if count := db.Count(); count != uint64(n) {
		t.Fatalf("expected synced count %d unchanged; got %d", n, count)
	}
}

func TestGetAcrossWindowBlocks(t *testing.T) {
	db, dir := openTestDB(t, WithBufferSize(1<<20), WithMemdbSize(1<<24), WithDefaultQueryLimit(5000))
	topic := []byte("unit.ops.window.blocks")
	n := 0
	// Sync in rounds so the topic's chain grows across several syncs and window
	// blocks: exactly one full block, one entry over, then several blocks.
	for _, total := range []int{entriesPerWindowBlock, entriesPerWindowBlock + 1, 3*entriesPerWindowBlock + 10} {
		for ; n < total; n++ {
			if err := db.Put(topic, testMsg(n)); err != nil {
				t.Fatal(err)
			}
		}
		deadline := time.Now().Add(10 * time.Second)
		for db.Count() < uint64(n) {
			if time.Now().After(deadline) {
				t.Fatalf("expected %d synced; got %d", n, db.Count())
			}
			time.Sleep(100 * time.Millisecond)
			if err := db.Sync(); err != nil {
				t.Fatal(err)
			}
		}
		assertMsgs(t, newestFirst(n), get(t, db, NewQuery(topic)))
	}

	// A limit that ends inside the second block.
	limit := entriesPerWindowBlock + 20
	assertMsgs(t, newestFirst(n)[:limit], get(t, db, NewQuery(topic).WithLimit(limit)))

	// After reopen the chain head comes from loadTrie.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenTestDB(t, dir, WithBufferSize(1<<20), WithMemdbSize(1<<24), WithDefaultQueryLimit(5000))
	assertMsgs(t, newestFirst(n), get(t, db, NewQuery(topic)))

	// New messages after reopen are linked to the existing chain.
	if err := db.Put(topic, testMsg(n)); err != nil {
		t.Fatal(err)
	}
	n++
	deadline := time.Now().Add(10 * time.Second)
	for db.Count() < uint64(n) {
		if time.Now().After(deadline) {
			t.Fatalf("expected %d synced; got %d", n, db.Count())
		}
		time.Sleep(100 * time.Millisecond)
		if err := db.Sync(); err != nil {
			t.Fatal(err)
		}
	}
	assertMsgs(t, newestFirst(n), get(t, db, NewQuery(topic)))
}

func TestGetDuringSyncNoDuplicates(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.sync.overlap")
	ids := putMsgs(t, db, topic, 0, 3)
	syncDB(t, db)

	// Recreate the moment in a sync when entries are on disk but not yet
	// released from memory.
	q := NewQuery(topic)
	q.internal.opts = &_QueryOptions{defaultQueryLimit: 1000, maxQueryLimit: 100000}
	if err := q.parse(); err != nil {
		t.Fatal(err)
	}
	hash := db.internal.trie.lookup(q.internal.parts, q.internal.depth, q.internal.topicType)[0].hash
	for _, id := range ids {
		db.internal.timeWindow.add(1, hash, newWinEntry(message.ID(id).Sequence(), 0))
	}

	assertMsgs(t, newestFirst(3), get(t, db, NewQuery(topic)))
}

func TestGetNewestFromMemory(t *testing.T) {
	// Messages spread over several memdb time blocks are held in memory in a
	// map; a limited Get must still return the newest ones.
	db, _ := openTestDB(t, WithMaxSyncDuration(time.Hour, 1))
	topic := []byte("unit.ops.memory.newest")
	n := 0
	for round := 0; round < 3; round++ {
		for k := 0; k < 5; k++ {
			if err := db.Put(topic, testMsg(n)); err != nil {
				t.Fatal(err)
			}
			n++
		}
		time.Sleep(time.Until(time.Now().Truncate(time.Second).Add(time.Second + 10*time.Millisecond)))
	}

	assertMsgs(t, newestFirst(n)[:4], get(t, db, NewQuery(topic).WithLimit(4)))
}
func TestDeleteWithContract(t *testing.T) {
	db, _ := openTestDB(t, WithMutable())
	topic := []byte("unit.ops.delete.contract")
	contract, _ := db.NewContract()
	ids := putMsgs(t, db, topic, contract, 2)

	if err := db.DeleteEntry(NewEntry(topic, nil).WithID(ids[0]).WithContract(contract)); err != nil {
		t.Fatal(err)
	}

	assertMsgs(t, [][]byte{testMsg(1)}, get(t, db, NewQuery(topic).WithContract(contract)))
}

func TestGetLimitSkipsDeleted(t *testing.T) {
	for _, synced := range []bool{false, true} {
		t.Run(fmt.Sprintf("synced=%v", synced), func(t *testing.T) {
			db, _ := openTestDB(t, WithMutable())
			topic := []byte("unit.ops.limit.deleted")
			ids := putMsgs(t, db, topic, 0, 6)
			if synced {
				syncDB(t, db)
			}
			// Delete the three newest; a limit of 3 must return the three oldest.
			for _, id := range ids[3:] {
				if err := db.Delete(id, topic); err != nil {
					t.Fatal(err)
				}
			}
			assertMsgs(t, newestFirst(3), get(t, db, NewQuery(topic).WithLimit(3)))
			assertMsgs(t, newestFirst(3)[:2], get(t, db, NewQuery(topic).WithLimit(2)))
		})
	}
}

func TestPersistAfterSync(t *testing.T) {
	db, dir := openTestDB(t)
	topic := []byte("unit.ops.persist")
	putMsgs(t, db, topic, 0, 10)
	syncDB(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = reopenTestDB(t, dir)
	assertMsgs(t, newestFirst(10), get(t, db, NewQuery(topic)))
	if count := db.Count(); count != 10 {
		t.Fatalf("expected count 10; got %d", count)
	}
}

func TestRecoverWithoutSync(t *testing.T) {
	// Disable the background syncer so entries only live in the memdb WAL.
	db, dir := openTestDB(t, WithMaxSyncDuration(time.Hour, 1))
	topic := []byte("unit.ops.recover")
	putMsgs(t, db, topic, 0, 10)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = reopenTestDB(t, dir)
	assertMsgs(t, newestFirst(10), get(t, db, NewQuery(topic)))
}

func TestPersistTopicAcrossReopen(t *testing.T) {
	db, dir := openTestDB(t)
	topic := []byte("unit.ops.reopen")
	putMsgs(t, db, topic, 0, 2)
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The topic is loaded from disk into the trie; new puts must still be found.
	db = reopenTestDB(t, dir)
	if err := db.Put(topic, testMsg(2)); err != nil {
		t.Fatal(err)
	}
	assertMsgs(t, newestFirst(3), get(t, db, NewQuery(topic)))
}

func TestClosedDB(t *testing.T) {
	db, err := Open(t.TempDir(), append(smallOpts(), WithMutable())...)
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit.ops.closed")
	ids := putMsgs(t, db, topic, 0, 1)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.Close(); err != errClosed {
		t.Errorf("Close: expected %v; got %v", errClosed, err)
	}
	if err := db.Put(topic, []byte("v")); err == nil {
		t.Error("Put: expected error on closed db")
	}
	if _, err := db.Get(NewQuery(topic)); err == nil {
		t.Error("Get: expected error on closed db")
	}
	if err := db.Delete(ids[0], topic); err == nil {
		t.Error("Delete: expected error on closed db")
	}
	done := make(chan error, 1)
	go func() { done <- db.Sync() }()
	select {
	case err := <-done:
		if err != errClosed {
			t.Errorf("Sync: expected %v; got %v", errClosed, err)
		}
	case <-time.After(5 * time.Second):
		t.Error("Sync: blocked on closed db")
	}
}

func TestEncryptedDB(t *testing.T) {
	db, dir := openTestDB(t, WithEncryption())
	topic := []byte("unit.ops.encrypted")
	putMsgs(t, db, topic, 0, 5)
	assertMsgs(t, newestFirst(5), get(t, db, NewQuery(topic)))

	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = reopenTestDB(t, dir, WithEncryption())
	assertMsgs(t, newestFirst(5), get(t, db, NewQuery(topic)))
}

func TestEncryptionDisabledByDefault(t *testing.T) {
	db, dir := openTestDB(t)
	if db.internal.dbInfo.encryption != 0 {
		t.Fatalf("new db: expected encryption off; got %d", db.internal.dbInfo.encryption)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = reopenTestDB(t, dir)
	if db.internal.dbInfo.encryption != 0 {
		t.Fatalf("reopened db: expected encryption off; got %d", db.internal.dbInfo.encryption)
	}
}

func TestEncryptionFlagPersists(t *testing.T) {
	db, dir := openTestDB(t, WithEncryption())
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// The flag is stored in the db info file, so it survives a reopen without the option.
	db = reopenTestDB(t, dir)
	if db.internal.dbInfo.encryption != 1 {
		t.Fatalf("expected encryption on after reopen; got %d", db.internal.dbInfo.encryption)
	}
}

func TestEncryptedEntry(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.encrypted.entry")

	if err := db.PutEntry(NewEntry(topic, testMsg(0)).WithEncryption()); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(topic, testMsg(1)); err != nil {
		t.Fatal(err)
	}

	assertMsgs(t, newestFirst(2), get(t, db, NewQuery(topic)))
}

func TestEncryptionKey(t *testing.T) {
	key := []byte("0123456789abcdef0123456789abcdef")
	db, _ := openTestDB(t, WithEncryption(), WithEncryptionKey(key))
	topic := []byte("unit.ops.encryption.key")
	putMsgs(t, db, topic, 0, 3)

	assertMsgs(t, newestFirst(3), get(t, db, NewQuery(topic)))
}

func TestEntryTTL(t *testing.T) {
	db, _ := openTestDB(t)

	tests := []struct {
		name  string
		entry func(topic []byte) *Entry
	}{
		{"duration ttl", func(topic []byte) *Entry { return NewEntry(topic, testMsg(0)).WithTTL("1h") }},
		{"seconds ttl", func(topic []byte) *Entry { return NewEntry(topic, testMsg(0)).WithTTL("3600") }},
		{"topic duration ttl", func(topic []byte) *Entry { return NewEntry(append(topic, []byte("?ttl=1h")...), testMsg(0)) }},
		{"topic seconds ttl", func(topic []byte) *Entry { return NewEntry(append(topic, []byte("?ttl=3600")...), testMsg(0)) }},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			topic := []byte(fmt.Sprintf("unit.ops.ttl.%d", i))
			if err := db.PutEntry(tt.entry(topic)); err != nil {
				t.Fatal(err)
			}
			assertMsgs(t, [][]byte{testMsg(0)}, get(t, db, NewQuery(topic)))
		})
	}
}

func TestWithTTLExpiresAt(t *testing.T) {
	now := time.Now()
	tests := []struct {
		ttl  string
		want time.Duration // 0 means no expiry
	}{
		{"3600", time.Hour},
		{"1h", time.Hour},
		{"90s", 90 * time.Second},
		{"not-a-ttl", 0},
	}
	for _, tt := range tests {
		got := NewEntry([]byte("unit.ops"), nil).WithTTL(tt.ttl).ExpiresAt
		if tt.want == 0 {
			if got != 0 {
				t.Errorf("WithTTL(%q): expected no expiry; got %d", tt.ttl, got)
			}
			continue
		}
		want := now.Add(tt.want).Unix()
		if diff := int64(got) - want; diff < -1 || diff > 1 {
			t.Errorf("WithTTL(%q): expected expiry ~%d; got %d", tt.ttl, want, got)
		}
	}
}

func TestExpiredEntryNotReturned(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.expired")

	expired := &Entry{Topic: topic, Payload: testMsg(0), ExpiresAt: uint32(time.Now().Add(-time.Minute).Unix())}
	if err := db.PutEntry(expired); err != nil {
		t.Fatal(err)
	}
	if err := db.Put(topic, testMsg(1)); err != nil {
		t.Fatal(err)
	}

	assertMsgs(t, [][]byte{testMsg(1)}, get(t, db, NewQuery(topic)))
}

func TestBatchPut(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.batch")

	err := db.Batch(func(b *Batch, completed <-chan struct{}) error {
		for i := 0; i < 10; i++ {
			if err := b.Put(topic, testMsg(i)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	assertMsgs(t, newestFirst(10), get(t, db, NewQuery(topic)))
}

func TestBatchContract(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.batch.contract")
	contract, _ := db.NewContract()

	err := db.Batch(func(b *Batch, completed <-chan struct{}) error {
		b.SetOptions(WithBatchContract(contract))
		for i := 0; i < 3; i++ {
			if err := b.Put(topic, testMsg(i)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	assertMsgs(t, newestFirst(3), get(t, db, NewQuery(topic).WithContract(contract)))
	if items := get(t, db, NewQuery(topic)); len(items) != 0 {
		t.Fatalf("master contract: expected no messages; got %q", items)
	}
}

func TestBatchEncryption(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.batch.encryption")

	err := db.Batch(func(b *Batch, completed <-chan struct{}) error {
		b.SetOptions(WithBatchEncryption())
		for i := 0; i < 3; i++ {
			if err := b.Put(topic, testMsg(i)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	assertMsgs(t, newestFirst(3), get(t, db, NewQuery(topic)))
}

func TestBatchDelete(t *testing.T) {
	db, _ := openTestDB(t, WithMutable())
	topic := []byte("unit.ops.batch.delete")
	ids := putMsgs(t, db, topic, 0, 4)
	syncDB(t, db)

	err := db.Batch(func(b *Batch, completed <-chan struct{}) error {
		return b.Delete(ids[1], topic)
	})
	if err != nil {
		t.Fatal(err)
	}

	assertMsgs(t, [][]byte{testMsg(3), testMsg(2), testMsg(0)}, get(t, db, NewQuery(topic)))
}

func TestBatchDeleteUnsyncedThenPut(t *testing.T) {
	db, _ := openTestDB(t, WithMutable())
	topic := []byte("unit.ops.batch.delete.unsynced")
	ids := putMsgs(t, db, topic, 0, 2)

	// ids[0] is not on disk yet; it must be deleted and later entries must still be written.
	err := db.Batch(func(b *Batch, completed <-chan struct{}) error {
		if err := b.Delete(ids[0], topic); err != nil {
			return err
		}
		return b.Put(topic, testMsg(2))
	})
	if err != nil {
		t.Fatal(err)
	}

	assertMsgs(t, [][]byte{testMsg(2), testMsg(1)}, get(t, db, NewQuery(topic)))
}

func TestBatchValidation(t *testing.T) {
	db, _ := openTestDB(t)

	err := db.Batch(func(b *Batch, completed <-chan struct{}) error {
		if err := b.Put(nil, []byte("v")); err != errTopicEmpty {
			t.Errorf("Put empty topic: expected %v; got %v", errTopicEmpty, err)
		}
		if err := b.Put([]byte("unit.ops"), nil); err != errValueEmpty {
			t.Errorf("Put empty payload: expected %v; got %v", errValueEmpty, err)
		}
		if err := b.Delete(db.NewID(), []byte("unit.ops")); err != errImmutable {
			t.Errorf("Delete on immutable db: expected %v; got %v", errImmutable, err)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestBatchReturnsFnError(t *testing.T) {
	db, _ := openTestDB(t)
	topic := []byte("unit.ops.batch.error")

	wantErr := errors.New("batch failed")
	err := db.Batch(func(b *Batch, completed <-chan struct{}) error {
		if err := b.Put(topic, testMsg(0)); err != nil {
			return err
		}
		return wantErr
	})
	if err != wantErr {
		t.Fatalf("expected %v; got %v", wantErr, err)
	}
	if items := get(t, db, NewQuery(topic)); len(items) != 0 {
		t.Fatalf("expected aborted batch to write nothing; got %q", items)
	}
}

func TestBatchManagedCommitPanics(t *testing.T) {
	db, _ := openTestDB(t)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic when committing a managed batch")
		}
	}()
	db.Batch(func(b *Batch, completed <-chan struct{}) error {
		return b.Commit()
	})
}

func TestNewIDUnique(t *testing.T) {
	db, _ := openTestDB(t)

	seen := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		id := string(db.NewID())
		if seen[id] {
			t.Fatalf("duplicate id after %d calls", i)
		}
		seen[id] = true
	}
}

func TestFileSize(t *testing.T) {
	db, _ := openTestDB(t)
	putMsgs(t, db, []byte("unit.ops.filesize"), 0, 10)
	syncDB(t, db)

	size, err := db.FileSize()
	if err != nil {
		t.Fatal(err)
	}
	if size <= 0 {
		t.Fatalf("expected positive file size; got %d", size)
	}
}

func TestSyncFsyncsFiles(t *testing.T) {
	db, _ := openTestDB(t)
	if err := db.sync(); err != nil {
		t.Fatal(err)
	}

	// With the files closed, fsync must fail and the error must be returned.
	if err := db.fs.close(); err != nil {
		t.Fatal(err)
	}
	if err := db.sync(); err == nil {
		t.Fatal("expected fsync error on closed files")
	}
}

// syncedSeqs returns the sequences of ids.
func syncedSeqs(ids [][]byte) []uint64 {
	var seqs []uint64
	for _, id := range ids {
		seqs = append(seqs, message.ID(id).Sequence())
	}
	return seqs
}

func TestFilterPersists(t *testing.T) {
	db, dir := openTestDB(t, WithMutable())
	topic := []byte("unit.ops.filter")
	ids := putMsgs(t, db, topic, 0, 4)
	syncDB(t, db)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = reopenTestDB(t, dir, WithMutable())
	for _, seq := range syncedSeqs(ids) {
		if !db.internal.filter.Test(seq) {
			t.Fatalf("filter rules out synced seq %d after reopen", seq)
		}
	}
	if db.internal.filter.Test(1 << 40) {
		t.Fatal("filter should rule out a seq that was never written")
	}

	// A delete after reopen depends on the filter not ruling the entry out.
	if err := db.Delete(ids[0], topic); err != nil {
		t.Fatal(err)
	}
	assertMsgs(t, [][]byte{testMsg(3), testMsg(2), testMsg(1)}, get(t, db, NewQuery(topic)))
	if count := db.Count(); count != 3 {
		t.Fatalf("expected count 3; got %d", count)
	}
}

func TestFilterRebuiltFromIndex(t *testing.T) {
	db, dir := openTestDB(t, WithMutable())
	topic := []byte("unit.ops.filter.rebuild")
	ids := putMsgs(t, db, topic, 0, 4)
	syncDB(t, db)
	// Simulate a db created before the filter was persisted.
	if err := db.internal.filter.file.truncate(0); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db = reopenTestDB(t, dir, WithMutable())
	for _, seq := range syncedSeqs(ids) {
		if !db.internal.filter.Test(seq) {
			t.Fatalf("rebuilt filter rules out synced seq %d", seq)
		}
	}
	if size := db.internal.filter.file.currSize(); size != int64(filter.Size()) {
		t.Fatalf("expected rebuilt filter to be saved (%d bytes); got %d", filter.Size(), size)
	}
	if err := db.Delete(ids[0], topic); err != nil {
		t.Fatal(err)
	}
	if count := db.Count(); count != 3 {
		t.Fatalf("expected count 3; got %d", count)
	}
}

func TestVarz(t *testing.T) {
	db, _ := openTestDB(t, WithMutable())
	topic := []byte("unit.ops.varz")
	ids := putMsgs(t, db, topic, 0, 5)
	get(t, db, NewQuery(topic))
	if err := db.Delete(ids[0], topic); err != nil {
		t.Fatal(err)
	}

	v, err := db.Varz()
	if err != nil {
		t.Fatal(err)
	}
	if v.Puts != 5 {
		t.Errorf("Puts: expected 5; got %d", v.Puts)
	}
	if v.Gets != 5 {
		t.Errorf("Gets: expected 5; got %d", v.Gets)
	}
	if v.Dels != 1 {
		t.Errorf("Dels: expected 1; got %d", v.Dels)
	}
	if v.Leases != 5 {
		t.Errorf("Leases: expected 5; got %d", v.Leases)
	}
}

func TestHandleVarz(t *testing.T) {
	db, _ := openTestDB(t)
	putMsgs(t, db, []byte("unit.ops.handlevarz"), 0, 2)

	rec := httptest.NewRecorder()
	db.HandleVarz(rec, httptest.NewRequest(http.MethodGet, "/varz", nil))
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected application/json; got %s", ct)
	}
	var v Varz
	if err := json.Unmarshal(rec.Body.Bytes(), &v); err != nil {
		t.Fatal(err)
	}
	if v.Puts != 2 {
		t.Fatalf("expected 2 puts; got %d", v.Puts)
	}

	rec = httptest.NewRecorder()
	db.HandleVarz(rec, httptest.NewRequest(http.MethodGet, "/varz?callback=cb", nil))
	if body := rec.Body.String(); !strings.HasPrefix(body, "cb(") || !strings.HasSuffix(body, ")") {
		t.Fatalf("expected JSONP response wrapped in cb(...); got %s", body)
	}
}

func TestConcurrentPutGet(t *testing.T) {
	db, _ := openTestDB(t)

	const workers = 8
	const perWorker = 50
	var wg sync.WaitGroup
	errC := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			// Topics share the "unit.ops.concurrent" prefix, so their trie nodes are created concurrently.
			topic := []byte(fmt.Sprintf("unit.ops.concurrent.%d", w))
			for i := 0; i < perWorker; i++ {
				if err := db.Put(topic, testMsg(i)); err != nil {
					errC <- err
					return
				}
				items, err := db.Get(NewQuery(topic))
				if err != nil {
					errC <- err
					return
				}
				if len(items) != i+1 {
					errC <- fmt.Errorf("worker %d: expected %d messages; got %d", w, i+1, len(items))
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errC)
	for err := range errC {
		t.Fatal(err)
	}
}
