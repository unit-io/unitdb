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
	"testing"
	"time"

	"github.com/unit-io/unitdb/message"
)

// openBenchDB opens a mutable DB with nSynced messages synced to disk.
func openBenchDB(b *testing.B, nSynced int) (*DB, []byte) {
	b.Helper()
	db, err := Open(b.TempDir(), WithBufferSize(1<<20), WithMemdbSize(1<<24), WithFreeBlockSize(1<<16), WithMutable())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	topic := []byte("unit.bench")
	for i := 0; i < nSynced; i++ {
		if err := db.Put(topic, testMsg(i)); err != nil {
			b.Fatal(err)
		}
	}
	syncAll(b, db, uint64(nSynced))
	return db, topic
}

// syncAll syncs until want messages are on disk. memdb hands only blocks older
// than the current one to Sync, and Sync returns early while the background
// syncer runs, so retry.
func syncAll(b *testing.B, db *DB, want uint64) {
	b.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for db.Count() < want {
		if time.Now().After(deadline) {
			b.Fatalf("expected %d synced; got %d", want, db.Count())
		}
		time.Sleep(100 * time.Millisecond)
		if err := db.Sync(); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkFilterTest measures the bloom filter check used before disk deletes.
func BenchmarkFilterTest(b *testing.B) {
	db, _ := openBenchDB(b, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.internal.filter.Test(uint64(i))
	}
}

// BenchmarkDeleteNotOnDisk deletes messages that were never synced, the case
// the filter is meant to short-circuit.
func BenchmarkDeleteNotOnDisk(b *testing.B) {
	db, topic := openBenchDB(b, 1000)
	ids := make([][]byte, b.N)
	for i := range ids {
		ids[i] = db.NewID()
		if err := db.PutEntry(NewEntry(topic, testMsg(i)).WithID(ids[i])); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Delete(ids[i], topic); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDeleteOnDisk deletes synced messages; the filter must say "maybe".
func BenchmarkDeleteOnDisk(b *testing.B) {
	db, topic := openBenchDB(b, 0)
	ids := make([][]byte, b.N)
	for i := range ids {
		ids[i] = db.NewID()
		if err := db.PutEntry(NewEntry(topic, testMsg(i)).WithID(ids[i])); err != nil {
			b.Fatal(err)
		}
	}
	syncAll(b, db, uint64(b.N))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Delete(ids[i], topic); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if seq := message.ID(ids[0]).Sequence(); db.internal.filter.Test(seq) != true {
		b.Fatal("filter must not rule out a synced message")
	}
}
