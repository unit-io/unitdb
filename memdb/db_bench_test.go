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

package memdb

import (
	"testing"
	"time"
)

const benchKeys = 10000

// openBenchDB opens a DB with benchKeys keys spread over nBlocks time blocks.
func openBenchDB(b *testing.B, nBlocks int) *DB {
	b.Helper()
	db, err := Open(WithLogFilePath(b.TempDir()), WithLogReset(), WithTimeBlockInterval(20*time.Millisecond), WithLogInterval(5*time.Millisecond))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	perBlock := benchKeys / nBlocks
	for k := uint64(0); k < benchKeys; k++ {
		if _, err := db.Put(k, testVal(k)); err != nil {
			b.Fatal(err)
		}
		if nBlocks > 1 && k%uint64(perBlock) == uint64(perBlock-1) {
			time.Sleep(25 * time.Millisecond)
		}
	}
	return db
}

func BenchmarkGet(b *testing.B) {
	db := openBenchDB(b, 1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(uint64(i % benchKeys)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetAcrossBlocks(b *testing.B) {
	db := openBenchDB(b, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Stride so consecutive gets hit different time blocks.
		if _, err := db.Get(uint64(i*1237) % benchKeys); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetParallel(b *testing.B) {
	db := openBenchDB(b, 8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := uint64(0)
		for pb.Next() {
			if _, err := db.Get((i * 1237) % benchKeys); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

func BenchmarkGetMissing(b *testing.B) {
	db := openBenchDB(b, 8)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(benchKeys + uint64(i)); err != errEntryDoesNotExist {
			b.Fatal(err)
		}
	}
}
