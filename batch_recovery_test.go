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
	"path/filepath"
	"testing"
	"time"
)

// TestManyBatchesRecover writes many small batches and reopens before the
// background sync runs, so recovery replays them all. Each batch commits
// its own memdb block; recovery syncs them in groups, not one fsync round
// per block (which took about 20 ms a block on macOS).
func TestManyBatchesRecover(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "batches")
	db, err := Open(dir, WithDefaultOptions(), WithMutable())
	if err != nil {
		t.Fatal(err)
	}
	const n = 600
	for i := 0; i < n; i++ {
		topic := []byte(fmt.Sprintf("b.%d", i%40))
		if err := db.Batch(func(b *Batch, _ <-chan struct{}) error { return b.Put(topic, []byte(fmt.Sprint(i))) }); err != nil {
			t.Fatal(err)
		}
	}
	want := uint64(n) // Count includes only synced entries until recovery syncs them
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	db, err = Open(dir, WithDefaultOptions(), WithMutable())
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	took := time.Since(start)
	t.Logf("reopened %d batches in %v", n, took)
	if got := db.Count(); got != want {
		t.Fatalf("count after recovery = %d, want %d", got, want)
	}
	total := 0
	for i := 0; i < 40; i++ {
		items, err := db.Get(NewQuery([]byte(fmt.Sprintf("b.%d", i))))
		if err != nil {
			t.Fatal(err)
		}
		total += len(items)
	}
	if total != n {
		t.Fatalf("entries after recovery = %d, want %d", total, n)
	}
	// Before grouping this took ~12 s; allow plenty for slow machines.
	if took > 5*time.Second {
		t.Errorf("recovery of %d batches took %v", n, took)
	}
}
