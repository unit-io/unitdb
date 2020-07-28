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
	"testing"
	"time"
)

func TestIteratorEmpty(t *testing.T) {
	db, err := open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Items(NewQuery(nil))
	if err == nil {
		t.Fatal(err)
	}
}

func TestIterator(t *testing.T) {
	db, err := open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}

	var i uint16
	var n uint16 = 255
	items := map[uint16]bool{}

	entry := NewEntry([]byte("unit5.test?ttl=1m"), nil)
	entry.WithContract(contract)
	for i = 0; i < n; i++ {
		items[i] = false
		val := []byte(fmt.Sprintf("msg.%2d", i))
		entry.WithPayload(val)
		if err := db.PutEntry(entry); err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(10 * time.Millisecond)
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}

	time.Sleep(10 * time.Millisecond)
	it, err := db.Items(NewQuery([]byte("unit5.test?last=1s")).WithContract(contract))
	if err != nil {
		t.Fatal(err)
	}
	i = 0
	for it.First(); it.Valid(); it.Next() {
		err := it.Error()
		if err != nil {
			t.Fatal(err)
		}
		i++
	}

	if i != n {
		t.Fatalf("expected %d records; got %d", n, i)
	}
}
