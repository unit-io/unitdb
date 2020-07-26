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
	"os"
	"reflect"
	"testing"
	"time"
)

func open(path string, opts *Options) (*DB, error) {
	os.Remove(path + indexPostfix)
	os.Remove(path + dataPostfix)
	os.Remove(path + logPostfix)
	os.Remove(path + lockPostfix)
	os.Remove(path + windowPostfix)
	os.Remove(path + filterPostfix)
	return Open(path, opts, WithMutable(), WithBackgroundKeyExpiry())
}

func TestSimple(t *testing.T) {
	opts := &Options{BufferSize: 1 << 4, MemdbSize: 1 << 16, LogSize: 1 << 16, MinimumFreeBlocksSize: 1 << 16}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var i uint16
	var n uint16 = 1000

	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit1.test")

	if db.count != 0 {
		t.Fatal()
	}

	if data, err := db.Get(NewQuery(topic).WithContract(contract)); data != nil || err != nil {
		t.Fatal()
	}

	if db.count != 0 {
		t.Fatal()
	}
	var ids [][]byte

	entry := NewEntry(topic).WithContract(contract).WithTTL([]byte("1m"))
	for i = 0; i < n; i++ {
		messageID := db.NewID()
		entry.WithID(messageID)
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := db.PutEntry(entry.WithPayload(val)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, messageID)
	}

	verifyMsgsAndClose := func() {
		if count := db.Count(); count != uint64(n) {
			if err := db.recoverLog(); err != nil {
				t.Fatal(err)
			}
		}
		var v, vals [][]byte
		v, err = db.Get(NewQuery(append(topic, []byte("?last=1h")...)).WithContract(contract))
		if err != nil {
			t.Fatal(err)
		}
		for i = 0; i < n; i++ {
			val := []byte(fmt.Sprintf("msg.%2d", n-i-1))
			vals = append(vals, val)
		}

		if !reflect.DeepEqual(vals, v) {
			t.Fatalf("expected %v; got %v", vals, v)
		}
		if size, err := db.FileSize(); err != nil || size == 0 {
			t.Fatal(err)
		}
		if _, err = db.Varz(); err != nil {
			t.Fatal(err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	verifyMsgsAndClose()
	db, err = Open("test.db", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Get(NewQuery(topic).WithContract(contract).WithLimit(int(n)))
	if err != nil {
		t.Fatal(err)
	}

	for i = 0; i < n; i++ {
		messageID := db.NewID()
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := db.Put(topic, val); err != nil {
			t.Fatal(err)
		}
		if err := db.PutEntry(NewEntry(topic).WithID(messageID).WithPayload(val)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, messageID)
	}
	db.tinyCommit()
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		db.Delete(id, topic)
	}
}

func TestBatch(t *testing.T) {
	opts := &Options{BufferSize: 1 << 16, MemdbSize: 1 << 16, LogSize: 1 << 16, MinimumFreeBlocksSize: 1 << 16}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit2.test")

	// if db.count != 0 {
	// 	t.Fatal()
	// }

	var i uint16
	var n uint16 = 100

	verifyMsgsAndClose := func() {
		if count := db.Count(); count != uint64(n) {
			if err := db.recoverLog(); err != nil {
				t.Fatal(err)
			}
		}
		var v, vals [][]byte
		v, err = db.Get(NewQuery(append(topic, []byte("?last=1h")...)).WithContract(contract))
		if err != nil {
			t.Fatal(err)
		}
		for i = 0; i < n; i++ {
			val := []byte(fmt.Sprintf("msg.%2d", n-i-1))
			vals = append(vals, val)
		}
		if !reflect.DeepEqual(vals, v) {
			t.Fatalf("expected %v; got %v", vals, v)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}

	err = db.Batch(func(b *Batch, completed <-chan struct{}) error {
		// wg.Add(1)
		var ids [][]byte
		for i = 0; i < n; i++ {
			messageID := db.NewID()
			topic := append(topic, []byte("?ttl=1h")...)
			val := []byte(fmt.Sprintf("msg.%2d", i))
			if err := b.PutEntry(NewEntry(topic).WithID(messageID).WithPayload(val).WithContract(contract)); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, messageID)
		}
		err := b.Write()
		return err
	})

	if err != nil {
		t.Fatal(err)
	}
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	verifyMsgsAndClose()
}

func TestBatchGroup(t *testing.T) {
	opts := &Options{BufferSize: 1 << 16, MemdbSize: 1 << 16, LogSize: 1 << 16, MinimumFreeBlocksSize: 1 << 16}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit3.test")

	var i uint16
	var n uint16 = 50

	// var wg sync.WaitGroup
	batch := func(b *Batch, completed <-chan struct{}) error {
		// wg.Add(1)
		for i = 0; i < n; i++ {
			topic := append(topic, []byte("?ttl=1h")...)
			val := []byte(fmt.Sprintf("msg.%2d", i))
			if err := db.PutEntry(NewEntry(topic).WithPayload(val).WithContract(contract)); err != nil {
				t.Fatal(err)
			}
		}
		err := b.Write()
		return err
	}

	g := db.NewBatchGroup()
	g.Add(batch)
	g.Add(batch)
	g.Add(batch)

	if err := g.Run(); err != nil {
		t.Fatal(err)
	}

	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	for i = 0; i < n; i++ {
		_, err = db.Get(NewQuery(append(topic, []byte("?last=1h")...)).WithContract(contract))
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestExpiry(t *testing.T) {
	db, err := open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	contract, err := db.NewContract()
	if err != nil {
		t.Fatal(err)
	}
	topic := []byte("unit4.test")

	var i uint16
	var n uint16 = 100

	err = db.Batch(func(b *Batch, completed <-chan struct{}) error {
		expiresAt := uint32(time.Now().Add(-1 * time.Hour).Unix())
		entry := &Entry{Topic: topic, ExpiresAt: expiresAt}
		for i = 0; i < n; i++ {
			val := []byte(fmt.Sprintf("msg.%2d", i))
			if err := db.PutEntry(entry.WithPayload(val).WithContract(contract)); err != nil {
				t.Fatal(err)
			}
		}
		err := b.Write()
		return err
	})

	if err != nil {
		t.Fatal(err)
	}

	if data, err := db.Get(NewQuery(topic).WithContract(contract).WithLimit(int(n))); len(data) != 0 || err != nil {
		t.Fatal()
	}
	db.expireEntries()
}

func TestAbort(t *testing.T) {
	opts := &Options{BufferSize: 1 << 16, MemdbSize: 1 << 16, LogSize: 1 << 16, MinimumFreeBlocksSize: 1 << 16}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var i uint16
	var n uint16 = 500

	topic := []byte("unit1.test")

	for i = 0; i < n; i++ {
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := db.Put(topic, val); err != nil {
			t.Fatal(err)
		}
	}
	db.tinyCommit()
	dbsync := syncHandle{DB: db, internal: internal{}}
	dbabort := syncHandle{DB: db, internal: dbsync.internal}
	dbabort.startSync()
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	dbabort.abort()
}

func TestLeasing(t *testing.T) {
	opts := &Options{BufferSize: 1 << 16, MemdbSize: 1 << 16, LogSize: 1 << 16, MinimumFreeBlocksSize: 1 << 4}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var i uint16
	var n uint16 = 100

	topic := []byte("unit1.test")
	var ids [][]byte
	for i = 0; i < n; i++ {
		messageID := db.NewID()
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := db.PutEntry(NewEntry(topic).WithID(messageID).WithPayload(val)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, messageID)
	}
	db.tinyCommit()
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		db.Delete(id, topic)
	}
	for i = 0; i < n; i++ {
		messageID := db.NewID()
		val := []byte(fmt.Sprintf("msg.%2d", i))
		if err := db.Put(topic, val); err != nil {
			t.Fatal(err)
		}
		if err := db.PutEntry(NewEntry(topic).WithID(messageID).WithPayload(val)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, messageID)
	}
	db.tinyCommit()
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	for _, id := range ids {
		db.Delete(id, topic)
	}
}

func TestWildcardTopics(t *testing.T) {
	opts := &Options{BufferSize: 1 << 16, MemdbSize: 1 << 16, LogSize: 1 << 16, MinimumFreeBlocksSize: 1 << 16}
	db, err := open("test.db", opts)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tests := []struct {
		wtopic []byte
		topic  []byte
		msg    []byte
	}{
		{[]byte("..."), []byte("unit.b.b1"), []byte("...1")},
		{[]byte("unit.b..."), []byte("unit.b.b1.b11.b111.b1111.b11111.b111111"), []byte("unit.b...1")},
		{[]byte("unit.*.b1.b11.*.*.b11111.*"), []byte("unit.b.b1.b11.b111.b1111.b11111.b111111"), []byte("unit.*.b1.b11.*.*.b11111.*.1")},
		{[]byte("unit.*.b1.*.*.*.b11111.*"), []byte("unit.b.b1.b11.b111.b1111.b11111.b111111"), []byte("unit.*.b1.*.*.*.b11111.*.1")},
		{[]byte("unit.b.b1"), []byte("unit.b.b1"), []byte("unit.b.b1.1")},
		{[]byte("unit.b.b1.b11"), []byte("unit.b.b1.b11"), []byte("unit.b.b1.b11.1")},
		{[]byte("unit.b"), []byte("unit.b"), []byte("unit.b.1")},
	}
	for _, tt := range tests {
		db.Put(tt.wtopic, tt.msg)
		if msg, err := db.Get(NewQuery(tt.wtopic).WithLimit(10)); len(msg) == 0 || err != nil {
			t.Fatal(err)
		}
		if msg, err := db.Get(NewQuery(tt.topic).WithLimit(10)); len(msg) == 0 || err != nil {
			t.Fatal(err)
		}
	}
}
