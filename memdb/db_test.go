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
	"reflect"
	"testing"
)

func TestSimple(t *testing.T) {
	db, err := Open(WithLogFilePath("test"))
	if err != nil {
		t.Fatal(err)
	}

	if db.Size() != 0 {
		t.Fatal()
	}

	var i byte
	var n uint8 = 255

	for i = 0; i < n; i++ {
		k := uint64(i)
		val := []byte("msg.")
		val = append(val, i)
		if _, err = db.Put(k, val); err != nil {
			t.Fatal(err)
		}
	}

	for i = 0; i < n; i++ {
		k := uint64(i)
		if data, err := db.Get(k); data == nil || err != nil {
			t.Fatal(err)
		}
	}

	verifyMsgs := func() {
		if size := db.Size(); size != int64(n) {
			db.Close()
			t.Fatalf("expected %d records; got %d", n, size)
		}
		var v []byte
		for i = 0; i < n; i++ {
			k := uint64(i)
			val := []byte("msg.")
			val = append(val, i)
			v, err = db.Get(k)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(val, v) {
				t.Fatalf("expected %v; got %v", val, v)
			}
		}
	}

	verifyMsgs()

	for i = 0; i < n; i++ {
		k := uint64(i)
		if err = db.Delete(k); err != nil {
			t.Fatal(err)
		}
	}

	verifyAndClose := func() {
		if size := db.Size(); size != 0 {
			db.Close()
			t.Fatalf("expected zero records; got %d", size)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	verifyAndClose()
}

func TestRecovery(t *testing.T) {
	db, err := Open(WithLogFilePath("test"), WithLogReset())
	if err != nil {
		t.Fatal(err)
	}

	if db.Size() != 0 {
		t.Fatal()
	}

	var i byte
	var n uint8 = 255

	for i = 0; i < n; i++ {
		k := uint64(i)
		val := []byte("msg.")
		val = append(val, i)
		if _, err = db.Put(k, val); err != nil {
			t.Fatal(err)
		}
	}

	for i = 0; i < n; i++ {
		k := uint64(i)
		if data, err := db.Get(k); data == nil || err != nil {
			t.Fatal(err)
		}
	}

	// Close and open db to start recovery from log file.
	db.Close()
	db, err = Open(WithLogFilePath("test"))
	if err != nil {
		t.Fatal(err)
	}

	verifyMsgs := func() {
		if size := db.Size(); size != int64(n) {
			db.Close()
			t.Fatalf("expected %d records; got %d", n, size)
		}
		var v []byte
		for i = 0; i < n; i++ {
			k := uint64(i)
			val := []byte("msg.")
			val = append(val, i)
			v, err = db.Get(k)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(val, v) {
				t.Fatalf("expected %v; got %v", val, v)
			}
		}
	}

	verifyMsgs()

	for i = 0; i < n; i++ {
		k := uint64(i)
		if err = db.Delete(k); err != nil {
			t.Fatal(err)
		}
	}

	verifyAndClose := func() {
		if size := db.Size(); size != 0 {
			db.Close()
			t.Fatalf("expected zero records; got %d", size)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	verifyAndClose()
}

func TestLogRelease(t *testing.T) {
	db, err := Open(WithLogFilePath("test"), WithLogReset())
	if err != nil {
		t.Fatal(err)
	}

	if db.Size() != 0 {
		t.Fatal()
	}

	var i byte
	var n uint8 = 255

	var timeID int64
	for i = 0; i < n; i++ {
		k := uint64(i)
		val := []byte("msg.")
		val = append(val, i)
		if timeID, err = db.Put(k, val); err != nil {
			t.Fatal(err)
		}
	}

	if err := db.Free(timeID); err != nil {
		t.Fatal(err)
	}

	verifyAndClose := func() {
		if size := db.Size(); size != 0 {
			db.Close()
			t.Fatalf("expected zero records; got %d", size)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
	verifyAndClose()
}
