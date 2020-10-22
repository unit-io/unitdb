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

package cache

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestSimple(t *testing.T) {
	size := int64(1 << 4)
	bc, err := Open(size, nil)
	if err != nil {
		t.Fatal(err)
	}

	if bc.Count() != 0 {
		t.Fatal()
	}

	var i byte
	var n uint8 = 255
	Contract := uint32(3376684800)
	part := uint32(857445537)
	contract := uint64(Contract)<<32 + uint64(part)
	cacheID := uint64(rand.Uint32())<<32 + uint64(rand.Uint32())

	for i = 0; i < n; i++ {
		k := cacheID ^ uint64(i)
		if data, err := bc.Get(contract, k); data != nil || err != nil {
			t.Fatal(err)
		}
	}

	for i = 0; i < n; i++ {
		k := cacheID ^ uint64(i)
		val := []byte("msg.")
		val = append(val, i)
		if err = bc.Set(contract, k, val); err != nil {
			t.Fatal(err)
		}
	}

	verifyMsgs := func() {
		if count := bc.Count(); count != uint64(n) {
			bc.Close()
			t.Fatalf("expected %d records; got %d", n, count)
		}
		var v []byte
		for i = 0; i < n; i++ {
			k := cacheID ^ uint64(i)
			val := []byte("msg.")
			val = append(val, i)
			v, err = bc.Get(contract, k)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(val, v) {
				t.Fatalf("expected %v; got %v", val, v)
			}
		}
		if _, err := bc.Size(); err != nil {
			t.Fatal(err)
		}
	}

	verifyMsgs()

	if err := bc.Free(contract, cacheID^uint64(n-1)); err != nil {
		t.Fatal(err)
	}

	for i = 0; i < n; i++ {
		k := cacheID ^ uint64(i)
		if err = bc.Remove(contract, k); err != nil {
			t.Fatal(err)
		}
	}

	if err := bc.shrinkDataTable(); err != nil {
		t.Fatal(err)
	}

	verifyAndClose := func() {
		if count := bc.Count(); count != 0 {
			bc.Close()
			t.Fatalf("expected zero records; got %d", count)
		}
		if err := bc.Close(); err != nil {
			t.Fatal(err)
		}
	}
	verifyAndClose()
}
