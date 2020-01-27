package memdb

import (
	"math/rand"
	"reflect"
	"testing"
)

func TestSimple(t *testing.T) {
	size := 1 << 33
	mdb, err := Open(int64(size))
	if err != nil {
		t.Fatal(err)
	}

	if mdb.Count() != 0 {
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
		if data, err := mdb.Get(contract, k); data != nil || err == nil {
			t.Fatal(err)
		}
	}

	for i = 0; i < n; i++ {
		k := cacheID ^ uint64(i)
		val := []byte("msg.")
		val = append(val, i)
		if err = mdb.Set(contract, k, val); err != nil {
			t.Fatal(err)
		}
	}

	verifyMsgsAndClose := func() {
		if count := mdb.Count(); count != 255 {
			mdb.Close()
			t.Fatalf("expected 255 records; got %d", count)
		}
		var v []byte
		for i = 0; i < n; i++ {
			k := cacheID ^ uint64(i)
			val := []byte("msg.")
			val = append(val, i)
			v, err = mdb.Get(contract, k)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(val, v) {
				t.Fatalf("expected %v; got %v", val, v)
			}

		}
		if err := mdb.Close(); err != nil {
			t.Fatal(err)
		}
	}

	verifyMsgsAndClose()
}
