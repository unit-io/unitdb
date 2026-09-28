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

// Package ringhash implementats a consistent ring hash:
// https://en.wikipedia.org/wiki/Consistent_hashing
package hash

import (
	"encoding/ascii85"
	"hash/fnv"
	"log"
	"sort"
	"strconv"
)

// Hash is a signature of a hash function used by the package.
type Hash func(data []byte) uint32

type elem struct {
	key  string
	hash uint32
}

type sortable []elem

func (k sortable) Len() int      { return len(k) }
func (k sortable) Swap(i, j int) { k[i], k[j] = k[j], k[i] }
func (k sortable) Less(i, j int) bool {
	// Weak hash function may cause collisions.
	if k[i].hash < k[j].hash {
		return true
	}
	if k[i].hash == k[j].hash {
		return k[i].key < k[j].key
	}
	return false
}

// Ring is the definition of the ringhash.
type Ring struct {
	keys []elem // Sorted list of keys.

	signature string
	replicas  int
	hashfunc  Hash
}

// New initializes an empty ringhash with the given number of replicas and a hash function.
// If the hash function is nil, fnv.New32a() is used, mixed by fmix32.
func NewRing(replicas int, fn Hash) *Ring {
	ring := &Ring{
		replicas: replicas,
		hashfunc: fn,
	}
	if ring.hashfunc == nil {
		ring.hashfunc = func(data []byte) uint32 {
			hash := fnv.New32a()
			hash.Write(data)
			return fmix32(hash.Sum32())
		}
	}
	return ring
}

// fmix32 is MurmurHash3's finalizer: every bit of h changes about half the
// bits of the result. FNV-1a alone changes few high bits for keys that
// differ in their last bytes, such as consecutive ids, and put them next to
// each other on the ring, with the same owner.
func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// Len returns the number of keys in the ring.
func (ring *Ring) Len() int {
	return len(ring.keys)
}

// Add adds keys to the ring.
func (ring *Ring) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < ring.replicas; i++ {
			ring.keys = append(ring.keys, elem{
				hash: ring.hashfunc([]byte(strconv.Itoa(i) + key)),
				key:  key})
		}
	}
	sort.Sort(sortable(ring.keys))

	// Calculate signature
	hash := fnv.New128a()
	b := make([]byte, 4)
	for _, key := range ring.keys {
		b[0] = byte(key.hash)
		b[1] = byte(key.hash >> 8)
		b[2] = byte(key.hash >> 16)
		b[3] = byte(key.hash >> 24)
		hash.Write(b)
		hash.Write([]byte(key.key))
	}

	b = []byte{}
	b = hash.Sum(b)
	dst := make([]byte, ascii85.MaxEncodedLen(len(b)))
	ascii85.Encode(dst, b)
	ring.signature = string(dst)
}

// Get returns the closest item in the ring to the provided key.
func (ring *Ring) Get(key string) string {

	if ring.Len() == 0 {
		return ""
	}

	return ring.keys[ring.index(key)].key
}

// GetN returns up to n distinct items for the provided key: the item Get
// returns, followed by the next distinct items clockwise around the ring.
func (ring *Ring) GetN(key string, n int) []string {
	if ring.Len() == 0 || n <= 0 {
		return nil
	}

	idx := ring.index(key)
	seen := make(map[string]bool, n)
	items := make([]string, 0, n)
	for i := 0; i < len(ring.keys) && len(items) < n; i++ {
		item := ring.keys[(idx+i)%len(ring.keys)].key
		if !seen[item] {
			seen[item] = true
			items = append(items, item)
		}
	}
	return items
}

// index returns the position in the ring of the closest item to key.
func (ring *Ring) index(key string) int {
	hash := ring.hashfunc([]byte(key))

	// Binary search for appropriate replica.
	idx := sort.Search(len(ring.keys), func(i int) bool {
		el := ring.keys[i]
		return (el.hash > hash) || (el.hash == hash && el.key >= key)
	})

	// Means we have cycled back to the first replica.
	if idx == len(ring.keys) {
		idx = 0
	}

	return idx
}

// Signature returns the ring's hash signature. Two identical ringhashes
// will have the same signature. Two hashes with different
// number of keys or replicas or hash functions will have different
// signatures.
func (ring *Ring) Signature() string {
	return ring.signature
}

func (ring *Ring) dump() {
	for _, e := range ring.keys {
		log.Printf("key %s hash %d\n", e.key, e.hash)
	}
}
