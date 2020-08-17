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
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/unitdb/hash"
)

type (
	winEntry struct {
		seq       uint64
		expiresAt uint32
	}
	winBlock struct {
		topicHash uint64
		entries   [seqsPerWindowBlock]winEntry
		next      int64 //next stores offset that links multiple winBlocks for a topic hash. Most recent offset is stored into the trie to iterate entries in reverse order)
		cutoff    int64
		entryIdx  uint16

		dirty  bool // dirty used during timeWindow append and not persisted
		leased bool // leased used in timeWindow write and not persisted
	}
)

func (e winEntry) Seq() uint64 {
	return e.seq
}

func (e winEntry) ExpiresAt() uint32 {
	return e.expiresAt
}

func (e winEntry) isExpired() bool {
	return e.expiresAt != 0 && e.expiresAt <= uint32(time.Now().Unix())
}

func (w winBlock) Cutoff(cutoff int64) bool {
	return w.cutoff != 0 && w.cutoff < cutoff
}

// MarshalBinary serialized window block into binary data
func (w winBlock) MarshalBinary() []byte {
	buf := make([]byte, blockSize)
	data := buf
	for i := 0; i < seqsPerWindowBlock; i++ {
		e := w.entries[i]
		binary.LittleEndian.PutUint64(buf[:8], e.seq)
		binary.LittleEndian.PutUint32(buf[8:12], e.expiresAt)
		buf = buf[12:]
	}
	binary.LittleEndian.PutUint64(buf[:8], uint64(w.cutoff))
	binary.LittleEndian.PutUint64(buf[8:16], w.topicHash)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(w.next))
	binary.LittleEndian.PutUint16(buf[24:26], w.entryIdx)
	return data
}

// UnmarshalBinary de-serialized window block from binary data
func (w *winBlock) UnmarshalBinary(data []byte) error {
	for i := 0; i < seqsPerWindowBlock; i++ {
		_ = data[12] // bounds check hint to compiler; see golang.org/issue/14808
		w.entries[i].seq = binary.LittleEndian.Uint64(data[:8])
		w.entries[i].expiresAt = binary.LittleEndian.Uint32(data[8:12])
		data = data[12:]
	}
	w.cutoff = int64(binary.LittleEndian.Uint64(data[:8]))
	w.topicHash = binary.LittleEndian.Uint64(data[8:16])
	w.next = int64(binary.LittleEndian.Uint64(data[16:24]))
	w.entryIdx = binary.LittleEndian.Uint16(data[24:26])
	return nil
}

type windowHandle struct {
	winBlock
	file
	offset int64
}

func winBlockOffset(idx int32) int64 {
	return (int64(blockSize) * int64(idx))
}

func (wh *windowHandle) read() error {
	buf, err := wh.file.Slice(wh.offset, wh.offset+int64(blockSize))
	if err != nil {
		return err
	}
	return wh.UnmarshalBinary(buf)
}

type (
	timeOptions struct {
		timeSlotDuration    time.Duration
		expDurationType     time.Duration
		maxExpDurations     int
		backgroundKeyExpiry bool
	}
	timeWindowBucket struct {
		sync.RWMutex
		timeID int64
		file
		*windowBlocks
		*expiryWindowBucket
		windowIdx int32
		opts      *timeOptions
	}
)

func (src *timeOptions) copyWithDefaults() *timeOptions {
	opts := timeOptions{}
	if src != nil {
		opts = *src
	}
	if opts.timeSlotDuration == 0 {
		opts.timeSlotDuration = 100 * time.Millisecond
	}
	if opts.expDurationType == 0 {
		opts.expDurationType = time.Minute
	}
	if opts.maxExpDurations == 0 {
		opts.maxExpDurations = 1
	}
	return &opts
}

type windowEntries []winEntry
type key struct {
	timeID    int64
	topicHash uint64
}
type timeWindow struct {
	mu sync.RWMutex

	entries map[key]windowEntries
}

// A "thread" safe windowBlocks.
// To avoid lock bottlenecks windowBlocks are divided into several shards (maxWinDurations).
type windowBlocks struct {
	sync.RWMutex
	window     []*timeWindow
	consistent *hash.Consistent
}

// newWindows creates a new concurrent windows.
func newWindowBlocks() *windowBlocks {
	wb := &windowBlocks{
		window:     make([]*timeWindow, nShards),
		consistent: hash.InitConsistent(nShards, nShards),
	}

	for i := 0; i < nShards; i++ {
		wb.window[i] = &timeWindow{entries: make(map[key]windowEntries)}
	}

	return wb
}

// getWindow returns shard under given blockID
func (w *windowBlocks) getWindowBlock(blockID uint64) *timeWindow {
	w.RLock()
	defer w.RUnlock()
	return w.window[w.consistent.FindBlock(blockID)]
}

func newTimeWindowBucket(f file, opts *timeOptions) *timeWindowBucket {
	opts = opts.copyWithDefaults()
	l := &timeWindowBucket{file: f, windowIdx: -1}
	l.windowBlocks = newWindowBlocks()
	l.expiryWindowBucket = newExpiryWindowBucket(opts.backgroundKeyExpiry, opts.expDurationType, opts.maxExpDurations)
	l.opts = opts.copyWithDefaults()
	return l
}

func (tw *timeWindowBucket) add(timeID int64, topicHash uint64, e winEntry) error {
	// get windowBlock shard
	wb := tw.getWindowBlock(topicHash)
	wb.mu.Lock()
	defer wb.mu.Unlock()

	key := key{
		timeID:    timeID,
		topicHash: topicHash,
	}
	if _, ok := wb.entries[key]; ok {
		wb.entries[key] = append(wb.entries[key], e)
	} else {
		wb.entries[key] = windowEntries{e}
	}
	return nil
}

func (w *timeWindow) reset() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.entries = make(map[key]windowEntries)
	return nil
}

// foreachTimeWindow iterates timewindow entries during sync or recovery process when writing entries to window file
func (tw *timeWindowBucket) foreachTimeWindow(f func(timeID int64, w windowEntries) (bool, error)) (err error) {
	var keys []key
	wEntries := make(map[int64]windowEntries)
	for i := 0; i < nShards; i++ {
		wb := tw.windowBlocks.window[i]
		wb.mu.RLock()
		for key := range wb.entries {
			// skip window entries for current timeID
			if key.timeID > tw.TimeID() {
				continue
			}
			if _, ok := wEntries[key.timeID]; ok {
				wEntries[key.timeID] = append(wEntries[key.timeID], wb.entries[key]...)
			} else {
				keys = append(keys, key)
				wEntries[key.timeID] = wb.entries[key]
			}
		}
		wb.mu.RUnlock()
	}

	sort.Slice(keys[:], func(i, j int) bool {
		return keys[i].timeID < keys[j].timeID
	})

	for _, key := range keys {
		stop, err1 := f(key.timeID, wEntries[key.timeID])
		if stop || err != nil {
			err = err1
			continue
		}
	}
	// delete window entries except for the current timeID
	for i := 0; i < nShards; i++ {
		wb := tw.windowBlocks.window[i]
		wb.mu.Lock()
		for key := range wb.entries {
			// skip window entries for current timeID
			if key.timeID > tw.TimeID() {
				continue
			}
			delete(wb.entries, key)
		}
		wb.mu.Unlock()
	}
	return err
}

// foreachWindowBlock iterates winBlocks on DB init to store topic hash and last offset of topic into trie.
func (tw *timeWindowBucket) foreachWindowBlock(f func(windowHandle) (bool, error)) (err error) {
	winBlockIdx := int32(0)
	nWinBlocks := tw.windowIndex()
	for winBlockIdx <= nWinBlocks {
		off := winBlockOffset(winBlockIdx)
		b := windowHandle{file: tw.file, offset: off}
		if err := b.read(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if stop, err := f(b); stop || err != nil {
			return err
		}
		winBlockIdx++
	}
	return nil
}

// ilookup lookups window entries from timeWindowBucket and not yet sync to DB
func (tw *timeWindowBucket) ilookup(topicHash uint64, limit int) (winEntries windowEntries) {
	winEntries = make([]winEntry, 0)
	// get windowBlock shard
	wb := tw.getWindowBlock(topicHash)
	wb.mu.RLock()
	defer wb.mu.RUnlock()
	var l int
	var expiryCount int

	for key := range wb.entries {
		if key.topicHash != topicHash {
			continue
		}
		wEntries := wb.entries[key]
		if len(wEntries) > 0 {
			l = limit + expiryCount - l
			if len(wEntries) < l {
				l = len(wEntries)
			}
			for _, we := range wEntries[len(wEntries)-l:] {
				if we.isExpired() {
					if err := tw.addExpiry(we); err != nil {
						expiryCount++
						logger.Error().Err(err).Str("context", "timeWindow.addExpiry")
					}
					// if id is expired it does not return an error but continue the iteration
					continue
				}
				winEntries = append(winEntries, we)
			}
		}
	}
	return winEntries
}

// lookup lookups window entries from window file.
func (tw *timeWindowBucket) lookup(topicHash uint64, off, cutoff int64, limit int) (winEntries windowEntries) {
	winEntries = make([]winEntry, 0)
	winEntries = tw.ilookup(topicHash, limit)
	if len(winEntries) >= limit {
		return winEntries
	}
	next := func(off int64, f func(windowHandle) (bool, error)) error {
		for {
			b := windowHandle{file: tw.file, offset: off}
			if err := b.read(); err != nil {
				return err
			}
			if stop, err := f(b); stop || err != nil {
				return err
			}
			if b.next == 0 {
				return nil
			}
			off = b.next
		}
	}
	expiryCount := 0
	err := next(off, func(curb windowHandle) (bool, error) {
		b := &curb
		if b.topicHash != topicHash {
			return true, nil
		}
		if len(winEntries) > limit-int(b.entryIdx) {
			limit = limit - len(winEntries)
			for _, we := range b.entries[b.entryIdx-uint16(limit) : b.entryIdx] {
				if we.isExpired() {
					if err := tw.addExpiry(we); err != nil {
						expiryCount++
						logger.Error().Err(err).Str("context", "timeWindow.addExpiry")
					}
					// if id is expired it does not return an error but continue the iteration
					continue
				}
				winEntries = append(winEntries, we)
			}
			if len(winEntries) >= limit {
				return true, nil
			}
		}
		for _, we := range b.entries[:b.entryIdx] {
			if we.isExpired() {
				if err := tw.addExpiry(we); err != nil {
					expiryCount++
					logger.Error().Err(err).Str("context", "timeWindow.addExpiry")
				}
				// if id is expired it does not return an error but continue the iteration
				continue
			}
			winEntries = append(winEntries, we)

		}
		if b.Cutoff(cutoff) {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return winEntries
	}

	return winEntries
}

func (w winBlock) validation(topicHash uint64) error {
	if w.topicHash != topicHash {
		return fmt.Errorf("timeWindow.write: validation failed block topicHash %d, topicHash %d", w.topicHash, topicHash)
	}
	return nil
}

func (tw *timeWindowBucket) TimeID() int64 {
	return atomic.LoadInt64(&tw.timeID)
}

func (tw *timeWindowBucket) setTimeID(timeID int64) {
	atomic.StoreInt64(&tw.timeID, timeID)
}

func (tw *timeWindowBucket) windowIndex() int32 {
	return tw.windowIdx
}

func (tw *timeWindowBucket) setWindowIndex(windowIdx int32) error {
	tw.windowIdx = windowIdx
	return nil
}
