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
	"sync"
	"time"

	"github.com/unit-io/unitdb/hash"
)

type (
	_WinEntry struct {
		sequence  uint64
		expiresAt uint32
	}
	_WinBlock struct {
		topicHash  uint64
		entries    [entriesPerWindowBlock]_WinEntry
		next       int64 //next stores offset that links multiple winBlocks for a topic hash. Most recent offset is stored into the trie to iterate entries in reverse order).
		cutoffTime int64
		entryIdx   uint16

		dirty  bool // dirty used during timeWindow append and not persisted.
		leased bool // leased used in timeWindow write and not persisted.
	}
)

func newWinEntry(seq uint64, expiresAt uint32) _WinEntry {
	return _WinEntry{sequence: seq, expiresAt: expiresAt}
}

func (e _WinEntry) seq() uint64 {
	return e.sequence
}

func (e _WinEntry) expiryTime() uint32 {
	return e.expiresAt
}

func (e _WinEntry) isExpired() bool {
	return e.expiresAt != 0 && e.expiresAt <= uint32(time.Now().Unix())
}

func (b _WinBlock) cutoff(cutoff int64) bool {
	return b.cutoffTime != 0 && b.cutoffTime < cutoff
}

// MarshalBinary serialized window block into binary data.
func (b _WinBlock) MarshalBinary() []byte {
	buf := make([]byte, blockSize)
	data := buf
	for i := 0; i < entriesPerWindowBlock; i++ {
		e := b.entries[i]
		binary.LittleEndian.PutUint64(buf[:8], e.sequence)
		binary.LittleEndian.PutUint32(buf[8:12], e.expiresAt)
		buf = buf[12:]
	}
	binary.LittleEndian.PutUint64(buf[:8], uint64(b.cutoffTime))
	binary.LittleEndian.PutUint64(buf[8:16], b.topicHash)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(b.next))
	binary.LittleEndian.PutUint16(buf[24:26], b.entryIdx)
	return data
}

// UnmarshalBinary de-serialized window block from binary data.
func (b *_WinBlock) UnmarshalBinary(data []byte) error {
	for i := 0; i < entriesPerWindowBlock; i++ {
		_ = data[12] // bounds check hint to compiler; see golang.org/issue/14808.
		b.entries[i].sequence = binary.LittleEndian.Uint64(data[:8])
		b.entries[i].expiresAt = binary.LittleEndian.Uint32(data[8:12])
		data = data[12:]
	}
	b.cutoffTime = int64(binary.LittleEndian.Uint64(data[:8]))
	b.topicHash = binary.LittleEndian.Uint64(data[8:16])
	b.next = int64(binary.LittleEndian.Uint64(data[16:24]))
	b.entryIdx = binary.LittleEndian.Uint16(data[24:26])
	return nil
}

type _WindowHandle struct {
	winBlock _WinBlock
	file     _File
	offset   int64
}

func winBlockOffset(idx int32) int64 {
	return (int64(blockSize) * int64(idx))
}

func (h *_WindowHandle) read() error {
	buf, err := h.file.Slice(h.offset, h.offset+int64(blockSize))
	if err != nil {
		return err
	}
	return h.winBlock.UnmarshalBinary(buf)
}

type (
	_TimeOptions struct {
		maxDuration         time.Duration
		expDurationType     time.Duration
		maxExpDurations     int
		backgroundKeyExpiry bool
	}
	_TimeInfo struct {
		windowIdx int32
	}
	_TimeWindowBucket struct {
		sync.RWMutex
		file               _File
		timeInfo           _TimeInfo
		timeIDs            map[int64]struct{}
		windowBlocks       *_WindowBlocks
		expiryWindowBucket *_ExpiryWindowBucket
		opts               *_TimeOptions
	}
)

func (src *_TimeOptions) copyWithDefaults() *_TimeOptions {
	opts := _TimeOptions{}
	if src != nil {
		opts = *src
	}
	if opts.maxDuration == 0 {
		opts.maxDuration = 1 * time.Second
	}
	if opts.expDurationType == 0 {
		opts.expDurationType = time.Minute
	}
	if opts.maxExpDurations == 0 {
		opts.maxExpDurations = 1
	}
	return &opts
}

type _WindowEntries []_WinEntry
type _Key struct {
	timeID    int64
	topicHash uint64
}
type _TimeWindow struct {
	mu      sync.RWMutex
	entries map[_Key]_WindowEntries
}

// A "thread" safe windowBlocks.
// To avoid lock bottlenecks windowBlocks are divided into several shards (nShards).
type _WindowBlocks struct {
	sync.RWMutex
	window     []*_TimeWindow
	consistent *hash.Consistent
}

// newWindowBlocks creates a new concurrent windows.
func newWindowBlocks() *_WindowBlocks {
	wb := &_WindowBlocks{
		window:     make([]*_TimeWindow, nShards),
		consistent: hash.InitConsistent(nShards, nShards),
	}

	for i := 0; i < nShards; i++ {
		wb.window[i] = &_TimeWindow{entries: make(map[_Key]_WindowEntries)}
	}

	return wb
}

// getWindowBlock returns shard under given blockID.
func (w *_WindowBlocks) getWindowBlock(blockID uint64) *_TimeWindow {
	w.RLock()
	defer w.RUnlock()
	return w.window[w.consistent.FindBlock(blockID)]
}

func newTimeWindowBucket(f _File, windowIdx int32, opts *_TimeOptions) *_TimeWindowBucket {
	opts = opts.copyWithDefaults()
	l := &_TimeWindowBucket{file: f, timeInfo: _TimeInfo{windowIdx: windowIdx}, timeIDs: make(map[int64]struct{})}
	l.windowBlocks = newWindowBlocks()
	l.expiryWindowBucket = newExpiryWindowBucket(opts.backgroundKeyExpiry, opts.expDurationType, opts.maxExpDurations)
	l.opts = opts.copyWithDefaults()
	return l
}

func (tw *_TimeWindowBucket) add(timeID int64, topicHash uint64, e _WinEntry) (ok bool) {
	// get windowBlock shard.
	tw.RLock()
	b := tw.windowBlocks.getWindowBlock(topicHash)
	tw.RUnlock()
	b.mu.Lock()
	defer b.mu.Unlock()

	key := _Key{
		timeID:    timeID,
		topicHash: topicHash,
	}

	if _, ok := b.entries[key]; ok {
		b.entries[key] = append(b.entries[key], e)
	} else {
		b.entries[key] = _WindowEntries{e}
		tw.timeIDs[timeID] = struct{}{}
	}
	return true
}
func (tw *_TimeWindowBucket) release() func(timeID int64) error {
	releasedKeys := make(map[int64][]_Key)
	for i := 0; i < nShards; i++ {
		wb := tw.windowBlocks.window[i]
		wb.mu.RLock()
		for k := range wb.entries {
			if _, ok := releasedKeys[k.timeID]; ok {
				releasedKeys[k.timeID] = append(releasedKeys[k.timeID], k)
			} else {
				releasedKeys[k.timeID] = []_Key{k}
			}
		}
		wb.mu.RUnlock()
	}

	return func(timeID int64) error {
		keys, ok := releasedKeys[timeID]
		if !ok {
			return errBadRequest
		}
		for _, k := range keys {
			b := tw.windowBlocks.getWindowBlock(k.topicHash)
			b.mu.Lock()
			delete(b.entries, k)
			b.mu.Unlock()
		}

		return nil
	}
}

// foreachWindowBlock iterates winBlocks on DB init to store topic hash and last offset of topic into trie.
func (tw *_TimeWindowBucket) foreachWindowBlock(f func(startSeq, topicHash uint64, off int64) (bool, error)) (err error) {
	winBlockIdx := int32(0)
	nWinBlocks := tw.windowIndex()
	for winBlockIdx <= nWinBlocks {
		off := winBlockOffset(winBlockIdx)
		h := _WindowHandle{file: tw.file, offset: off}
		if err := h.read(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		winBlockIdx++
		if h.winBlock.entryIdx == 0 || h.winBlock.next != 0 {
			continue
		}
		// fmt.Println("timeWindow.foreachTimeBlock: topicHash, seq ", h.winBlock.topicHash, h.winBlock.entries[0].sequence)
		if stop, err := f(h.winBlock.entries[0].sequence, h.winBlock.topicHash, h.offset); stop || err != nil {
			return err
		}
	}
	return nil
}

// ilookup lookups window entries from timeWindowBucket and not yet sync to DB.
func (tw *_TimeWindowBucket) ilookup(topicHash uint64, limit int) (winEntries _WindowEntries) {
	winEntries = make([]_WinEntry, 0)
	// get windowBlock shard.
	b := tw.windowBlocks.getWindowBlock(topicHash)
	b.mu.RLock()
	defer b.mu.RUnlock()
	var l int
	var expiryCount int

	for key := range b.entries {
		if key.topicHash != topicHash {
			continue
		}
		wEntries := b.entries[key]
		if len(wEntries) > 0 {
			l = limit + expiryCount - l
			if len(wEntries) < l {
				l = len(wEntries)
			}
			for i := len(wEntries) - 1; i >= len(wEntries)-l; i-- {
				we := wEntries[i]
				if we.isExpired() {
					if err := tw.expiryWindowBucket.addExpiry(we); err != nil {
						expiryCount++
						logger.Error().Err(err).Str("context", "timeWindow.addExpiry")
					}
					// if id is expired it does not return an error but continue the iteration.
					continue
				}
				winEntries = append(winEntries, we)
			}
		}
	}
	return winEntries
}

// lookup lookups window entries from window file.
func (tw *_TimeWindowBucket) lookup(topicHash uint64, off, cutoff int64, limit int) (winEntries _WindowEntries) {
	winEntries = make([]_WinEntry, 0)
	winEntries = tw.ilookup(topicHash, limit)
	if len(winEntries) >= limit {
		return winEntries
	}
	next := func(blockOff int64, f func(_WindowHandle) (bool, error)) error {
		for {
			b := _WindowHandle{file: tw.file, offset: blockOff}
			if err := b.read(); err != nil {
				return err
			}
			if stop, err := f(b); stop || err != nil {
				return err
			}
			if b.winBlock.next == 0 {
				return nil
			}
			blockOff = b.winBlock.next
		}
	}
	expiryCount := 0
	err := next(off, func(curb _WindowHandle) (bool, error) {
		b := &curb
		if b.winBlock.topicHash != topicHash {
			return true, nil
		}
		if len(winEntries) > limit-int(b.winBlock.entryIdx) {
			limit = limit - len(winEntries)
			for i := len(b.winBlock.entries) - 1; i >= len(b.winBlock.entries)-limit; i-- {
				we := b.winBlock.entries[i]
				if we.isExpired() {
					if err := tw.expiryWindowBucket.addExpiry(we); err != nil {
						expiryCount++
						logger.Error().Err(err).Str("context", "timeWindow.addExpiry")
					}
					// if id is expired it does not return an error but continue the iteration.
					continue
				}
				winEntries = append(winEntries, we)
			}
			if len(winEntries) >= limit {
				return true, nil
			}
		}
		for i := len(b.winBlock.entries) - 1; i >= 0; i-- {
			we := b.winBlock.entries[i]
			if we.isExpired() {
				if err := tw.expiryWindowBucket.addExpiry(we); err != nil {
					expiryCount++
					logger.Error().Err(err).Str("context", "timeWindow.addExpiry")
				}
				// if id is expired it does not return an error but continue the iteration.
				continue
			}
			winEntries = append(winEntries, we)

		}
		if b.winBlock.cutoff(cutoff) {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		return winEntries
	}

	return winEntries
}

func (b _WinBlock) validation(topicHash uint64) error {
	if b.topicHash != topicHash {
		return fmt.Errorf("timeWindow.write: validation failed block topicHash %d, topicHash %d", b.topicHash, topicHash)
	}
	return nil
}

func (tw *_TimeWindowBucket) windowIndex() int32 {
	return tw.timeInfo.windowIdx
}

// func (tw *_TimeWindowBucket) setWindowIndex(windowIdx int32) error {
// 	tw.timeInfo.windowIdx = windowIdx
// 	return nil
// }
