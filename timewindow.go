package tracedb

import (
	"encoding/binary"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/tracedb/hash"
)

const (
	winBlockSize uint32 = 4096
)

type (
	winEntry struct {
		contract uint64
		seq      uint64 //seq specific to time block entries
	}
	winBlock struct {
		contract   uint64
		topicHash  uint64
		winEntries [seqsPerWindowBlock]winEntry
		next       int64 //next stores offset that links multiple winBlocks for a topic hash. Most recent offset is stored into the trie (to iterate entries in reverse order)
		entryIdx   uint16
	}
)

type windowHandle struct {
	winBlock
	file   file
	offset int64
}

func winBlockOffset(idx int32) int64 {
	return (int64(blockSize) * int64(idx))
}

func (e winEntry) time() uint32 {
	return 0
}

func (e winEntry) Seq() uint64 {
	return e.seq
}

// MarshalBinary serliazed time block into binary data
func (b winBlock) MarshalBinary() ([]byte, error) {
	buf := make([]byte, blockSize)
	data := buf
	for i := 0; i < seqsPerWindowBlock; i++ {
		e := b.winEntries[i]
		binary.LittleEndian.PutUint64(buf[:8], e.seq)
		buf = buf[8:]
	}
	binary.LittleEndian.PutUint64(buf[:8], b.contract)
	binary.LittleEndian.PutUint64(buf[8:16], b.topicHash)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(b.next))
	binary.LittleEndian.PutUint16(buf[24:26], b.entryIdx)
	return data, nil
}

// UnmarshalBinary dserliazed time block from binary data
func (b *winBlock) UnmarshalBinary(data []byte) error {
	for i := 0; i < seqsPerWindowBlock; i++ {
		_ = data[8] // bounds check hint to compiler; see golang.org/issue/14808
		b.winEntries[i].seq = binary.LittleEndian.Uint64(data[:8])
		data = data[8:]
	}
	b.contract = binary.LittleEndian.Uint64(data[:8])
	b.topicHash = binary.LittleEndian.Uint64(data[8:16])
	b.next = int64(binary.LittleEndian.Uint64(data[16:24]))
	b.entryIdx = binary.LittleEndian.Uint16(data[24:26])
	return nil
}

func (h *windowHandle) read() error {
	buf, err := h.file.Slice(h.offset, h.offset+int64(blockSize))
	if err != nil {
		return err
	}
	return h.UnmarshalBinary(buf)
}

func (h *windowHandle) write() error {
	buf, err := h.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = h.file.WriteAt(buf, h.offset)

	return err
}

// A "thread" safe timeWindows.
// To avoid lock bottlenecks timeWindows are dived to several (nShards).
type timeWindows struct {
	sync.RWMutex
	windows    []*windows
	consistent *hash.Consistent
}

type windows struct {
	expiry map[int64]windowEntries // map[expiryHash]windowEntries
	timeWindow
	mu sync.RWMutex // Read Write mutex, guards access to internal collection.
}

// newTimeWindows creates a new concurrent timeWindows.
func newTimeWindows() *timeWindows {
	w := &timeWindows{
		windows:    make([]*windows, nShards),
		consistent: hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
		w.windows[i] = &windows{expiry: make(map[int64]windowEntries), timeWindow: timeWindow{friezedEntries: make(map[uint64]windowEntries), entries: make(map[uint64]windowEntries)}}
	}

	return w
}

// getWindows returns shard under given key
func (w *timeWindows) getWindows(key uint64) *windows {
	w.RLock()
	defer w.RUnlock()
	return w.windows[w.consistent.FindBlock(key)]
}

type timeWindowEntry interface {
	Seq() uint64
	time() uint32
}

type windowEntries []timeWindowEntry
type timeWindow struct {
	freezed        bool
	entries        map[uint64]windowEntries // map[topicHash]windowEntries
	friezedEntries map[uint64]windowEntries
}

type (
	timeOptions struct {
		expDurationType    time.Duration
		maxExpDurations    int
		windowDurationType time.Duration
		maxWindowDurations int
	}
	timeWindowBucket struct {
		sync.RWMutex
		file
		*timeWindows
		timeWindowIdx      int32
		earliestExpiryHash int64
		opts               *timeOptions
	}
)

func (src *timeOptions) copyWithDefaults() *timeOptions {
	opts := timeOptions{}
	if src != nil {
		opts = *src
	}
	if opts.expDurationType == 0 {
		opts.expDurationType = time.Minute
	}
	if opts.windowDurationType == 0 {
		opts.expDurationType = time.Hour
	}
	if opts.maxExpDurations == 0 {
		opts.maxExpDurations = 1
	}
	if opts.maxWindowDurations == 0 {
		opts.maxWindowDurations = 24
	}
	return &opts
}

func newTimeWindowBucket(f file, opts *timeOptions) *timeWindowBucket {
	l := &timeWindowBucket{file: f, timeWindowIdx: -1}
	l.timeWindows = newTimeWindows()
	l.opts = opts.copyWithDefaults()
	return l
}

// newBlock adds new block to windowBlock and returns block offset
func (wb *timeWindowBucket) newBlock() (int64, error) {
	off, err := wb.extend(blockSize)
	if err != nil {
		return off, err
	}
	wb.nextTimeWindowIdx()
	return off, err
}

func (wb *timeWindowBucket) expireOldEntries(maxResults int) []timeWindowEntry {
	var expiredEntries []timeWindowEntry
	startTime := uint32(time.Now().Unix())

	if atomic.LoadInt64(&wb.earliestExpiryHash) > int64(startTime) {
		return expiredEntries
	}

	for i := 0; i < nShards; i++ {
		// get windows shard
		ws := wb.timeWindows.windows[i]
		ws.mu.Lock()
		defer ws.mu.Unlock()
		windowTimes := make([]int64, 0, len(ws.expiry))
		for windowTime := range ws.expiry {
			windowTimes = append(windowTimes, windowTime)
		}
		sort.Slice(windowTimes[:], func(i, j int) bool { return windowTimes[i] < windowTimes[j] })
		for i := 0; i < len(windowTimes); i++ {
			if windowTimes[i] > int64(startTime) || len(expiredEntries) > maxResults {
				break
			}
			windowEntries := ws.expiry[windowTimes[i]]
			expiredEntriesCount := 0
			for i := range windowEntries {
				entry := windowEntries[i]
				if entry.time() < startTime {
					expiredEntries = append(expiredEntries, entry)
					expiredEntriesCount++
				}
			}
			if expiredEntriesCount == len(windowEntries) {
				delete(ws.expiry, windowTimes[i])
			}
		}
	}
	return expiredEntries
}

// addExpiry adds expiry for first winBlock. Entries expires in future is not added yet to expiry window
func (wb *timeWindowBucket) addExpiry(e timeWindowEntry) error {
	timeExpiry := int64(time.Unix(int64(e.time()), 0).Truncate(wb.opts.expDurationType).Add(1 * wb.opts.expDurationType).Unix())
	atomic.CompareAndSwapInt64(&wb.earliestExpiryHash, 0, timeExpiry)

	// get windows shard
	ws := wb.getWindows(uint64(e.time()))
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if expiryWindow, ok := ws.expiry[timeExpiry]; ok {
		expiryWindow = append(expiryWindow, e)
		ws.expiry[timeExpiry] = expiryWindow
	} else {
		ws.expiry[timeExpiry] = windowEntries{e}
		if atomic.LoadInt64(&wb.earliestExpiryHash) > timeExpiry {
			wb.earliestExpiryHash = timeExpiry
		}
	}

	return nil
}

func (wb *timeWindowBucket) add(topicHash uint64, e timeWindowEntry) error {
	// get windows shard
	ws := wb.getWindows(topicHash)
	ws.mu.Lock()
	defer ws.mu.Unlock()

	if ws.freezed {
		if _, ok := ws.friezedEntries[topicHash]; ok {
			ws.friezedEntries[topicHash] = append(ws.friezedEntries[topicHash], e)
		} else {
			ws.friezedEntries[topicHash] = []timeWindowEntry{e}
		}
		return nil
	}
	if _, ok := ws.entries[topicHash]; ok {
		ws.entries[topicHash] = append(ws.entries[topicHash], e)
	} else {
		ws.entries[topicHash] = []timeWindowEntry{e}
	}
	return nil
}

func (w *timeWindow) reset() error {
	w.entries = make(map[uint64]windowEntries)
	return nil
}

func (w *timeWindow) freeze() error {
	w.freezed = true
	return nil
}

func (w *timeWindow) unFreeze() error {
	w.freezed = false
	for h, _ := range w.friezedEntries {
		w.entries[h] = append(w.entries[h], w.friezedEntries[h]...)
	}
	w.friezedEntries = make(map[uint64]windowEntries)
	return nil
}

// foreachTimeWindow iterates timewindow entries during sync or recovery process to write entries to timewindow files
// it takes timewindow snapshot to iterate and deletes blocks from timewindow
func (wb *timeWindowBucket) foreachTimeWindow(freeze bool, f func(w map[uint64]windowEntries) (bool, error)) (err error) {
	for i := 0; i < nShards; i++ {
		ws := wb.timeWindows.windows[i]
		ws.mu.RLock()
		wEntries := make(map[uint64]windowEntries)
		if freeze {
			ws.freeze()
		}
		for h, entries := range ws.entries {
			wEntries[h] = entries
		}
		ws.mu.RUnlock()
		if stop, err1 := f(wEntries); stop || err1 != nil {
			err = err1
			if freeze {
				ws.mu.Lock()
				ws.unFreeze()
				ws.mu.Unlock()
			}
			continue
		}
		if freeze {
			ws.mu.Lock()
			ws.reset()
			ws.unFreeze()
			ws.mu.Unlock()
		}
	}
	return nil
}

// foreachwinBlock iterates winBlocks on init db to store topic hash and last offset of topic in a timeWindow file into trie.
func (wb *timeWindowBucket) foreachWindowBlock(f func(windowHandle) (bool, error)) (err error) {
	winBlockIdx := int32(0)
	nwinBlocks := wb.getTimeWindowIdx() + 1
	for winBlockIdx < nwinBlocks {
		off := winBlockOffset(winBlockIdx)
		b := windowHandle{file: wb.file, offset: off}
		if err := b.read(); err != nil {
			return err
		}
		if stop, err := f(b); stop || err != nil {
			return err
		}
		winBlockIdx++
	}
	return nil
}

// getwinBlockHandle reads winBlock from timewindow file for windowIdx (file num) and a topic.
// If offset is zero then its a new topic so it create a new winBlock to the end of timewindow file
func (wb *timeWindowBucket) getWindowBlockHandle(topicHash uint64, off int64) (windowHandle, error) {
	var err error
	if off == 0 {
		if off, err = wb.newBlock(); err != nil {
			return windowHandle{}, err
		}
	}
	b := windowHandle{file: wb.file, offset: off}
	if err := b.read(); err != nil {
		return b, err
	}
	if b.topicHash == 0 {
		b.topicHash = topicHash
	}
	return b, nil
}

// ilookup lookups window entries timeWindowBucket and not yet sync to db
func (wb *timeWindowBucket) ilookup(topicHash uint64, limit uint32) (winEntries []winEntry, fanout bool) {
	winEntries = make([]winEntry, 0)
	for i := 0; i < nShards; i++ {
		ws := wb.timeWindows.windows[i]
		ws.mu.RLock()
		wEntries := ws.friezedEntries[topicHash]
		for _, we := range wEntries {
			winEntries = append(winEntries, we.(winEntry))
		}
		wEntries = ws.entries[topicHash]
		for _, we := range wEntries {
			winEntries = append(winEntries, we.(winEntry))
		}
		ws.mu.RUnlock()
		if uint32(len(winEntries)) > limit {
			return winEntries, false
		}
	}
	return winEntries, len(winEntries) < int(limit)
}

func (wb *timeWindowBucket) lookup(topicHash uint64, offs []int64, skip int, limit uint32) (winEntries []winEntry, fanout bool) {
	winEntries = make([]winEntry, 0)
	next := func(off int64, f func(windowHandle) (bool, error)) error {
		for {
			b := windowHandle{file: wb.file, offset: off}
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
	count := 0
	for _, off := range offs {
		err := next(off, func(curb windowHandle) (bool, error) {
			b := &curb
			if skip > count+seqsPerWindowBlock {
				count += seqsPerWindowBlock
				return false, nil
			}
			winEntries = append(winEntries, b.winEntries[:b.entryIdx]...)
			if uint32(len(winEntries)) > limit {
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return winEntries, false
		}
	}
	return winEntries, len(winEntries) < int(limit)
}

// write writes timewidnow entries into timewindow files
func (wb *timeWindowBucket) write(b *windowHandle, we timeWindowEntry) (off int64, err error) {
	entryIdx := 0
	// e := we.(winEntry)
	for i := 0; i < seqsPerWindowBlock; i++ {
		entry := b.winEntries[i]
		if entry.seq == we.Seq() { //record exist in db
			entryIdx = -1
			break
		}
	}
	if entryIdx == -1 {
		return 0, nil
	}
	if b.entryIdx == seqsPerWindowBlock {
		off, err = wb.newBlock()
		if err != nil {
			return 0, err
		}
		topicHash := b.topicHash
		next := b.offset
		*b = windowHandle{file: wb.file, offset: off}
		b.topicHash = topicHash
		b.next = next
		wb.nextTimeWindowIdx()
	}

	b.winEntries[b.entryIdx] = winEntry{
		seq: we.Seq(),
	}
	b.entryIdx++

	if err := b.write(); err != nil {
		return 0, err
	}
	return off, nil
}

func (wb *timeWindowBucket) getTimeWindowIdx() int32 {
	return wb.timeWindowIdx
}

func (wb *timeWindowBucket) setTimeWindowIdx(timeWindowIdx int32) error {
	wb.timeWindowIdx = timeWindowIdx
	return nil
}

func (wb *timeWindowBucket) nextTimeWindowIdx() int32 {
	wb.timeWindowIdx++
	return wb.timeWindowIdx
}
