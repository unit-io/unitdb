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
	timBlockSize uint32 = 4096
)

type (
	timeEntry struct {
		contract  uint64 // contract is used to query memdb it is not stored into timewindow files
		seq       uint64 //seq specific to time block entries
		entryTime uint32
	}
	timeBlock struct {
		contract    uint64
		topicHash   uint64
		timeEntries [seqsPerTimeBlock]timeEntry
		next        int64 //next stores offset that links multiple timeblocks for a topic hash. Most recent offset is stored into trie (to iterate entries in reverse order)
		entryIdx    uint16
	}
)

type timeHandle struct {
	timeBlock
	file   file
	offset int64
}

func timeBlockOffset(idx int32) int64 {
	return (int64(blockSize) * int64(idx))
}

func (e timeEntry) time() uint32 {
	return e.entryTime
}

func (e timeEntry) Seq() uint64 {
	return e.seq
}

// MarshalBinary serliazed time block into binary data
func (b timeBlock) MarshalBinary() ([]byte, error) {
	buf := make([]byte, blockSize)
	data := buf
	for i := 0; i < seqsPerTimeBlock; i++ {
		e := b.timeEntries[i]
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
func (b *timeBlock) UnmarshalBinary(data []byte) error {
	for i := 0; i < seqsPerTimeBlock; i++ {
		_ = data[8] // bounds check hint to compiler; see golang.org/issue/14808
		b.timeEntries[i].seq = binary.LittleEndian.Uint64(data[:8])
		data = data[8:]
	}
	b.contract = binary.LittleEndian.Uint64(data[:8])
	b.topicHash = binary.LittleEndian.Uint64(data[8:16])
	b.next = int64(binary.LittleEndian.Uint64(data[16:24]))
	b.entryIdx = binary.LittleEndian.Uint16(data[24:26])
	return nil
}

func (h *timeHandle) read() error {
	buf, err := h.file.Slice(h.offset, h.offset+int64(blockSize))
	if err != nil {
		return err
	}
	return h.UnmarshalBinary(buf)
}

func (h *timeHandle) write() error {
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
	blocks map[int64]timeWindow    // map[entryHash]timeWindow
	mu     sync.RWMutex            // Read Write mutex, guards access to internal collection.
}

// newTimeWindows creates a new concurrent timeWindows.
func newTimeWindows() *timeWindows {
	w := &timeWindows{
		windows:    make([]*windows, nShards),
		consistent: hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
		w.windows[i] = &windows{expiry: make(map[int64]windowEntries), blocks: make(map[int64]timeWindow)}
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

// newBlock adds new block to timwWindowBucket and returns block offset
func (wb *timeWindowBucket) newBlock() (int64, error) {
	off, err := wb.extend(blockSize)
	if err != nil {
		return off, err
	}
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

// addExpiry adds expiry for first timeblock. Entries expires in future is not added yet to expiry window
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
	entryTime := int64(time.Unix(int64(e.time()), 0).Truncate(wb.opts.expDurationType).Add(1 * wb.opts.expDurationType).Unix())

	// get windows shard
	ws := wb.getWindows(uint64(entryTime))
	ws.mu.Lock()
	defer ws.mu.Unlock()

	block, ok := ws.blocks[entryTime]
	if block.freezed {
		if ok {
			if _, ok := block.friezedEntries[topicHash]; ok {
				ws.blocks[entryTime].friezedEntries[topicHash] = append(ws.blocks[entryTime].friezedEntries[topicHash], e)
			} else {
				entries := map[uint64][]timeWindowEntry{}
				entries[topicHash] = []timeWindowEntry{e}
				ws.blocks[entryTime].friezedEntries[topicHash] = entries[topicHash]
			}
		} else {
			windowEntries := map[uint64]windowEntries{}
			windowEntries[topicHash] = []timeWindowEntry{e}
			ws.blocks[entryTime] = timeWindow{friezedEntries: windowEntries}
		}
		return nil
	}
	if ok {
		if _, ok := block.entries[topicHash]; ok {
			ws.blocks[entryTime].entries[topicHash] = append(ws.blocks[entryTime].entries[topicHash], e)
		} else {
			entries := map[uint64][]timeWindowEntry{}
			entries[topicHash] = []timeWindowEntry{e}
			ws.blocks[entryTime].entries[topicHash] = entries[topicHash]
		}
	} else {
		windowEntries := map[uint64]windowEntries{}
		windowEntries[topicHash] = []timeWindowEntry{e}
		ws.blocks[entryTime] = timeWindow{entries: windowEntries}
	}
	return nil
}

func (w *timeWindow) freeze() error {
	w.freezed = true
	return nil
}

func (w *timeWindow) unFreeze() error {
	w.freezed = false
	w.entries = make(map[uint64]windowEntries)
	for h, _ := range w.friezedEntries {
		w.entries[h] = w.friezedEntries[h]
	}
	w.friezedEntries = make(map[uint64]windowEntries)
	return nil
}

// foreachTimeWindow iterates timewindow entries during sync or recovery process to write entries to timewindow files
// it takes timewindow snapshot to iterate and deletes blocks from timewindow
func (wb *timeWindowBucket) foreachTimeWindow(freeze bool, f func(timeHash int64, w map[uint64]windowEntries) (bool, error)) (err error) {
	for i := 0; i < nShards; i++ {
		ws := wb.timeWindows.windows[i]
		windowTimes := make([]int64, 0, len(ws.blocks))
		for windowTime := range ws.blocks {
			windowTimes = append(windowTimes, windowTime)
		}
		for j := 0; j < len(windowTimes); j++ {
			window := ws.blocks[windowTimes[j]]
			if freeze {
				ws.mu.Lock()
				window.freeze()
				ws.mu.Unlock()
			}
			if stop, err1 := f(windowTimes[j], window.entries); stop || err1 != nil {
				err = err1
				continue
			}
			if windowTimes[j] == 0 {
				for _, windowEntries := range window.entries {
					for _, te := range windowEntries {
						if err := wb.addExpiry(te); err != nil {
							return err
						}
					}
				}
			}
			if freeze {
				ws.mu.Lock()
				window.unFreeze()
				ws.mu.Unlock()
			}
		}
	}
	return nil
}

// foreachTimeBlock iterates timeblocks on init db to store topic hash and last offset of topic in a timeWindow file into trie.
func (wb *timeWindowBucket) foreachTimeBlock(f func(timeHandle) (bool, error)) (err error) {
	timeBlockIdx := int32(0)
	nTimeBlocks := wb.getTimeWindowIdx() + 1
	for timeBlockIdx < nTimeBlocks {
		off := timeBlockOffset(timeBlockIdx)
		b := timeHandle{file: wb.file, offset: off}
		if err := b.read(); err != nil {
			return err
		}
		if stop, err := f(b); stop || err != nil {
			return err
		}
		timeBlockIdx++
	}
	return nil
}

// getTimeBlockHandle reads timeblock from timewindow file for windowIdx (file num) and a topic.
// If offset is zero then its a new topic so it create a new timeblock to the end of timewindow file
func (wb *timeWindowBucket) getTimeBlockHandle(topicHash uint64, off int64) (timeHandle, error) {
	var err error
	if off == 0 {
		if off, err = wb.newBlock(); err != nil {
			return timeHandle{}, err
		}
	}
	b := timeHandle{file: wb.file, offset: off}
	if err := b.read(); err != nil {
		return b, err
	}
	if b.topicHash == 0 {
		b.topicHash = topicHash
	}
	return b, nil
}

func (wb *timeWindowBucket) lookup(topicHash uint64, offs []int64, limit uint32) (timeEntries []timeEntry, fanout bool) {
	timeEntries = make([]timeEntry, 0)
	next := func(off int64, f func(timeHandle) (bool, error)) error {
		for {
			b := timeHandle{file: wb.file, offset: off}
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
	err := wb.foreachTimeWindow(false, func(timeHash int64, windowEntries map[uint64]windowEntries) (bool, error) {
		entries := windowEntries[topicHash]
		for _, te := range entries {
			timeEntries = append(timeEntries, te.(timeEntry))

		}
		return false, nil
	})
	if err != nil {
		return timeEntries, false
	}
	if uint32(len(timeEntries)) > limit {
		return timeEntries, false
	}
	for _, off := range offs {
		err := next(off, func(curb timeHandle) (bool, error) {
			b := &curb
			timeEntries = append(timeEntries, b.timeEntries[:]...)
			if uint32(len(timeEntries)) > limit {
				return true, nil
			}
			return false, nil
		})
		if err != nil {
			return timeEntries, false
		}
	}
	return timeEntries, len(timeEntries) < int(limit)
}

// write writes timewidnow entries into timewindow files
func (wb *timeWindowBucket) write(b *timeHandle, te timeWindowEntry) (off int64, err error) {
	entryIdx := 0
	e := te.(timeEntry)
	for i := 0; i < seqsPerTimeBlock; i++ {
		entry := b.timeEntries[i]
		if entry.seq == e.seq { //record exist in db
			entryIdx = -1
			break
		}
	}
	if entryIdx == -1 {
		return 0, nil
	}
	if b.entryIdx == seqsPerTimeBlock {
		off, err = wb.newBlock()
		if err != nil {
			return 0, err
		}
		topicHash := b.topicHash
		next := b.offset
		*b = timeHandle{file: wb.file, offset: off}
		b.topicHash = topicHash
		b.next = next
		wb.nextTimeWindowIdx()
		// fmt.Println("timewindow.write: off ", off)
	}

	b.timeEntries[b.entryIdx] = timeEntry{
		seq: e.seq,
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
