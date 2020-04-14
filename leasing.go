package tracedb

import (
	"encoding/binary"
	"sort"
	"sync"

	"github.com/unit-io/tracedb/hash"
)

type freeslots struct {
	fs           map[uint64]bool // map[seq]bool
	sync.RWMutex                 // Read Write mutex, guards access to internal collection.
}

// A "thread" safe lease freeblocks.
// To avoid lock bottlenecks slots are divided into several shards (nShards).
type lease struct {
	file
	slots                 []*freeslots
	blocks                []*freeBlocks
	size                  int64 // total size of free blocks
	minimumFreeBlocksSize int64 // minimum free blocks size before free blocks are reused for new allocation.
	consistent            *hash.Consistent
}

type freeblock struct {
	offset int64
	size   uint32
}

type freeBlocks struct {
	fb           []freeblock
	cache        map[int64]bool // cache free offset
	sync.RWMutex                // Read Write mutex, guards access to internal collection.
}

// newLeaswing creates a new concurrent freeblocks.
func newLease(f file, minimumSize int64) *lease {
	l := &lease{
		file:                  f,
		slots:                 make([]*freeslots, nShards),
		blocks:                make([]*freeBlocks, nShards),
		minimumFreeBlocksSize: minimumSize,
		consistent:            hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
		l.slots[i] = &freeslots{fs: make(map[uint64]bool)}
	}

	for i := 0; i < nShards; i++ {
		l.blocks[i] = &freeBlocks{cache: make(map[int64]bool)}
	}

	return l
}

// freeSlots returns freeSlots under given contract
func (l *lease) freeSlots(contract uint64) *freeslots {
	return l.slots[l.consistent.FindBlock(contract)]
}

// get first free seq
func (l *lease) getSlot(contract uint64) (ok bool, seq uint64) {
	// Get shard
	fss := l.freeSlots(contract)
	fss.Lock()
	defer fss.Unlock()
	for seq, ok = range fss.fs {
		delete(fss.fs, seq)
		return ok, seq
	}

	return false, seq
}

func (l *lease) freeSlot(seq uint64) (ok bool) {
	// Get shard
	fss := l.freeSlots(seq)
	fss.Lock()
	defer fss.Unlock()
	if ok := fss.fs[seq]; ok {
		return !ok
	}
	fss.fs[seq] = true
	return true
}

func (fs *freeslots) len() int {
	return len(fs.fs)
}

// freeBlocks returns freeBlocks under given contract
func (l *lease) freeBlocks(contract uint64) *freeBlocks {
	return l.blocks[l.consistent.FindBlock(contract)]
}

func (s *freeBlocks) search(size uint32) int {
	// limit search to first 100 freeblocks
	return sort.Search(100, func(i int) bool {
		return s.fb[i].size >= size
	})
}

// contains checks whether a message id is in the set.
func (s *freeBlocks) contains(off int64) bool {
	for _, v := range s.fb {
		if v.offset == off {
			return true
		}
	}
	return false
}

func (s *freeBlocks) defrag() {
	l := len(s.fb)
	if l <= 1 {
		return
	}
	// limit fragmentation to first 1000 freeblocks
	if l > 1000 {
		l = 1000
	}
	sort.Slice(s.fb[:l], func(i, j int) bool {
		return s.fb[i].offset < s.fb[j].offset
	})
	var merged []freeblock
	curOff := s.fb[0].offset
	curSize := s.fb[0].size
	for i := 1; i < l; i++ {
		if curOff+int64(curSize) == s.fb[i].offset {
			curSize += s.fb[i].size
			delete(s.cache, s.fb[i].offset)
		} else {
			merged = append(merged, freeblock{size: curSize, offset: curOff})
			curOff = s.fb[i].offset
			curSize = s.fb[i].size
		}
	}
	merged = append(merged, freeblock{offset: curOff, size: curSize})
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].size < merged[j].size
	})
	copy(s.fb[:l], merged)
}

func (l *lease) defrag() {
	for i := 0; i < nShards; i++ {
		fbs := l.blocks[i]
		fbs.defrag()
	}
}

func (l *lease) freeBlock(off int64, size uint32) {
	fbs := l.freeBlocks(uint64(off))
	fbs.Lock()
	defer fbs.Unlock()
	// Verify that block is not already free.
	if fbs.cache[off] {
		return
	}
	fbs.fb = append(fbs.fb, freeblock{offset: off, size: size})
	fbs.cache[off] = true
	l.size += int64(size)
}

func (l *lease) free(seq uint64, off int64, size uint32) {
	if size == 0 {
		panic("unable to free zero bytes")
	}
	l.freeSlot(seq)
	l.freeBlock(off, size)
}

func (l *lease) allocate(size uint32) int64 {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	if l.size < l.minimumFreeBlocksSize {
		return -1
	}
	fbs := l.freeBlocks(uint64(size))
	fbs.Lock()
	defer fbs.Unlock()
	if len(fbs.fb) < 100 {
		return -1
	}
	i := fbs.search(size)
	if i >= len(fbs.fb) {
		return -1
	}
	off := fbs.fb[i].offset
	if fbs.fb[i].size == size {
		copy(fbs.fb[i:], fbs.fb[i+1:])
		fbs.fb[len(fbs.fb)-1] = freeblock{}
		fbs.fb = fbs.fb[:len(fbs.fb)-1]
	} else {
		fbs.fb[i].size -= size
		fbs.fb[i].offset += int64(size)
	}
	delete(fbs.cache, off)
	l.size -= int64(size)
	return off
}

// MarshalBinary serializes lease into binary data
func (s *freeBlocks) MarshalBinary() ([]byte, error) {
	size := s.binarySize()
	buf := make([]byte, size)
	data := buf
	binary.LittleEndian.PutUint32(data[:4], uint32(len(s.fb)))
	data = data[4:]
	for i := 0; i < len(s.fb); i++ {
		binary.LittleEndian.PutUint64(data[:8], uint64(s.fb[i].offset))
		binary.LittleEndian.PutUint32(data[8:12], s.fb[i].size)
		data = data[12:]
	}
	return buf, nil
}

func (s *freeBlocks) binarySize() uint32 {
	return uint32((4 + (8+4)*len(s.fb))) // FIXME: this is ugly
}

func (l *lease) read() error {
	var size uint32
	offset := int64(0)
	for i := 0; i < nShards; i++ {
		fbs := l.blocks[i]
		buf := make([]byte, 4)
		if _, err := l.ReadAt(buf, offset); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(buf)
		size += n
		buf = make([]byte, (4+8)*n)
		if _, err := l.ReadAt(buf, offset+4); err != nil {
			return err
		}
		for i := uint32(0); i < n; i++ {
			blockOff := int64(binary.LittleEndian.Uint64(buf[:8]))
			blockSize := binary.LittleEndian.Uint32(buf[8:12])
			if blockOff != 0 {
				fbs.fb = append(fbs.fb, freeblock{size: blockSize, offset: blockOff})
				l.size += int64(blockSize)
			}
			buf = buf[12:]
		}
		offset += int64(12 * n)
	}
	return nil
}

func (l *lease) write() error {
	if len(l.blocks) == 0 {
		return nil
	}
	var marshaledSize uint32
	var buf []byte
	for i := 0; i < nShards; i++ {
		fbs := l.blocks[i]
		// marshaledSize += align(fbs.binarySize())
		marshaledSize += fbs.binarySize()
		data, err := fbs.MarshalBinary()
		buf = append(buf, data...)
		if err != nil {
			return err
		}
	}
	_, err := l.WriteAt(buf, 0)
	return err
}
