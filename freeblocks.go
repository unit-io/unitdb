package tracedb

import (
	"encoding/binary"
	"sort"
	"sync"

	"github.com/unit-io/tracedb/hash"
)

const (
	nShards = 271 // TODO implelemt sharding based on total Contracts in db
)

// A "thread" safe freeslot.
// To avoid lock bottlenecks slots are dived to several (nShards).
// type freeslots []*freeslot

type freeslots struct {
	slots      []*freeslot
	consistent *hash.Consistent
}

type freeslot struct {
	seqs []uint64 // seq holds free sequences
	// sync.RWMutex          // Read Write mutex, guards access to internal collection.
}

// newFreeSlots creates a new concurrent free slots.
func newFreeSlots() freeslots {
	s := freeslots{
		slots:      make([]*freeslot, nShards+1),
		consistent: hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i <= nShards; i++ {
		s.slots[i] = &freeslot{}
	}

	return s
}

// getShard returns shard under given prefix
func (fss *freeslots) getShard(prefix uint64) *freeslot {
	return fss.slots[fss.consistent.FindBlock(prefix)]
}

// TODO implement btree+ search
// contains checks whether a message id is in the set.
func (fs *freeslot) contains(value uint64) bool {
	for _, v := range fs.seqs {
		if v == value {
			return true
		}
	}
	return false
}

// get first free seq
func (fss *freeslots) get(prefix uint64) (ok bool, seq uint64) {
	// Get shard
	shard := fss.getShard(prefix)
	// shard.RLock()
	// defer shard.RUnlock()
	// Get item from shard.
	if len(shard.seqs) == 0 {
		return ok, seq
	}

	seq = shard.seqs[0]
	shard.seqs = shard.seqs[1:]
	return true, seq
}

func (fss *freeslots) free(seq uint64) (ok bool) {
	// queue to last shard
	fss.slots[nShards].seqs = append(fss.slots[nShards].seqs, seq)
	go func() {
		if len(fss.slots[nShards].seqs) < 100 {
			return
		}
		for _, s := range fss.slots[nShards].seqs {
			// Get shard
			shard := fss.getShard(s)
			if shard.contains(s) == false {
				shard.seqs = append(shard.seqs, s)
				ok = true
			}
		}
	}()
	return
}

func (fs *freeslot) len() int {
	return len(fs.seqs)
}

// A "thread" safe freeslot.
// To avoid lock bottlenecks slots are dived to several (nShards).
// type freeblocks []*freeblock

type freeblocks struct {
	blocks                []*shard
	size                  int64 // total size of free blocks
	minimumFreeBlocksSize int64 // minimum free blocks size to allocate free blocks and reuse it.
	consistent            *hash.Consistent
}

type freeblock struct {
	offset int64
	size   uint32
}

type shard struct {
	blocks       []freeblock
	sync.RWMutex // Read Write mutex, guards access to internal collection.
}

// newFreeBlocks creates a new concurrent freeblocks.
func newFreeBlocks(minimumSize int64) freeblocks {
	fb := freeblocks{
		blocks:                make([]*shard, nShards+1),
		minimumFreeBlocksSize: minimumSize,
		consistent:            hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i <= nShards; i++ {
		fb.blocks[i] = &shard{}
	}

	return fb
}

// getShard returns shard under given prefix
func (fb *freeblocks) getShard(prefix uint64) *shard {
	return fb.blocks[fb.consistent.FindBlock(prefix)]
}

func (s *shard) search(size uint32) int {
	// limit search to first 100 freeblocks
	return sort.Search(100, func(i int) bool {
		return s.blocks[i].size >= size
	})
}

// TODO implement btree+ search
// contains checks whether a message id is in the set.
func (s *shard) contains(off int64) bool {
	for _, v := range s.blocks {
		if v.offset == off {
			return true
		}
	}
	return false
}

func (s *shard) defrag() {
	l := len(s.blocks)
	if l <= 1 {
		return
	}
	// limit fragmentation to first 1000 freeblocks
	if l > 1000 {
		l = 1000
	}
	sort.Slice(s.blocks[:l], func(i, j int) bool {
		return s.blocks[i].offset < s.blocks[j].offset
	})
	var merged []freeblock
	curOff := s.blocks[0].offset
	curSize := s.blocks[0].size
	for i := 1; i < l; i++ {
		if curOff+int64(curSize) == s.blocks[i].offset {
			curSize += s.blocks[i].size
		} else {
			merged = append(merged, freeblock{size: curSize, offset: curOff})
			curOff = s.blocks[i].offset
			curSize = s.blocks[i].size
		}
	}
	merged = append(merged, freeblock{offset: curOff, size: curSize})
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].size < merged[j].size
	})
	copy(s.blocks[:l], merged)
}

func (fb *freeblocks) defrag() {
	for i := 0; i < nShards; i++ {
		shard := fb.blocks[i]
		shard.defrag()
	}
}

func (fb *freeblocks) freequeue() {
	shard := fb.blocks[nShards]
	shard.Lock()
	defer shard.Unlock()
	shard.defrag()
	for _, b := range shard.blocks {
		// Get shard
		s := fb.getShard(uint64(b.size))
		if s.contains(b.offset) == false {
			s.blocks = append(s.blocks, freeblock{offset: b.offset, size: b.size})
			fb.size += int64(b.size)
		}
	}
}

func (fb *freeblocks) free(off int64, size uint32) {
	if size == 0 {
		panic("unable to free zero bytes")
	}
	shard := fb.blocks[nShards]
	shard.blocks = append(shard.blocks, freeblock{offset: off, size: size})
	if len(shard.blocks) < 100 {
		return
	}
	fb.freequeue()
	return
}

func (fb *freeblocks) allocate(size uint32) int64 {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	if fb.size < fb.minimumFreeBlocksSize {
		return -1
	}
	shard := fb.getShard(uint64(size))

	if len(shard.blocks) < 100 {
		return -1
	}
	i := shard.search(size)
	if i >= len(shard.blocks) {
		return -1
	}
	off := shard.blocks[i].offset
	if shard.blocks[i].size == size {
		copy(shard.blocks[i:], shard.blocks[i+1:])
		shard.blocks[len(shard.blocks)-1] = freeblock{}
		shard.blocks = shard.blocks[:len(shard.blocks)-1]
	} else {
		shard.blocks[i].size -= size
		shard.blocks[i].offset += int64(size)
	}
	fb.size -= int64(size)
	return off
}

// MarshalBinary serializes freeblocks into binary data
func (s *shard) MarshalBinary() ([]byte, error) {
	size := s.binarySize()
	buf := make([]byte, size)
	data := buf
	binary.LittleEndian.PutUint32(data[:4], uint32(len(s.blocks)))
	data = data[4:]
	for i := 0; i < len(s.blocks); i++ {
		binary.LittleEndian.PutUint64(data[:8], uint64(s.blocks[i].offset))
		binary.LittleEndian.PutUint32(data[8:12], s.blocks[i].size)
		data = data[12:]
	}
	return buf, nil
}

func (s *shard) binarySize() uint32 {
	return uint32((4 + (8+4)*len(s.blocks))) // FIXME: this is ugly
}

func (fb *freeblocks) read(t table, off int64) error {
	if off == -1 {
		return nil
	}

	var size uint32
	offset := off
	for i := 0; i < nShards; i++ {
		shard := fb.blocks[i]
		buf := make([]byte, 4)
		if _, err := t.ReadAt(buf, offset); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(buf)
		size += n
		buf = make([]byte, (4+8)*n)
		if _, err := t.ReadAt(buf, offset+4); err != nil {
			return err
		}
		for i := uint32(0); i < n; i++ {
			blockOff := int64(binary.LittleEndian.Uint64(buf[:8]))
			blockSize := binary.LittleEndian.Uint32(buf[8:12])
			if blockOff != 0 {
				shard.blocks = append(shard.blocks, freeblock{size: blockSize, offset: blockOff})
				fb.size += int64(blockSize)
			}
			buf = buf[12:]
		}
		offset += int64((4 + 8) * n)
	}
	fb.free(off, align512(4+(4+8)*size))
	return nil
}

func (fb *freeblocks) write(t table) (int64, error) {
	// free blocks in queue. As last shard is used to queue freeblocks
	fb.freequeue()
	if len(fb.blocks) == 0 {
		return -1, nil
	}
	var marshaledSize uint32
	var buf []byte
	for i := 0; i < nShards; i++ {
		shard := fb.blocks[i]
		marshaledSize += align512(shard.binarySize())
		data, err := shard.MarshalBinary()
		buf = append(buf, data...)
		if err != nil {
			return -1, err
		}
	}
	off, err := t.extend(marshaledSize)
	if err != nil {
		return -1, err
	}
	_, err = t.WriteAt(buf, off)
	return off, err
}
