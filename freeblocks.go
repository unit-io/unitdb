package tracedb

import (
	"encoding/binary"
	"sort"

	"github.com/unit-io/tracedb/hash"
)

const (
	nShards = 32 // TODO implelemt sharding based on total Contracts in db
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
		slots:      make([]*freeslot, nShards),
		consistent: hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
		s.slots[i] = &freeslot{}
	}

	return s
}

// getShard returns shard under given prefix
func (fs *freeslots) getShard(prefix uint64) *freeslot {
	return fs.slots[fs.consistent.FindBlock(prefix)]
}

func (fs *freeslot) search(seq uint64) int {
	return sort.Search(len(fs.seqs), func(i int) bool {
		return fs.seqs[i] == seq
	})
}

// get first free seq
func (fs *freeslots) get(prefix uint64) (ok bool, seq uint64) {
	// Get shard
	shard := fs.getShard(prefix)
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

func (fs *freeslots) free(prefix, seq uint64) (ok bool) {
	// Get shard
	shard := fs.getShard(prefix)
	// shard.Lock()
	// defer shard.Unlock()
	i := shard.search(seq)
	if i < len(shard.seqs) && seq == shard.seqs[i] {
		return false
	}
	shard.seqs = append(shard.seqs, seq)
	return true
}

func (fs *freeslot) len() int {
	return len(fs.seqs)
}

// A "thread" safe freeslot.
// To avoid lock bottlenecks slots are dived to several (nShards).
// type freeblocks []*freeblock

type freeblocks struct {
	blocks     []*shard
	size       int64 // total size of free blocks
	consistent *hash.Consistent
}

type freeblock struct {
	offset int64
	size   uint32
}

type shard struct {
	blocks []freeblock
	// sync.RWMutex // Read Write mutex, guards access to internal collection.
}

// newFreeBlocks creates a new concurrent freeblocks.
func newFreeBlocks() freeblocks {
	fb := freeblocks{
		blocks:     make([]*shard, nShards),
		consistent: hash.InitConsistent(int(nShards), int(nShards)),
	}

	for i := 0; i < nShards; i++ {
		fb.blocks[i] = &shard{}
	}

	return fb
}

// getShard returns shard under given prefix
func (fb *freeblocks) getShard(prefix uint64) *shard {
	return fb.blocks[fb.consistent.FindBlock(prefix)]
}

func (s *shard) search(size uint32) int {
	return sort.Search(len(s.blocks), func(i int) bool {
		return s.blocks[i].size >= size
	})
}

func (fb *freeblocks) free(off int64, size uint32) {
	if size == 0 {
		panic("unable to free zero bytes")
	}
	// Get shard
	shard := fb.getShard(uint64(size))
	// shard.Lock()
	// defer shard.Unlock()
	i := shard.search(size)
	if i < len(shard.blocks) && off == shard.blocks[i].offset {
		// panic("freeing already freed offset")
		return
	}

	shard.blocks = append(shard.blocks, freeblock{})
	fb.size += int64(size)
	copy(shard.blocks[i+1:], shard.blocks[i:])
	shard.blocks[i] = freeblock{offset: off, size: size}
}

func (fb *freeblocks) allocate(size uint32) int64 {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	shard := fb.getShard(uint64(size))
	// shard.Lock()
	// defer shard.Unlock()
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

func (fb *freeblocks) defrag() {
	if len(fb.blocks) <= 1 {
		return
	}
	for i := 0; i < nShards; i++ {
		shard := fb.blocks[i]
		if len(shard.blocks) <= 1 {
			continue
		}
		sort.Slice(shard.blocks, func(i, j int) bool {
			return shard.blocks[i].offset < shard.blocks[j].offset
		})
		var merged []freeblock
		curOff := shard.blocks[0].offset
		curSize := shard.blocks[0].size
		for i := 1; i < len(shard.blocks); i++ {
			if curOff+int64(curSize) == shard.blocks[i].offset {
				curSize += shard.blocks[i].size
			} else {
				merged = append(merged, freeblock{size: curSize, offset: curOff})
				curOff = shard.blocks[i].offset
				curSize = shard.blocks[i].size
			}
		}
		merged = append(merged, freeblock{offset: curOff, size: curSize})
		sort.Slice(merged, func(i, j int) bool {
			return merged[i].size < merged[j].size
		})
		shard.blocks = merged
	}
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
	// scratch := make([]byte, 2)
	// if _, err := t.ReadAt(scratch, off); err != nil {
	// 	return err
	// }
	// b := binary.LittleEndian.Uint16(scratch)

	var size uint32
	for i := 0; i < nShards; i++ {
		buf := make([]byte, 4)
		if _, err := t.ReadAt(buf, off); err != nil {
			return err
		}
		n := binary.LittleEndian.Uint32(buf)
		size += n
		buf = make([]byte, (4+8)*n)
		if _, err := t.ReadAt(buf, off+4); err != nil {
			return err
		}
		for i := uint32(0); i < n; i++ {
			blockOff := int64(binary.LittleEndian.Uint64(buf[:8]))
			blockSize := binary.LittleEndian.Uint32(buf[8:12])
			if blockOff != 0 {
				fb.blocks[i].blocks = append(fb.blocks[i].blocks, freeblock{size: blockSize, offset: blockOff})
				fb.size += int64(blockSize)
			}
			buf = buf[12:]
		}
	}
	fb.free(off, align512(4+(4+8)*size))
	return nil
}

func (fb *freeblocks) write(t table) (int64, error) {
	if len(fb.blocks) == 0 {
		return -1, nil
	}
	var marshaledSize uint32
	var buf []byte
	for i := 0; i < nShards; i++ {
		shard := fb.blocks[i]
		// scratch := make([]byte, 2)
		// binary.LittleEndian.PutUint16(scratch[0:2], uint16(i))
		// buf = append(buf, scratch...)
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
