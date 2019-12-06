package memdb

import (
	"sort"
)

type freesequence struct {
	seqs []uint64 // seq holds free sequence
}

func (fs *freesequence) search(seq uint64) int {
	return sort.Search(len(fs.seqs), func(i int) bool {
		return fs.seqs[i] == seq
	})
}

// get first free seq
func (fs *freesequence) get() (ok bool, seq uint64) {
	if len(fs.seqs) == 0 {
		return ok, seq
	}
	seq = fs.seqs[0]
	fs.seqs = fs.seqs[1:]
	return true, seq
}

func (fs *freesequence) free(seq uint64) (ok bool) {
	i := fs.search(seq)
	if i < len(fs.seqs) && seq == fs.seqs[i] {
		return false
	}
	fs.seqs = append(fs.seqs, seq)
	return true
}

func (fs *freesequence) len() int {
	return len(fs.seqs)
}

type freeblock struct {
	offset int64
	size   uint32
}

type freeblocks struct {
	blocks []freeblock
}

func (fb *freeblocks) search(size uint32) int {
	return sort.Search(len(fb.blocks), func(i int) bool {
		return fb.blocks[i].size >= size
	})
}

func (fb *freeblocks) free(off int64, size uint32) {
	if size == 0 {
		panic("unable to free zero bytes")
	}
	i := fb.search(size)
	if i < len(fb.blocks) && off == fb.blocks[i].offset {
		panic("freeing already freed offset")
	}

	fb.blocks = append(fb.blocks, freeblock{})
	copy(fb.blocks[i+1:], fb.blocks[i:])
	fb.blocks[i] = freeblock{offset: off, size: size}
}

func (fb *freeblocks) allocate(size uint32) int64 {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	i := fb.search(size)
	if i >= len(fb.blocks) {
		return -1
	}
	off := fb.blocks[i].offset
	if fb.blocks[i].size == size {
		copy(fb.blocks[i:], fb.blocks[i+1:])
		fb.blocks[len(fb.blocks)-1] = freeblock{}
		fb.blocks = fb.blocks[:len(fb.blocks)-1]
	} else {
		fb.blocks[i].size -= size
		fb.blocks[i].offset += int64(size)
	}
	return off
}

func (fb *freeblocks) defrag() {
	if len(fb.blocks) <= 1 {
		return
	}
	sort.Slice(fb.blocks, func(i, j int) bool {
		return fb.blocks[i].offset < fb.blocks[j].offset
	})
	var merged []freeblock
	curOff := fb.blocks[0].offset
	curSize := fb.blocks[0].size
	for i := 1; i < len(fb.blocks); i++ {
		if curOff+int64(curSize) == fb.blocks[i].offset {
			curSize += fb.blocks[i].size
		} else {
			merged = append(merged, freeblock{size: curSize, offset: curOff})
			curOff = fb.blocks[i].offset
			curSize = fb.blocks[i].size
		}
	}
	merged = append(merged, freeblock{offset: curOff, size: curSize})
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].size < merged[j].size
	})
	fb.blocks = merged
}
