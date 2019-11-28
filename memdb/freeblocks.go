package memdb

import "sort"

type freeblock struct {
	blockIndex uint32
	empty      bool
}

type freeblocks struct {
	blocks []freeblock
}

func (fb *freeblocks) search(blockIdx uint32) int {
	return sort.Search(len(fb.blocks), func(i int) bool {
		return fb.blocks[i].blockIndex == blockIdx
	})
}

func (fb *freeblocks) get(empty bool) (ok bool, blockIdx uint32) {
	ok = len(fb.blocks) > 0
	if ok {
		// get empty block with all slots empty
		if empty {
			for i := range fb.blocks {
				if fb.blocks[i].empty {
					blockIdx = fb.blocks[i].blockIndex
					fb.blocks[i] = fb.blocks[len(fb.blocks)-1]
					fb.blocks = fb.blocks[:len(fb.blocks)-1]
					return ok, blockIdx
				}
			}
		}
		blockIdx = fb.blocks[0].blockIndex
		fb.blocks = fb.blocks[1:len(fb.blocks)]
	}

	return ok, blockIdx
}

func (fb *freeblocks) free(blockIdx uint32, empty bool) (ok bool) {
	i := fb.search(blockIdx)
	if i < len(fb.blocks) && blockIdx == fb.blocks[i].blockIndex {
		return false
	}

	fb.blocks = append(fb.blocks, freeblock{})
	copy(fb.blocks[i+1:], fb.blocks[i:])
	fb.blocks[i] = freeblock{blockIndex: blockIdx, empty: empty}
	return true
}
