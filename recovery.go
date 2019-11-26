package tracedb

import (
	"math"
	"sort"

	"github.com/saffat-in/tracedb/fs"
)

type userdblock struct {
	offset int64
	size   uint32
}

func align51264(n int64) int64 {
	return (n + 511) &^ 511
}

func truncateFiles(db *DB) error {
	db.index.size = align51264(db.index.size)
	if err := db.index.Truncate(db.index.size); err != nil {
		return err
	}

	if db.index.Type() == "MemoryMap" {
		if err := db.index.FileManager.(*fs.OSFile).Mmap(db.index.size); err != nil {
			return err
		}
	}

	db.data.size = align51264(db.data.size)
	if err := db.data.Truncate(db.data.size); err != nil {
		return err
	}

	if db.data.Type() == "MemoryMap" {
		if err := db.data.FileManager.(*fs.OSFile).Mmap(db.data.size); err != nil {
			return err
		}
	}
	return nil
}

func getUsedBlocks(db *DB) (uint32, []userdblock, error) {
	var itemCount uint32
	var usedBlocks []userdblock
	for blockIdx := uint32(0); blockIdx < db.nBlocks; blockIdx++ {
		err := db.forEachBlock(blockIdx, false, func(b blockHandle) (bool, error) {
			for i := 0; i < entriesPerBlock; i++ {
				sl := b.entries[i]
				if sl.kvOffset == 0 {
					return true, nil
				}
				itemCount++
				usedBlocks = append(usedBlocks, userdblock{size: align512(sl.kvSize()), offset: sl.kvOffset})
			}
			if b.next != 0 {
				usedBlocks = append(usedBlocks, userdblock{size: blockSize, offset: b.next})
			}
			return false, nil
		})
		if err != nil {
			return 0, nil, err
		}
	}
	return itemCount, usedBlocks, nil
}

func recoverSplitCrash(db *DB) error {
	if db.nBlocks == 1 {
		return nil
	}
	prevnBlocks := db.nBlocks - 1
	prevLevel := uint8(math.Floor(math.Log2(float64(prevnBlocks))))
	prevSplitBlockIdx := prevnBlocks - (uint32(1) << prevLevel)
	splitCrash := false
	err := db.forEachBlock(prevSplitBlockIdx, false, func(b blockHandle) (bool, error) {
		for i := 0; i < entriesPerBlock; i++ {
			sl := b.entries[i]
			if sl.kvOffset == 0 {
				return true, nil
			}
			if db.blockIndex(sl.hash) != prevSplitBlockIdx {
				splitCrash = true
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		return err
	}
	if !splitCrash {
		return nil
	}
	logger.Error().Str("context: recovery.recoverSplitCrash", "Detected split crash. Truncating index file...")
	if err := db.index.Truncate(db.index.size - int64(blockSize)); err != nil {
		return err
	}
	// db.index.size -= int64(blockSize)
	// if err := db.index.Mmap(db.index.size); err != nil {
	// 	return err
	// }
	db.nBlocks = prevnBlocks
	db.level = prevLevel
	db.splitBlockIdx = prevSplitBlockIdx
	return nil
}

func recoverFreeList(db *DB, usedBlocks []userdblock) error {
	if len(usedBlocks) == 0 {
		return nil
	}
	sort.Slice(usedBlocks, func(i, j int) bool {
		return usedBlocks[i].offset < usedBlocks[j].offset
	})
	fb := freeblocks{}
	expectedOff := int64(headerSize)
	for _, bl := range usedBlocks {
		if bl.offset > expectedOff {
			fb.free(expectedOff, uint32(bl.offset-expectedOff))
		}
		expectedOff = bl.offset + int64(bl.size)
	}
	lastBlock := usedBlocks[len(usedBlocks)-1]
	lastOffset := int64(lastBlock.size) + lastBlock.offset
	if db.data.size > lastOffset {
		fb.free(lastOffset, uint32(db.data.size-lastOffset))
		logger.Info().Str("context", "recovery.recoverFreeList").Msgf("%v %d", lastBlock, db.data.size)
	}
	logger.Info().Str("context", "recovery.recoverFreeList").Int("Old len", len(db.data.fb.blocks)).Int("new len", len(fb.blocks)).Msg("Recovered freelist")
	db.data.fb = fb
	return nil
}

func (db *DB) recover() error {
	logger.Info().Str("context", "recovery.recover").Msg("Performing recovery...")
	logger.Info().Str("context", "recovery.recover").Int64("Index file size", db.index.size).Int64("data file size", db.data.size)
	logger.Info().Str("context", "recovery.recover").Msgf("Header dbInfo %+v", db.dbInfo)

	// Truncate index and data files.
	if err := truncateFiles(db); err != nil {
		return err
	}

	// Recover header.
	db.nBlocks = uint32((db.index.size - int64(headerSize)) / int64(blockSize))
	db.level = uint8(math.Floor(math.Log2(float64(db.nBlocks))))
	db.splitBlockIdx = db.nBlocks - (uint32(1) << db.level)
	itemCount, usedBlocks, err := getUsedBlocks(db)
	if err != nil {
		return err
	}
	db.count = itemCount

	// Check if crash occurred during split.
	if err := recoverSplitCrash(db); err != nil {
		return err
	}
	logger.Info().Str("context", "recovery.recover").Msgf("Recovered dbInfo %+v\n", db.dbInfo)

	// Recover free list.
	if err := recoverFreeList(db, usedBlocks); err != nil {
		return err
	}
	logger.Info().Str("context", "recovery.recover").Msg("Recovery complete.")
	return nil
}
