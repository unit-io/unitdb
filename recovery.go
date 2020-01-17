package tracedb

import (
	"sort"

	"github.com/unit-io/tracedb/fs"
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

func getUsedBlocks(db *DB) (int64, []userdblock, error) {
	var itemCount int64
	var usedBlocks []userdblock
	for blockIdx := uint32(0); blockIdx < db.nBlocks; blockIdx++ {
		off := blockOffset(blockIdx)
		b := blockHandle{table: db.index.FileManager, offset: off}
		if err := b.read(); err != nil {
			return 0, nil, err
		}
		for i := 0; i < entriesPerBlock; i++ {
			e := b.entries[i]
			if e.mOffset == 0 {
				continue
			}
			itemCount++
			usedBlocks = append(usedBlocks, userdblock{size: align512(e.mSize()), offset: e.mOffset})
		}
		if b.next != 0 {
			usedBlocks = append(usedBlocks, userdblock{size: blockSize, offset: int64(b.next)})
		}
	}
	return itemCount, usedBlocks, nil
}

func recoverFreeBlocks(db *DB, usedBlocks []userdblock) error {
	if len(usedBlocks) == 0 {
		return nil
	}
	sort.Slice(usedBlocks, func(i, j int) bool {
		return usedBlocks[i].offset < usedBlocks[j].offset
	})
	fb := newFreeBlocks(0)
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
		logger.Info().Str("context", "recovery.recoverFreeBlocks").Msgf("%v %d", lastBlock, db.data.size)
	}
	logger.Info().Str("context", "recovery.recoverFreeBlocks").Int("Old len", len(db.data.fb.blocks)).Int("new len", len(fb.blocks)).Msg("Recovered freeblocks")
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
	itemCount, usedBlocks, err := getUsedBlocks(db)
	if err != nil {
		return err
	}
	db.count = itemCount

	// Recover free list.
	if err := recoverFreeBlocks(db, usedBlocks); err != nil {
		return err
	}
	logger.Info().Str("context", "recovery.recover").Msg("Recovery complete.")
	return nil
}

func (db *DB) recoverLog() error {
	db.closeW.Add(1)
	db.mu.Lock()
	defer func() {
		db.mu.Unlock()
		db.closeW.Done()
	}()
	seqs, err := db.wal.Scan()
	if err != nil {
		return err
	}
	if err := db.extendBlocks(); err != nil {
		return err
	}
	for _, s := range seqs {
		it, err := db.wal.Read(s)
		if err != nil {
			return err
		}
		for {
			logData, ok := it.Next()
			if !ok {
				break
			}
			entryData, data := logData[:entrySize], logData[entrySize:]
			e := entry{}
			e.UnmarshalBinary(entryData)
			startBlockIdx := startBlockIndex(e.seq)
			off := blockOffset(startBlockIdx)
			b := &blockHandle{table: db.index, offset: off}
			if err := b.read(); err != nil {
				return err
			}
			entryIdx := 0
			for i := 0; i < entriesPerBlock; i++ {
				ie := b.entries[i]
				if ie.seq == e.seq { //record exist in db
					entryIdx = -1
					break
				}
			}
			if entryIdx == -1 {
				continue
			}
			db.count++
			moffset := e.mSize()
			m := data[:moffset]
			if e.mOffset, err = db.data.writeRaw(m); err != nil {
				return err
			}
			db.meter.Puts.Inc(1)
			db.meter.InBytes.Inc(int64(e.valueSize))
			b.entries[b.entryIdx] = e
			b.entryIdx++
			if err := b.write(); err != nil {
				return err
			}
			db.filter.Append(e.seq)
		}
		if err := db.sync(); err != nil {
			return err
		}
		if err := db.wal.SignalLogApplied(s); err != nil {
			return err
		}
	}

	return nil
}
