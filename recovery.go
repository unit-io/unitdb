package tracedb

import (
	"errors"
	"fmt"

	"github.com/unit-io/tracedb/bpool"
	"github.com/unit-io/tracedb/message"
)

func (db *DB) recoverWindowBlocks() error {
	err := db.timeWindow.foreachTimeWindow(true, func(windowEntries map[uint64]windowEntries) (bool, error) {
		for h, wEntries := range windowEntries {
			topicOff, ok := db.trie.getOffset(h)
			if !ok {
				return true, errors.New("recovery.recoverWindowBlocks error: unbale to get topic offset from trie")
			}
			wOff, err := db.timeWindow.sync(h, topicOff, wEntries)
			if err != nil {
				return true, err
			}
			if ok := db.trie.setOffset(h, wOff); !ok {
				return true, errors.New("db:Sync: timeWindow sync error: unbale to set topic offset in trie")
			}
		}
		return false, nil
	})
	return err
}

func (db *DB) recoverLog() error {
	db.closeW.Add(1)
	defer func() {
		db.closeW.Done()
	}()
	fmt.Println("db.recoverLog: start recovery")
	logSeqs, upperSeqs, err := db.wal.Scan()
	if err != nil {
		return err
	}

	var idxBlocks []blockHandle
	blockIdx := db.blocks()
	idxBlockOff := int64(0)
	dataBlockOff := int64(0)
	bufPool := bpool.NewBufferPool(1 << 33)
	rawIndex := bufPool.Get()
	rawData := bufPool.Get()
	defer func() {
		bufPool.Put(rawData)
		bufPool.Put(rawIndex)
	}()

	write := func() error {
		for _, upperSeq := range upperSeqs {
			nBlocks := int32(upperSeq / entriesPerIndexBlock)
			for nBlocks > db.blockIndex {
				if _, err := db.newBlock(); err != nil {
					return err
				}
			}
		}
		for _, idxBlock := range idxBlocks {
			rawIndex.Write(idxBlock.MarshalBinary())
		}
		if _, err := db.index.WriteAt(rawIndex.Bytes(), idxBlockOff); err != nil {
			return err
		}
		if _, err := db.data.WriteAt(rawData.Bytes(), dataBlockOff); err != nil {
			return err
		}

		// reset blocks
		idxBlockOff = int64(0)
		dataBlockOff = int64(0)
		idxBlocks = idxBlocks[:0]
		bufPool.Put(rawIndex)
		bufPool.Put(rawData)
		return nil
	}

	var off int64
	var bh, leasedBh blockHandle
	for _, s := range logSeqs {
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
			logEntry := entry{}
			if err := logEntry.UnmarshalBinary(entryData); err != nil {
				return err
			}
			startBlockIdx := startBlockIndex(logEntry.seq)
			newOff := blockOffset(startBlockIdx)
			bh = blockHandle{file: db.index, offset: newOff}
			// any seq less than current db count is leased seq
			if startBlockIdx < blockIdx {
				if newOff != off {
					off = newOff
					// write previous block
					if err := leasedBh.write(); err != nil {
						return err
					}
					if err := bh.read(); err != nil {
						return err
					}
				}
				entryIdx := 0
				for i := 0; i < entriesPerIndexBlock; i++ {
					e := bh.entries[i]
					if e.seq == logEntry.seq { //record exist in db
						entryIdx = -1
						break
					}
				}
				if entryIdx == -1 {
					continue
				}
				leasedBh = blockHandle{file: db.index, offset: bh.offset}
			} else {
				if idxBlockOff == 0 {
					idxBlockOff = newOff
				}
				idxBlocks = append(idxBlocks, bh)
			}
			db.incount()
			msgOffset := logEntry.mSize()
			m := data[:msgOffset]
			if logEntry.msgOffset, err = db.data.write(m); err != nil {
				if err != errLeasedBlock {
					return err
				}
			} else {
				if dataBlockOff == 0 {
					dataBlockOff = logEntry.msgOffset
				}
				rawData.Write(m)
			}
			bh.entries[bh.entryIdx] = logEntry
			bh.entryIdx++

			t := m[int64(idSize) : int64(logEntry.topicSize)+int64(idSize)]

			// t, err := db.data.readTopic(logEntry)
			// if err != nil {
			// 	return err
			// }
			topic := new(message.Topic)
			err = topic.Unmarshal(t)
			if err != nil {
				return err
			}
			contract := message.Contract(topic.Parts)
			topicHash := topic.GetHash(contract)
			we := winEntry{
				contract: contract,
				seq:      logEntry.seq,
			}
			db.timeWindow.add(topicHash, we)
			if ok := db.trie.setOffset(topicHash, logEntry.topicOffset); !ok {
				if ok := db.trie.addTopic(contract, topicHash, topic.Parts, topic.Depth); ok {
					if ok := db.trie.setOffset(topicHash, logEntry.topicOffset); !ok {
						return errors.New("recovery.recoverLog error: unable to set topic offset to topic trie")
					}
				}
			}

			db.filter.Append(logEntry.seq)
			db.meter.Puts.Inc(1)
			db.meter.InBytes.Inc(int64(logEntry.valueSize))
		}

		if rawData.Size() > db.opts.BufferSize {
			if err := write(); err != nil {
				return err
			}
		}
	}

	// write any pending entries
	if err := leasedBh.write(); err != nil {
		return err
	}
	if err := db.recoverWindowBlocks(); err != nil {
		return err
	}

	if err := write(); err != nil {
		return err
	}

	if err := db.sync(); err != nil {
		return err
	}

	for i, upperSeq := range upperSeqs {
		if db.Seq() > upperSeq {
			if err := db.wal.SignalLogApplied(logSeqs[i]); err != nil {
				return err
			}
		}
	}

	return err
}
