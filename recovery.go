package tracedb

import (
	"errors"
	"fmt"

	"github.com/unit-io/tracedb/message"
)

func (db *DB) recoverWindowBlocks() error {
	err := db.timeWindow.foreachTimeWindow(true, func(windowEntries map[uint64]windowEntries) (bool, uint64, error) {
		var readSeq uint64
		for h, wEntries := range windowEntries {
			topicOff, ok := db.trie.getOffset(h)
			if !ok {
				return true, readSeq, errors.New("recovery.recoverWindowBlocks error: unbale to get topic offset from trie")
			}
			wOff, err := db.timeWindow.sync(h, topicOff, wEntries)
			if err != nil {
				return true, readSeq, err
			}
			if ok := db.trie.setOffset(h, wOff); !ok {
				return true, readSeq, errors.New("db:Sync: timeWindow sync error: unbale to set topic offset in trie")
			}
			readSeq = wEntries[len(wEntries)-1].Seq()
		}
		return false, readSeq, nil
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

	bufSize := int64(1 << 30)
	idxBlockOff := int64(0)
	dataBlockOff := int64(0)
	rawIndex := db.bufPool.Get()
	rawData := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(rawData)
		db.bufPool.Put(rawIndex)
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
		rawIndex.Reset()
		rawData.Reset()
		return nil
	}

	var off int64
	var bh, leasedBh blockHandle
	logEntry := entry{}
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

		if rawData.Size() > bufSize {
			if err := write(); err != nil {
				return err
			}

			if err := db.sync(); err != nil {
				return err
			}

			if err := db.wal.SignalLogApplied(logEntry.seq); err != nil {
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

	// for i, upperSeq := range upperSeqs {
	// 	if db.Seq() > upperSeq {
	if err := db.wal.SignalLogApplied(logEntry.seq); err != nil {
		return err
	}
	// 	}
	// }

	return err
}
