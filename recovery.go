package tracedb

import (
	"errors"
	"fmt"

	_ "net/http/pprof"

	"github.com/unit-io/tracedb/message"
	"github.com/unit-io/tracedb/wal"
)

func (db *DB) recoverWindowBlocks() error {
	err := db.timeWindow.foreachTimeWindow(true, func(windowEntries map[uint64]windowEntries) (bool, uint64, error) {
		var readSeq uint64
		for h, wEntries := range windowEntries {
			topicOff, ok := db.trie.getOffset(h)
			if !ok {
				return true, readSeq, errors.New("recovery.recoverWindowBlocks error: unable to get topic offset from trie")
			}
			wOff, err := db.timeWindow.sync(h, topicOff, wEntries)
			if err != nil {
				return true, readSeq, err
			}
			if ok := db.trie.setOffset(h, wOff); !ok {
				return true, readSeq, errors.New("db:Sync: timeWindow sync error: unable to set topic offset in trie")
			}
			readSeq = wEntries[len(wEntries)-1].Seq()
		}
		return false, readSeq, nil
	})
	return err
}

func (db *DB) recoverLog() error {
	// p := profile.Start(profile.MemProfile, profile.ProfilePath("."), profile.NoShutdownHook)
	// defer p.Stop()
	db.closeW.Add(1)
	defer func() {
		db.closeW.Done()
	}()
	fmt.Println("db.recoverLog: start recovery")

	var idxBlocks []blockHandle
	blockIdx := db.blocks()

	bufSize := int64(1 << 27)
	idxBlockOff := int64(0)
	dataBlockOff := int64(0)
	rawIndex := db.bufPool.Get()
	rawData := db.bufPool.Get()
	defer func() {
		db.bufPool.Put(rawData)
		db.bufPool.Put(rawIndex)
	}()

	write := func() error {
		nBlocks := int32(float64(db.wal.Seq()) / float64(entriesPerIndexBlock))
		for nBlocks > db.blocks() {
			if _, err := db.newBlock(); err != nil {
				return err
			}
		}
		for _, idxBlock := range idxBlocks {
			switch {
			case idxBlock.entryIdx == 0:
			case idxBlock.leased:
				if err := idxBlock.write(); err != nil {
					return err
				}
			default:
				rawIndex.Write(idxBlock.MarshalBinary())
			}
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

	logEntry := entry{}
	err := db.wal.Read(func(r *wal.Reader) (bool, error) {
		var off int64
		var bh, leasedBh blockHandle
		for {
			logData, ok := r.Next()
			if !ok {
				break
			}
			entryData, data := logData[:entrySize], logData[entrySize:]
			if err := logEntry.UnmarshalBinary(entryData); err != nil {
				return true, err
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
						return true, err
					}
					if err := bh.read(); err != nil {
						return true, err
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
				bh.leased = true
				leasedBh = blockHandle{file: db.index, offset: bh.offset}
			}
			db.incount()
			msgOffset := logEntry.mSize()
			m := data[:msgOffset]
			if msgOffset, err := db.data.write(m); err != nil {
				if err != errLeasedBlock {
					return true, err
				}
				logEntry.msgOffset = msgOffset
			} else {
				if dataBlockOff == 0 {
					dataBlockOff = logEntry.msgOffset
				}
				rawData.Write(m)
			}
			bh.entries[bh.entryIdx] = logEntry
			bh.entryIdx++

			if bh.entryIdx == entriesPerIndexBlock {
				if idxBlockOff == 0 {
					idxBlockOff = newOff
				}
				idxBlocks = append(idxBlocks, bh)
			}
			t := m[int64(idSize) : int64(logEntry.topicSize)+int64(idSize)]

			topic := new(message.Topic)
			if err := topic.Unmarshal(t); err != nil {
				return true, err
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
						return true, errors.New("recovery.recoverLog error: unable to set topic offset to topic trie")
					}
				}
			}

			db.filter.Append(logEntry.seq)
			db.meter.Puts.Inc(1)
			db.meter.InBytes.Inc(int64(logEntry.valueSize))
		}

		idxBlocks = append(idxBlocks, bh)

		if rawData.Size() > bufSize {
			if err := db.recoverWindowBlocks(); err != nil {
				return false, err
			}

			if err := write(); err != nil {
				return false, err
			}

			if err := db.sync(); err != nil {
				return false, err
			}
		}

		return false, nil
	})
	if err != nil {
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

	if err := db.wal.SignalLogApplied(logEntry.seq); err != nil {
		return err
	}

	return err
}
