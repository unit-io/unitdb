package tracedb

import (
	"errors"

	"github.com/unit-io/tracedb/message"
)

func (db *DB) recoverWindowBlocks() error {
	err := db.timeWindow.foreachTimeWindow(true, func(windowEntries map[uint64]windowEntries) (bool, error) {
		for h, t := range windowEntries {
			off, ok := db.trie.getOffset(h)
			if !ok {
				return true, errors.New("recovery.recoverWindowBlocks error: unbale to get topic offset from trie")
			}
			wb, err := db.timeWindow.getWindowBlockHandle(h, off)
			if err != nil {
				return true, err
			}
			for _, we := range t {
				newOff, err := db.timeWindow.write(&wb, we)
				if err != nil {
					return true, err
				}
				if newOff > off {
					if ok := db.trie.setOffset(h, newOff); !ok {
						return true, errors.New("recovery.recoverWindowBlocks error: unbale to set topic offset in trie")
					}
				}
			}
			if err != nil {
				return true, err
			}
			if err := wb.write(); err != nil {
				return true, err
			}
			if err := db.timeWindow.Sync(); err != nil {
				return true, err
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
	logSeqs, upperSeqs, err := db.wal.Scan()
	if err != nil {
		return err
	}
	for i, s := range logSeqs {
		nBlocks := int32(upperSeqs[i] / entriesPerBlock)
		for nBlocks > db.blockIndex {
			if _, err := db.newBlock(); err != nil {
				return err
			}
		}
		it, err := db.wal.Read(s)
		if err != nil {
			return err
		}
		var off int64
		var b blockHandle
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
			if newOff != off {
				off = newOff
				// write previous block
				if err := b.write(); err != nil {
					return err
				}
				b = blockHandle{file: db.index, offset: newOff}
				if err := b.read(); err != nil {
					return err
				}
			}
			entryIdx := 0
			for i := 0; i < entriesPerBlock; i++ {
				e := b.entries[i]
				if e.seq == logEntry.seq { //record exist in db
					entryIdx = -1
					break
				}
			}
			if entryIdx == -1 {
				continue
			}
			db.incount()
			msgOffset := logEntry.mSize()
			m := data[:msgOffset]
			if logEntry.msgOffset, err = db.data.write(m); err != nil {
				return err
			}
			t, err := db.data.readTopic(logEntry)
			if err != nil {
				return err
			}
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
			db.meter.Puts.Inc(1)
			db.meter.InBytes.Inc(int64(logEntry.valueSize))
			if err := b.append(logEntry); err != nil {
				return err
			}
			db.filter.Append(logEntry.seq)
		}
		// write any pending entries
		if err := b.write(); err != nil {
			return err
		}
		if err := db.recoverWindowBlocks(); err != nil {
			return err
		}
		if err := db.sync(); err != nil {
			return err
		}
		if err := db.wal.SignalLogApplied(s); err != nil {
			return err
		}
	}

	return err
}
