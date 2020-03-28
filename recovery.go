package tracedb

import (
	"errors"
	"fmt"

	_ "net/http/pprof"

	"github.com/unit-io/tracedb/message"
	"github.com/unit-io/tracedb/wal"
)

func (db *DB) recoverWindowBlocks() error {
	err := db.timeWindow.foreachTimeWindow(true, func(windowEntries map[uint64]windowEntries) (bool, error) {
		for h, wEntries := range windowEntries {
			topicOff, ok := db.trie.getOffset(h)
			if !ok {
				return true, errors.New("recovery.recoverWindowBlocks error: unable to get topic offset from trie")
			}
			wOff, err := db.timeWindow.sync(h, topicOff, wEntries)
			if err != nil {
				return true, err
			}
			if ok := db.trie.setOffset(h, wOff); !ok {
				return true, errors.New("db:Sync: timeWindow sync error: unable to set topic offset in trie")
			}
		}
		return false, nil
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

	blockIdx := db.blocks()
	var (
		upperSeq uint64
		logEntry entry
	)

	blockWriter := newBlockWriter(db.index, blockOffset(blockIdx))
	dataWriter := newDataWriter(&db.data)
	defer func() {
		blockWriter.close()
		dataWriter.close()
	}()

	err := db.wal.Read(func(upperSeq uint64, r *wal.Reader) (ok bool, err error) {
		l := r.Count()
		for i := uint32(0); i < l; i++ {
			logData, ok := r.Next()
			if !ok {
				break
			}
			entryData, data := logData[:entrySize], logData[entrySize:]
			if err := logEntry.UnmarshalBinary(entryData); err != nil {
				return true, err
			}
			db.meter.Recovers.Inc(1)
			msgOffset := logEntry.mSize()
			m := data[:msgOffset]
			if logEntry.msgOffset, err = dataWriter.writeMessage(m); err != nil {
				return true, err
			}
			blockWriter.append(logEntry, blockIdx > startBlockIndex(logEntry.seq))

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
			if ok := db.trie.add(topicHash, we); !ok {
				return true, errors.New("recovery.recoverLog: unable to add entry to trie")
			}
			db.filter.Append(logEntry.seq)
			db.incount()
			db.meter.InMsgs.Inc(1)
			db.meter.InBytes.Inc(int64(logEntry.valueSize))
		}

		if upperSeq < logEntry.seq {
			upperSeq = logEntry.seq
		}

		if err := db.recoverWindowBlocks(); err != nil {
			return true, err
		}

		nBlocks := int32(upperSeq / entriesPerIndexBlock)
		for nBlocks > db.blocks() {
			if _, err := db.newBlock(); err != nil {
				return false, err
			}
		}

		if err := blockWriter.write(); err != nil {
			return true, err
		}
		if _, err := dataWriter.write(); err != nil {
			return true, err
		}

		if err := db.sync(); err != nil {
			return true, err
		}

		return false, nil
	})
	if err != nil {
		return err
	}
	if err := db.wal.SignalLogApplied(upperSeq); err != nil {
		return err
	}
	return nil
}
