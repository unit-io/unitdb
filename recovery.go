package tracedb

import (
	"errors"
	"fmt"

	_ "net/http/pprof"

	"github.com/unit-io/bpool"
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

	off := int64(-1)
	var (
		upperSeq uint64
		logEntry entry
		b        blockHandle
		rawData  *bpool.Buffer
	)

	bufSize := int64(1 << 27)
	rawData = db.bufPool.Get()
	defer func() {
		db.bufPool.Put(rawData)
	}()

	nBlocks := int32(float64(db.wal.Seq()) / float64(entriesPerIndexBlock))
	for nBlocks > db.blocks() {
		if _, err := db.newBlock(); err != nil {
			return err
		}
	}

	write := func() error {
		if _, err := db.data.write(rawData.Bytes()); err != nil {
			return err
		}

		// reset blocks
		rawData.Reset()
		return nil
	}

	err := db.wal.Read(func(r *wal.Reader) (bool, error) {
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
			if off != newOff {
				off = newOff
				// write previous block
				if err := b.write(); err != nil {
					return false, err
				}
				b = blockHandle{file: db.index, offset: newOff}
				if err := b.read(); err != nil {
					return false, err
				}
			}

			entryIdx := 0
			for i := 0; i < entriesPerIndexBlock; i++ {
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
			if msgOffset, err := db.data.writeMessage(m); err != nil {
				if err != errLeasedBlock {
					return true, err
				}
				logEntry.msgOffset = msgOffset
			} else {
				rawData.Write(m)
			}
			b.entries[b.entryIdx] = logEntry
			b.entryIdx++
			if b.entryIdx == entriesPerIndexBlock {
				if err := b.write(); err != nil {
					return false, err
				}
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
			if ok := db.trie.add(topicHash, we); !ok {
				return true, errors.New("recovery.recoverLog: unable to add entry to trie")
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

		if upperSeq < logEntry.seq {
			upperSeq = logEntry.seq
		}

		// write any pending block
		if err := b.write(); err != nil {
			return false, err
		}

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

	if err := db.wal.SignalLogApplied(upperSeq); err != nil {
		return err
	}

	return err
}
