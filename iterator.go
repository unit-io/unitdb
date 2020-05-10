package unitdb

import (
	"sync"

	"github.com/golang/snappy"

	"github.com/unit-io/unitdb/message"
)

// Item items returned by the iterator
type Item struct {
	topic     []byte
	value     []byte
	expiresAt uint32
	err       error
}

// Query represents a topic to query and optional contract information.
type Query struct {
	Topic      []byte         // The topic of the message
	Contract   uint32         // The contract is used as prefix in the message Id
	parts      []message.Part // parts represents a topic which contains a contract and a list of hashes for various parts of the topic.
	depth      uint8
	contract   uint64
	cutoff     int64 // time limit check on message Ids.
	winEntries []winEntry
	Limit      int // The maximum number of elements to return.
}

// ItemIterator is an iterator over DB topic->key/value pairs. It iterates the items in an unspecified order.
type ItemIterator struct {
	db          *DB
	mu          sync.Mutex
	query       *Query
	item        *Item
	queue       []*Item
	next        int
	invalidKeys int
}

// Next returns the next topic->key/value pair if available, otherwise it returns ErrIterationDone error.
func (it *ItemIterator) Next() {
	it.mu.Lock()
	defer it.mu.Unlock()

	mu := it.db.getMutex(it.query.contract)
	mu.RLock()
	defer mu.RUnlock()
	it.item = nil
	if len(it.queue) == 0 {
		for _, we := range it.query.winEntries[it.next:] {
			err := func() error {
				if we.seq == 0 {
					return nil
				}
				e, err := it.db.readEntry(it.query.contract, we.seq)
				if err != nil {
					if err == errMsgIdDeleted {
						it.invalidKeys++
						return nil
					}
					logger.Error().Err(err).Str("context", "db.readEntry")
					return err
				}
				if e.isExpired() {
					it.invalidKeys++
					it.db.timeWindow.addExpiry(e)
					// if id is expired it does not return an error but continue the iteration
					return nil
				}
				id, val, err := it.db.data.readMessage(e)
				if err != nil {
					logger.Error().Err(err).Str("context", "data.readMessage")
					return err
				}
				msgId := message.ID(id)
				if !msgId.EvalPrefix(it.query.Contract, it.query.cutoff) {
					it.invalidKeys++
					return nil
				}

				if msgId.IsEncrypted() {
					val, err = it.db.mac.Decrypt(nil, val)
					if err != nil {
						logger.Error().Err(err).Str("context", "mac.Decrypt")
						return err
					}
				}
				var buffer []byte
				val, err = snappy.Decode(buffer, val)
				if err != nil {
					logger.Error().Err(err).Str("context", "snappy.Decode")
					return err
				}
				it.queue = append(it.queue, &Item{topic: it.query.Topic, value: val, err: err})
				it.db.meter.Gets.Inc(1)
				it.db.meter.OutMsgs.Inc(1)
				it.db.meter.OutBytes.Inc(int64(e.valueSize))
				return nil
			}()
			if err != nil {
				it.item = &Item{err: err}
			}
			it.next++
			if len(it.queue) > 0 {
				break
			}
		}
	}

	if len(it.queue) > 0 {
		it.item = it.queue[0]
		it.queue = it.queue[1:]
	}
}

// First is similar to init. It query and loads window entries from trie/timeWindowBucket or summary file if available.
func (it *ItemIterator) First() {
	topics := it.db.trie.lookup(it.query.parts, it.query.depth)
	for _, topic := range topics {
		var wEntries []winEntry
		wEntries = it.db.timeWindow.ilookup(topic.hash, it.query.Limit)
		if len(wEntries) > 0 {
			it.query.contract = wEntries[0].contract
			it.query.winEntries = append(it.query.winEntries, wEntries...)
		}
		if len(it.query.winEntries) < it.query.Limit {
			limit := it.query.Limit - len(it.query.winEntries)
			wEntries, _ = it.db.timeWindow.lookup(topic.hash, topic.offset, len(it.query.winEntries), limit)
			if len(wEntries) > 0 {
				it.query.winEntries = append(it.query.winEntries, wEntries...)
			}
		}
	}
	if len(it.query.winEntries) == 0 || it.next >= 1 {
		return
	}
	it.Next()
}

// Item returns pointer to the current item.
// This item is only valid until it.Next() gets called.
func (it *ItemIterator) Item() *Item {
	return it.item
}

// Valid returns false when iteration is done.
func (it *ItemIterator) Valid() bool {
	if (it.next - it.invalidKeys) > it.query.Limit {
		return false
	}
	if len(it.queue) > 0 {
		return true
	}
	return it.item != nil
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error. A memory iterator cannot encounter errors.
func (it *ItemIterator) Error() error {
	return it.item.err
}

// Topic returns the topic of the current item, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (item *Item) Topic() []byte {
	return item.topic
}

// Value returns the value of the current item, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (item *Item) Value() []byte {
	return item.value
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (it *ItemIterator) Release() {
	return
}
