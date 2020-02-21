package tracedb

import (
	"sync"

	"github.com/golang/snappy"

	"github.com/unit-io/tracedb/message"
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
	Topic        []byte         // The topic of the message
	Contract     uint32         // The contract is used as prefix in the message Id
	parts        []message.Part // parts represents a subscription ID which contains a contract and a list of hashes for various parts of the topic.
	contract     uint64
	cutoff       int64 // The end of the time window.
	topicHash    uint64
	topicOffsets []int64
	entries      []timeEntry
	fanout       bool
	Limit        uint32 // The maximum number of elements to return.
}

// ItemIterator is an iterator over DB key/value pairs. It iterates the items in an unspecified order.
type ItemIterator struct {
	db          *DB
	mu          sync.Mutex
	query       *Query
	item        *Item
	queue       []*Item
	next        uint32
	invalidKeys uint32
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
		for _, te := range it.query.entries[it.next:] {
			err := func() error {
				if te.seq == 0 {
					return nil
				}
				e, err := it.db.readEntry(it.query.contract, te.seq)
				if err != nil {
					return err
				}
				if e.isExpired() {
					if ok := it.db.trie.remove(it.query.contract, it.query.parts, te); ok {
						it.db.timeWindow.addExpiry(e)
					}
					it.invalidKeys++
					// if id is expired it does not return an error but continue the iteration
					return nil
				}
				pid, val, err := it.db.data.readMessage(e)
				if err != nil {
					return err
				}
				id := message.ID(pid)
				if !id.EvalPrefix(it.query.contract, it.query.cutoff) {
					it.invalidKeys++
					return nil
				}

				if id.IsEncrypted() {
					val, err = it.db.mac.Decrypt(nil, val)
					if err != nil {
						return err
					}
				}
				var buffer []byte
				val, err = snappy.Decode(buffer, val)
				if err != nil {
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

// First returns the first key/value pair if available.
func (it *ItemIterator) First() {
	it.query.entries, it.query.topicOffsets, it.query.fanout = it.db.trie.lookup(it.query.contract, it.query.parts, it.query.Limit)
	if it.query.fanout {
		limit := it.query.Limit - uint32(len(it.query.entries))
		pEntries, _ := it.db.timeWindow.lookup(it.query.topicHash, it.query.topicOffsets, limit)
		it.query.entries = append(it.query.entries, pEntries...)
	}
	if len(it.query.entries) == 0 || it.next >= 1 {
		return
	}
	it.Next()
}

// Item returns pointer to the current key-value pair.
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

// Topic returns the topic of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (item *Item) Topic() []byte {
	return item.topic
}

// Value returns the value of the current key/value pair, or nil if done. The
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
