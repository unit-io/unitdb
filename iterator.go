package tracedb

import (
	"github.com/golang/snappy"

	"github.com/saffat-in/tracedb/message"
)

type Item struct {
	key       []byte
	value     []byte
	expiresAt uint32
	err       error
}

// Query represents a topic to query and optional contract information.
type Query struct {
	Topic    []byte         // The topic of the message
	Contract uint32         // The contract is used as prefix in the message Id
	parts    []message.Part // Ssid represents a subscription ID which contains a contract and a list of hashes for various parts of the topic.
	prefix   message.ID     // The beginning of the time window.
	cutoff   int64          // The end of the time window.
	keys     []uint32
	Limit    uint32 // The maximum number of elements to return.
}

// ItemIterator is an iterator over DB key/value pairs. It iterates the items in an unspecified order.
type ItemIterator struct {
	db          *DB
	query       *Query
	item        *Item
	queue       []*Item
	next        uint32
	invalidKeys uint32
}

// Next returns the next key/value pair if available, otherwise it returns ErrIterationDone error.
func (it *ItemIterator) Next() {
	it.item = nil
	if len(it.queue) == 0 && it.next < uint32(len(it.query.keys)) {
		h := uint32(it.query.keys[it.next])
		err := it.db.forEachBlock(it.db.blockIndex(h), true, func(b blockHandle) (bool, error) {
			for i := 0; i < entriesPerBlock; i++ {
				e := b.entries[i]
				if e.kvOffset == 0 {
					return b.next == 0, nil
				} else if h == e.hash {
					if e.isExpired() {
						it.invalidKeys++
						return true, nil
					}
					key, val, err := it.db.data.readKeyValue(e, true)
					if err != nil {
						return true, err
					}
					id := message.ID(key)
					if !id.EvalPrefix(it.query.parts, it.query.cutoff) {
						it.invalidKeys++
						return true, nil
					}
					if id.IsEncrypted() {
						val, err = it.db.mem.mac.Decrypt(nil, val)
						if err != nil {
							return true, err
						}
					}
					var e message.Entry
					var buffer []byte
					val, err = snappy.Decode(buffer, val)
					if err != nil {
						return true, err
					}
					err = e.Unmarshal(val)
					if err != nil {
						return true, err
					}
					it.queue = append(it.queue, &Item{key: e.Topic, value: e.Payload, err: err})
					return true, nil
				}
			}
			return false, nil
		})
		it.next++
		if err != nil {
			it.queue = append(it.queue, &Item{err: err})
			return
		}
	}

	if len(it.queue) > 0 {
		it.item = it.queue[0]
		it.queue = it.queue[1:]
	}
}

// First returns the first key/value pair if available.
func (it *ItemIterator) First() {
	if len(it.query.keys) == 0 || it.next >= 1 {
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
	return nil
}

// Key returns the key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (item *Item) Key() []byte {
	return item.key
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
