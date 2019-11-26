package memdb

import (
	"errors"
	"sync"
)

// ErrIterationDone is returned by ItemIterator.Next calls when there are no more items to return.
var ErrIterationDone = errors.New("no more items in iterator")

type Item struct {
	dFlag     bool
	topic     []byte
	key       []byte
	value     []byte
	expiresAt uint32
	err       error
}

// ItemIterator is an iterator over DB topic->key/value pairs. It iterates the items in an unspecified order.
type ItemIterator struct {
	db           *DB
	startSeq     uint64
	endSeq       uint64
	nextBlockIdx uint32
	item         *Item
	queue        []*Item
	mu           sync.Mutex
}

// Next returns the next key/value pair if available, otherwise it returns ErrIterationDone error.
func (it *ItemIterator) Next() {
	it.item = nil
	it.mu.Lock()
	defer it.mu.Unlock()
	if len(it.queue) == 0 {
		for it.nextBlockIdx < it.db.nBlocks {
			err := it.db.forEachBlock(it.nextBlockIdx, func(b blockHandle) (bool, error) {
				for i := 0; i < entriesPerBlock; i++ {
					e := b.entries[i]
					if e.kvOffset == 0 {
						return false, nil
					}
					ikey, err := it.db.data.readKey(e)
					if err != nil {
						return true, err
					}
					key, seq, dFlag, expiresAt, err := parseInternalKey(ikey)
					if err != nil {
						return true, err
					}
					if seq <= it.startSeq {
						continue
					}
					if seq > it.endSeq {
						return true, ErrIterationDone
					}
					_, value, err := it.db.data.readKeyValue(e)
					if err != nil {
						it.item.err = err
					}
					topic, err := it.db.data.readTopic(e)
					if err != nil {
						return true, err
					}
					it.queue = append(it.queue, &Item{topic: topic, dFlag: dFlag, key: key, value: value, expiresAt: expiresAt})
				}
				return false, nil
			})
			if err != nil {
				it.item.err = err
			}
			it.nextBlockIdx++
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
	it.nextBlockIdx = uint32(it.startSeq / entriesPerBlock)
	it.Next()
}

// Item returns pointer to the current key-value pair.
// This item is only valid until it.Next() gets called.
func (it *ItemIterator) Item() *Item {
	return it.item
}

// Valid returns false when iteration is done.
func (it *ItemIterator) Valid() bool {
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

// Topic returns the topic of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (item *Item) Topic() []byte {
	return item.topic
}

// DeleteFlag returns the delete flag swet on key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (item *Item) DeleteFlag() bool {
	return item.dFlag
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

// ExpiresAt returns the key expiry value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (item *Item) ExpiresAt() uint32 {
	return item.expiresAt
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (it *ItemIterator) Release() {
	return
}
