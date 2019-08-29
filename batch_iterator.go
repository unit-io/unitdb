package tracedb

import (
	"errors"
	"sync"
)

// ErrIterationDone is returned by ItemIterator.Next calls when there are no more items to return.
var ErrBatchIterationDone = errors.New("no more items in iterator")

type batchitem struct {
	key   []byte
	value []byte
}

// ItemIterator is an iterator over DB key/value pairs. It iterates the items in an unspecified order.
type BatchIterator struct {
	db            *memdb
	nextBucketIdx uint32
	queue         []batchitem
	mu            sync.Mutex
}

// Next returns the next key/value pair if available, otherwise it returns ErrIterationDone error.
func (it *BatchIterator) Next() ([]byte, []byte, error) {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.db.mu.RLock()
	defer it.db.mu.RUnlock()

	if len(it.queue) == 0 {
		for it.nextBucketIdx < it.db.nBuckets {
			err := it.db.forEachBucket(it.nextBucketIdx, func(b bucketHandle) (bool, error) {
				for i := 0; i < entriesPerBucket; i++ {
					sl := b.entries[i]
					if sl.kvOffset == 0 {
						return true, nil
					}
					ikey, value, err := it.db.data.readKeyValue(sl)
					if err == ErrKeyExpired {
						return false, nil
					}
					if err != nil {
						return true, err
					}
					key, _, _, _, err := parseInternalKey(ikey)
					it.queue = append(it.queue, batchitem{key: key, value: value})
				}
				return false, nil
			})
			if err != nil {
				return nil, nil, err
			}
			it.nextBucketIdx++
			if len(it.queue) > 0 {
				break
			}
		}
	}

	if len(it.queue) > 0 {
		item := it.queue[0]
		it.queue = it.queue[1:]
		return item.key, item.value, nil
	}

	return nil, nil, ErrBatchIterationDone
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error. A memory iterator cannot encounter errors.
func (it *BatchIterator) Error() error {
	return nil
}

// Key returns the key of the current key/value pair, or nil if done. The caller
// should not modify the contents of the returned slice, and its contents may
// change on the next call to Next.
func (it *BatchIterator) Key() []byte {
	return nil
}

// Value returns the value of the current key/value pair, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (it *BatchIterator) Value() []byte {
	return nil
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (it *BatchIterator) Release() {
	return
}
