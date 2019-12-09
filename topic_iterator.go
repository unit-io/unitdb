package tracedb

import (
	"github.com/saffat-in/tracedb/message"
)

type Topic struct {
	parts []message.Part
	depth uint8
	id    message.ID
	err   error
}

// TopicIterator is an iterator over DB key/value pairs. It iterates the Topics in an unspecified order.
type TopicIterator struct {
	db           *DB
	topic        *Topic
	queue        []*Topic
	nextBlockIdx uint32
}

// Next returns the next key/value pair if available, otherwise it returns ErrIterationDone error.
func (it *TopicIterator) Next() {
	it.topic = nil
	if len(it.queue) == 0 {
		for it.nextBlockIdx < it.db.nBlocks {
			err := it.db.readBlock(it.nextBlockIdx, false, func(b blockHandle) (bool, error) {
				for i := 0; i < entriesPerBlock; i++ {
					e := b.entries[i]
					if e.mOffset == 0 {
						continue
					}
					if e.isExpired() {
						return false, nil
					}
					id, err := it.db.data.readId(e)
					if err != nil {
						return true, err
					}
					etopic, err := it.db.data.readTopic(e)
					if err != nil {
						return true, err
					}
					topic := new(message.Topic)
					err = topic.Unmarshal(etopic)
					if err != nil {
						return true, err
					}
					it.queue = append(it.queue, &Topic{parts: topic.Parts, depth: topic.Depth, id: message.ID(id), err: err})
				}
				return true, nil
			})
			if err != nil {
				it.queue = append(it.queue, &Topic{err: err})
			}
			it.nextBlockIdx++
			if len(it.queue) > 0 {
				break
			}
		}
	}
	if len(it.queue) > 0 {
		it.topic = it.queue[0]
		it.queue = it.queue[1:]
	}
}

// First returns the first key/value pair if available.
func (it *TopicIterator) First() {
	if it.nextBlockIdx >= 1 {
		return
	}
	it.Next()
}

// Topic returns pointer to the current key-value pair.
// This Topic is only valid until it.Next() gets called.
func (it *TopicIterator) Topic() *Topic {
	return it.topic
}

// Valid returns false when iteration is done.
func (it *TopicIterator) Valid() bool {
	if len(it.queue) > 0 {
		return true
	}
	return it.topic != nil
}

// Error returns any accumulated error. Exhausting all the key/value pairs
// is not considered to be an error. A memory iterator cannot encounter errors.
func (it *TopicIterator) Error() error {
	return nil
}

// Parts returns the topic parts, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (Topic *Topic) Parts() []message.Part {
	return Topic.parts
}

// Depth returns the topic depth, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (Topic *Topic) Depth() uint8 {
	return Topic.depth
}

// ID returns the id to store into topic part, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (Topic *Topic) ID() message.ID {
	return Topic.id
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (it *TopicIterator) Release() {
	return
}
