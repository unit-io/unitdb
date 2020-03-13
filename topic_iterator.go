package tracedb

import (
	"github.com/unit-io/tracedb/message"
)

// Topic topic returned by iterator
type Topic struct {
	parts    []message.Part
	contract uint64
	hash     uint64
	depth    uint8
	seq      uint64
	err      error
}

// TopicIterator is an iterator over DB topic->key/value pairs. It iterates the Topics in an unspecified order.
type TopicIterator struct {
	db             *DB
	topic          *Topic
	queue          []*Topic
	nextBlockIndex int32
}

// Next returns the next key/value pair if available.
func (it *TopicIterator) Next() {
	it.topic = nil
	if len(it.queue) == 0 {
		for it.nextBlockIndex < it.db.blocks() {
			err := func() error {
				off := blockOffset(it.nextBlockIndex)
				b := blockHandle{file: it.db.index, offset: off}
				if err := b.read(); err != nil {
					return err
				}
				for i := 0; i < entriesPerIndexBlock; i++ {
					e := b.entries[i]
					if e.msgOffset == 0 {
						continue
					}

					if e.isExpired() {
						it.db.timeWindow.addExpiry(e)
						continue
					}
					id, err := it.db.data.readId(e)
					if err != nil {
						return err
					}
					t, err := it.db.data.readTopic(e)
					if err != nil {
						return err
					}
					topic := new(message.Topic)
					err = topic.Unmarshal(t)
					if err != nil {
						return err
					}
					contract := message.Contract(topic.Parts)
					it.queue = append(it.queue, &Topic{contract: contract, hash: topic.GetHash(contract), parts: topic.Parts, depth: topic.Depth, seq: message.ID(id).Seq(), err: err})
				}
				return nil
			}()
			if err != nil {
				it.queue = append(it.queue, &Topic{err: err})
			}
			it.nextBlockIndex++
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
	return it.topic.err
}

// Topic returns pointer to the current topic.
// This Topic is only valid until it.Next() gets called.
func (it *TopicIterator) Topic() *Topic {
	return it.topic
}

// Contract returns contract of the topic, or nil if done.
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (Topic *Topic) Contract() uint64 {
	return Topic.contract
}

// Hash returns topic hash, or nil if done.
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (Topic *Topic) Hash() uint64 {
	return Topic.hash
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

// Seq returns the seq of topic, or nil if done. The
// caller should not modify the contents of the returned slice, and its contents
// may change on the next call to Next.
func (Topic *Topic) Seq() uint64 {
	return Topic.seq
}

// Release releases associated resources. Release should always succeed and can
// be called multiple times without causing error.
func (it *TopicIterator) Release() {
	return
}
