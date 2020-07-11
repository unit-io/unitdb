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
type (
	internalQuery struct {
		parts      []message.Part // parts represents a topic which contains a contract and a list of hashes for various parts of the topic.
		depth      uint8
		topicType  uint8
		contract   uint64
		cutoff     int64 // time limit check on message Ids.
		winEntries []winEntry

		opts *QueryOptions
	}
	Query struct {
		internalQuery
		Topic    []byte // The topic of the message
		Contract uint32 // The contract is used as prefix in the message Id
		Limit    int    // The maximum number of elements to return.
	}
)

// NewQuery creates a new query structure from the topic.
func NewQuery(topic []byte) *Query {
	return &Query{
		Topic: topic,
	}
}

// WithContract sets contract on query.
func (q *Query) WithContract(contract uint32) *Query {
	q.Contract = contract
	return q
}

// WithLimit sets query limit
func (q *Query) WithLimit(limit int) *Query {
	q.Limit = limit
	return q
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

func (q *Query) parse() error {
	if q.Contract == 0 {
		q.Contract = message.MasterContract
	}
	topic := new(message.Topic)
	//Parse the Key
	topic.ParseKey(q.Topic)
	// Parse the topic
	topic.Parse(q.Contract, true)
	if topic.TopicType == message.TopicInvalid {
		return errBadRequest
	}
	topic.AddContract(q.Contract)
	q.parts = topic.Parts
	q.depth = topic.Depth
	q.topicType = topic.TopicType
	q.contract = message.Contract(q.parts)
	// In case of last, include it to the query
	if from, limit, ok := topic.Last(); ok {
		q.cutoff = from.Unix()
		switch {
		case (q.Limit == 0 && limit == 0):
			q.Limit = q.opts.DefaultQueryLimit
		case q.Limit > q.opts.MaxQueryLimit || limit > q.opts.MaxQueryLimit:
			q.Limit = q.opts.MaxQueryLimit
		case limit > q.Limit:
			q.Limit = limit
		}
	}
	if q.Limit == 0 {
		q.Limit = q.opts.DefaultQueryLimit
	}
	return nil
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
				e, err := it.db.readEntry(we.topicHash, we.seq)
				if err != nil {
					if err == errMsgIdDoesNotExist {
						logger.Error().Err(err).Str("context", "db.readEntry")
						return err
					}
					it.invalidKeys++
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
	it.db.lookup(it.query)
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
