/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unitdb

import (
	"github.com/unit-io/unitdb/message"
)

// Query represents a topic to query and optional contract information.
type (
	_Query struct {
		topicHash uint64
		seq       uint64
	}
	_InternalQuery struct {
		parts      []message.Part // The parts represents a topic which contains a contract and a list of hashes for various parts of the topic.
		depth      uint8
		topicType  uint8
		prefix     uint64 // The prefix is generated from contract and first of the topic.
		cutoff     int64  // The cutoff is time limit check on message IDs.
		winEntries []_Query

		opts *_QueryOptions
	}
	Query struct {
		internal _InternalQuery
		Topic    []byte // The topic of the message.
		Contract uint32 // The contract is used as prefix in the message ID.
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

// WithLimit sets query limit.
func (q *Query) WithLimit(limit int) *Query {
	q.Limit = limit
	return q
}

func (q *Query) parse() error {
	if q.Contract == 0 {
		q.Contract = message.MasterContract
	}
	topic := new(message.Topic)
	//Parse the Key.
	topic.ParseKey(q.Topic)
	// Parse the topic.
	topic.Parse(q.Contract, true)
	if topic.TopicType == message.TopicInvalid {
		return errBadRequest
	}
	topic.AddContract(q.Contract)
	q.internal.parts = topic.Parts
	q.internal.depth = topic.Depth
	q.internal.topicType = topic.TopicType
	q.internal.prefix = message.Prefix(q.internal.parts)
	// In case of last, include it to the query.
	if from, limit, ok := topic.Last(); ok {
		q.internal.cutoff = from.Unix()
		switch {
		case (q.Limit == 0 && limit == 0):
			q.Limit = q.internal.opts.defaultQueryLimit
		case q.Limit > q.internal.opts.maxQueryLimit || limit > q.internal.opts.maxQueryLimit:
			q.Limit = q.internal.opts.maxQueryLimit
		case limit > q.Limit:
			q.Limit = limit
		}
	}
	if q.Limit == 0 {
		q.Limit = q.internal.opts.defaultQueryLimit
	}
	return nil
}
