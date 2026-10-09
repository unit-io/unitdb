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
	"bytes"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/unit-io/unitdb/hash"
	"github.com/unit-io/unitdb/message"
)

// This file is what a query layer (package uql) needs beyond Get and Put:
// the topics that match a pattern, reading one topic by its hash, the topic
// of each entry a read returns, and a hook on every write.

// Op is a write operation.
type Op uint8

// Write operations.
const (
	OpPut Op = iota + 1
	OpDelete
)

// WriteEvent describes a put or delete, for OnWrite hooks.
type WriteEvent struct {
	Op        Op
	Topic     []byte // as the writer gave it, without options such as ?ttl=
	TopicHash uint64
	Contract  uint32
	ID        []byte // the entry's id
	Payload   []byte // a copy; nil for a delete
	ExpiresAt uint32 // 0 when the entry doesn't expire
}

// Wildcard reports whether the event's topic is a wildcard topic.
func (ev WriteEvent) Wildcard() bool {
	return bytes.ContainsRune(ev.Topic, '*') || bytes.HasSuffix(ev.Topic, []byte(message.TopicMultiWildcardSymbol))
}

type _Hooks struct {
	mu    sync.RWMutex
	next  int
	hooks map[int]func(WriteEvent)
}

func (h *_Hooks) active() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.hooks) > 0
}

// OnWrite registers fn to be called after each put and delete succeeds:
// after PutEntry and DeleteEntry, and for each entry of a batch once it is
// committed. fn runs on the writer's goroutine, holding no DB lock; it may
// write to the DB, and is then called for those writes too. The returned
// function removes the hook.
func (db *DB) OnWrite(fn func(WriteEvent)) (remove func()) {
	h := &db.internal.hooks
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.hooks == nil {
		h.hooks = map[int]func(WriteEvent){}
	}
	id := h.next
	h.next++
	h.hooks[id] = fn
	return func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.hooks, id)
	}
}

func (db *DB) writeEvent(op Op, e *Entry) WriteEvent {
	topic := e.Topic
	if i := bytes.IndexByte(topic, '?'); i >= 0 {
		topic = topic[:i]
	}
	ev := WriteEvent{
		Op:        op,
		Topic:     append([]byte(nil), topic...),
		TopicHash: e.entry.topicHash,
		Contract:  e.Contract,
		ID:        append([]byte(nil), e.entry.id...),
		ExpiresAt: e.entry.expiresAt,
	}
	if op == OpPut {
		ev.Payload = append([]byte(nil), e.Payload...)
	}
	return ev
}

func (db *DB) fire(ev WriteEvent) {
	h := &db.internal.hooks
	h.mu.RLock()
	fns := make([]func(WriteEvent), 0, len(h.hooks))
	for _, fn := range h.hooks {
		fns = append(fns, fn)
	}
	h.mu.RUnlock()
	for _, fn := range fns {
		fn(ev)
	}
}

// errBadPattern is returned for a pattern MatchTopics can't use.
var errBadPattern = errors.New("bad topic pattern")

// TopicHash returns the hash of a topic, in the contract (0 for the master
// contract): the TopicHash of its write events and of the entries Read
// returns.
func (db *DB) TopicHash(topic []byte, contract uint32) (uint64, error) {
	if contract == 0 {
		contract = message.MasterContract
	}
	t, _, err := db.parseTopic(contract, topic)
	if err != nil {
		return 0, err
	}
	t.AddContract(contract)
	return t.GetHash(contract), nil
}

type _MatchPart struct {
	hash uint32
	any  bool
}

// MatchTopics returns the hashes of the static topics, in the contract (0
// for the master contract), that match pattern: dot-separated parts where
// "*" matches any one part and a trailing "..." matches the rest, including
// nothing. Wildcard topics written to are not returned: they are read
// through the topics they match.
func (db *DB) MatchTopics(pattern []byte, contract uint32) ([]uint64, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}
	if contract == 0 {
		contract = message.MasterContract
	}
	p := string(pattern)
	rest := false
	if strings.HasSuffix(p, message.TopicMultiWildcardSymbol) {
		rest = true
		p = strings.TrimSuffix(strings.TrimSuffix(p, message.TopicMultiWildcardSymbol), ".")
	}
	var parts []_MatchPart
	if p != "" {
		for _, s := range strings.Split(p, ".") {
			switch {
			case s == "":
				return nil, errBadPattern
			case s == string(message.TopicWildcardSymbol):
				parts = append(parts, _MatchPart{any: true})
			case strings.ContainsAny(s, "*?/"):
				return nil, errBadPattern
			default:
				parts = append(parts, _MatchPart{hash: hash.WithSalt([]byte(s), contract)})
			}
		}
	}
	if len(parts) == 0 && !rest {
		return nil, errBadPattern
	}
	star := hash.WithSalt([]byte{message.TopicWildcardSymbol}, contract)
	t := db.internal.trie
	t.RLock()
	defer t.RUnlock()
	root, ok := t.topicTrie.root.children[_Part{hash: contract}]
	if !ok {
		return nil, nil
	}
	var out []uint64
	var collect func(n *_Node)
	collect = func(n *_Node) {
		for _, top := range n.topics {
			out = append(out, top.hash)
		}
		for part, c := range n.children {
			if static(part, star) {
				collect(c)
			}
		}
	}
	var walk func(n *_Node, rem []_MatchPart)
	walk = func(n *_Node, rem []_MatchPart) {
		if len(rem) == 0 {
			if rest {
				collect(n)
				return
			}
			for _, top := range n.topics {
				out = append(out, top.hash)
			}
			return
		}
		if rem[0].any {
			for part, c := range n.children {
				if static(part, star) {
					walk(c, rem[1:])
				}
			}
			return
		}
		if c, ok := n.children[_Part{hash: rem[0].hash}]; ok {
			walk(c, rem[1:])
		}
	}
	walk(root, parts)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// static reports whether a trie part is a part of static topics: not a
// wildcard topic's.
func static(p _Part, star uint32) bool {
	return p.wildchars == 0 && p.hash != message.Wildcard && p.hash != star
}

// Entry read by GetEntries and ReadTopic.
type Item struct {
	ID        []byte // the entry's id: DeleteEntry takes it
	TopicHash uint64 // the topic the entry was put to: the read topic or a matching wildcard topic
	Payload   []byte
}

// GetEntries returns the items matching the query, as GetWithIDs does, with
// the topic each was put to.
func (db *DB) GetEntries(q *Query) ([]Item, error) {
	items, ids, topics, err := db.getEntries(q, true, true)
	if err != nil {
		return nil, err
	}
	out := make([]Item, len(items))
	for i := range items {
		out[i] = Item{ID: ids[i], TopicHash: topics[i], Payload: items[i]}
	}
	return out, nil
}

// ReadOptions bound a ReadTopic.
type ReadOptions struct {
	Since time.Time // entries put at or after; zero for all
	Limit int       // at most; 0 for the default query limit
}

// ReadTopic returns the entries put to the topic of hash, newest first: its
// own entries, without those of wildcard topics that match it.
func (db *DB) ReadTopic(topicHash uint64, opts ReadOptions) ([]Item, error) {
	if err := db.ok(); err != nil {
		return nil, err
	}
	raw, ok := db.internal.topics.get(topicHash)
	if !ok {
		return nil, nil
	}
	var t message.Topic
	if err := t.Unmarshal(raw); err != nil {
		return nil, err
	}
	if len(t.Parts) == 0 {
		return nil, nil
	}
	off, ok := db.internal.trie.getOffset(topicHash)
	if !ok {
		return nil, nil
	}
	limit := opts.Limit
	if limit <= 0 {
		limit = db.opts.queryOptions.defaultQueryLimit
	}
	if limit > db.opts.queryOptions.maxQueryLimit {
		limit = db.opts.queryOptions.maxQueryLimit
	}
	q := &Query{Contract: t.Parts[0].Hash, Limit: limit}
	if !opts.Since.IsZero() {
		q.internal.cutoff = opts.Since.Unix()
	}
	mu := db.internal.mutex.getMutex(message.Prefix(t.Parts))
	mu.RLock()
	defer mu.RUnlock()

	var out []Item
	for fetch := limit; ; fetch *= 2 {
		wes := db.internal.timeWindow.lookup(db.fs, topicHash, off, q.internal.cutoff, fetch)
		seqs := make([]uint64, 0, len(wes))
		for _, we := range wes {
			seqs = append(seqs, we.seq())
		}
		sort.Slice(seqs, func(i, j int) bool { return seqs[i] > seqs[j] })
		out = out[:0]
		for i, seq := range seqs {
			if len(out) == limit {
				break
			}
			if i > 0 && seq == seqs[i-1] {
				continue // in memory and on disk while a sync runs
			}
			val, stored, _, ok, err := db.readValue(q, _Query{topicHash: topicHash, seq: seq})
			if err != nil {
				return out, err
			}
			if ok {
				id := message.NewID(seq)
				copy(id[:8], stored[:8])
				out = append(out, Item{ID: id, TopicHash: topicHash, Payload: val})
			}
		}
		if len(out) == limit || len(wes) < fetch || fetch >= db.opts.queryOptions.maxQueryLimit {
			break
		}
	}
	return out, nil
}

// DeleteTopicEntry deletes an entry by the hash of the topic it was put to
// and its id, as ReadTopic and GetEntries return them: for a caller that
// has the topic's hash and not its name. Write hooks get the event with no
// Topic.
func (db *DB) DeleteTopicEntry(topicHash uint64, id []byte) error {
	if err := db.ok(); err != nil {
		return err
	}
	switch {
	case db.opts.flags.immutable:
		return errImmutable
	case len(id) != 16: // a full message ID: prefix and sequence
		return errMsgIDEmpty
	}
	mid := message.ID(id)
	if err := db.delete(topicHash, mid.Sequence()); err != nil {
		return err
	}
	if db.internal.hooks.active() {
		var contract uint32
		if raw, ok := db.internal.topics.get(topicHash); ok {
			var t message.Topic
			if t.Unmarshal(raw) == nil && len(t.Parts) > 0 {
				contract = t.Parts[0].Hash
			}
		}
		db.fire(WriteEvent{Op: OpDelete, TopicHash: topicHash, Contract: contract, ID: append([]byte(nil), id...)})
	}
	return nil
}
