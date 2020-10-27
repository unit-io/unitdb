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
	"sync"

	"github.com/unit-io/unitdb/message"
)

const (
	nul = 0x0
)

type _Topic struct {
	hash   uint64
	offset int64
}

type _Topics []_Topic

func newTopic(hash uint64, off int64) _Topic {
	return _Topic{hash: hash, offset: off}
}

// addUnique adds topic to the set.
func (top *_Topics) addUnique(value _Topic) (added bool) {
	for i, v := range *top {
		if v.hash == value.hash {
			(*top)[i].offset = value.offset
			return false
		}
	}
	*top = append(*top, value)
	added = true
	return
}

type _Part struct {
	hash      uint32
	wildchars uint8
}

type _Node struct {
	part     _Part
	depth    uint8
	parent   *_Node
	children map[_Part]*_Node
	topics   _Topics
}

func (n *_Node) orphan() {
	if n.parent == nil {
		return
	}

	delete(n.parent.children, n.part)
	if len(n.parent.children) == 0 {
		n.parent.orphan()
	}
}

// _topicTrie represents an efficient collection of Trie with lookup capability.
type _TopicTrie struct {
	summary map[uint64]*_Node // summary is map of topichash to node of tree.
	root    *_Node            // The root node of the tree.
}

// newTopicTrie creates a new Trie.
func newTopicTrie() *_TopicTrie {
	return &_TopicTrie{
		summary: make(map[uint64]*_Node),
		root: &_Node{
			children: make(map[_Part]*_Node),
		},
	}
}

// _Trie trie data structure to store topic parts
type _Trie struct {
	sync.RWMutex
	mutex     _Mutex
	topicTrie *_TopicTrie
}

// newTrie new trie creates a Trie with an initialized Trie.
// Mutex is used to lock concurent read/write on a contract, and it does not lock entire trie.
func newTrie() *_Trie {
	return &_Trie{
		mutex:     newMutex(),
		topicTrie: newTopicTrie(),
	}
}

// Count returns the number of topics in the Trie.
func (t *_Trie) Count() int {
	t.RLock()
	defer t.RUnlock()
	return len(t.topicTrie.summary)
}

// add adds a topic to trie.
func (t *_Trie) add(topic _Topic, parts []message.Part, depth uint8) (added bool) {
	// Get mutex
	mu := t.mutex.getMutex(topic.hash)
	mu.Lock()
	defer mu.Unlock()
	if _, ok := t.topicTrie.summary[topic.hash]; ok {
		return false
	}
	curr := t.topicTrie.root
	for _, p := range parts {
		newPart := _Part{
			hash:      p.Hash,
			wildchars: p.Wildchars,
		}
		t.RLock()
		child, ok := curr.children[newPart]
		t.RUnlock()
		if !ok {
			child = &_Node{
				part:     newPart,
				parent:   curr,
				children: make(map[_Part]*_Node),
			}
			t.Lock()
			curr.children[newPart] = child
			t.Unlock()
		}
		curr = child
	}
	t.Lock()
	curr.topics.addUnique(topic)
	t.topicTrie.summary[topic.hash] = curr
	t.Unlock()
	added = true
	curr.depth = depth
	return
}

// lookup returns window entry set for given topic.
func (t *_Trie) lookup(query []message.Part, depth, topicType uint8) (tops _Topics) {
	t.RLock()
	defer t.RUnlock()
	t.ilookup(query, depth, topicType, &tops, t.topicTrie.root)
	return
}

func (t *_Trie) ilookup(query []message.Part, depth, topicType uint8, tops *_Topics, currNode *_Node) {
	// Add topics from the current branch.
	if currNode.depth == depth || (topicType == message.TopicStatic && currNode.part.hash == message.Wildcard) {
		for _, topic := range currNode.topics {
			tops.addUnique(topic)
		}
	}

	// If done then stop.
	if len(query) == 0 {
		return
	}

	q := query[0]
	// Go through the wildcard match branch.
	for part, n := range currNode.children {
		switch {
		case part.hash == q.Hash && q.Wildchars == part.wildchars:
			t.ilookup(query[1:], depth, topicType, tops, n)
		case part.hash == q.Hash && uint8(len(query)) >= part.wildchars+1:
			t.ilookup(query[part.wildchars+1:], depth, topicType, tops, n)
		case part.hash == message.Wildcard:
			t.ilookup(query[:], depth, topicType, tops, n)
		}
	}
}

func (t *_Trie) getOffset(topicHash uint64) (off int64, ok bool) {
	t.RLock()
	defer t.RUnlock()
	if curr, ok := t.topicTrie.summary[topicHash]; ok {
		for _, topic := range curr.topics {
			if topic.hash == topicHash {
				return topic.offset, ok
			}
		}
	}
	return off, ok
}

func (t *_Trie) setOffset(topic _Topic) (ok bool) {
	t.Lock()
	defer t.Unlock()
	if curr, ok := t.topicTrie.summary[topic.hash]; ok {
		curr.topics.addUnique(topic)
		return ok
	}
	return false
}
