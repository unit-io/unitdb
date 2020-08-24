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

type topic struct {
	hash   uint64
	offset int64
}

type topics []topic

func newTopic(hash uint64, off int64) topic {
	return topic{hash: hash, offset: off}
}

// addUnique adds topic to the set.
func (top *topics) addUnique(value topic) (added bool) {
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

type part struct {
	hash      uint32
	wildchars uint8
}

type node struct {
	part     part
	depth    uint8
	parent   *node
	children map[part]*node
	topics   topics
}

func (n *node) orphan() {
	if n.parent == nil {
		return
	}

	delete(n.parent.children, n.part)
	if len(n.parent.children) == 0 {
		n.parent.orphan()
	}
}

// topicTrie represents an efficient collection of Trie with lookup capability.
type topicTrie struct {
	summary map[uint64]*node // summary is map of topichash to node of tree.
	root    *node            // The root node of the tree.
}

// newTopicTrie creates a new Trie.
func newTopicTrie() *topicTrie {
	return &topicTrie{
		summary: make(map[uint64]*node),
		root: &node{
			children: make(map[part]*node),
		},
	}
}

// trie trie data structure to store topic parts
type trie struct {
	sync.RWMutex
	mutex
	topicTrie *topicTrie
}

// newTrie new trie creates a Trie with an initialized Trie.
// Mutex is used to lock concurent read/write on a contract, and it does not lock entire trie.
func newTrie() *trie {
	return &trie{
		mutex:     newMutex(),
		topicTrie: newTopicTrie(),
	}
}

// Count returns the number of topics in the Trie.
func (t *trie) Count() int {
	t.RLock()
	defer t.RUnlock()
	return len(t.topicTrie.summary)
}

// add adds a topic to trie.
func (t *trie) add(topic topic, parts []message.Part, depth uint8) (added bool) {
	// Get mutex
	mu := t.getMutex(topic.hash)
	mu.Lock()
	defer mu.Unlock()
	if _, ok := t.topicTrie.summary[topic.hash]; ok {
		return false
	}
	curr := t.topicTrie.root
	for _, p := range parts {
		newPart := part{
			hash:      p.Hash,
			wildchars: p.Wildchars,
		}
		t.RLock()
		child, ok := curr.children[newPart]
		t.RUnlock()
		if !ok {
			child = &node{
				part:     newPart,
				parent:   curr,
				children: make(map[part]*node),
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
func (t *trie) lookup(query []message.Part, depth, topicType uint8) (tops topics) {
	t.RLock()
	defer t.RUnlock()
	t.ilookup(query, depth, topicType, &tops, t.topicTrie.root)
	return
}

func (t *trie) ilookup(query []message.Part, depth, topicType uint8, tops *topics, currNode *node) {
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

func (t *trie) getOffset(topicHash uint64) (off int64, ok bool) {
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

func (t *trie) setOffset(top topic) (ok bool) {
	t.Lock()
	defer t.Unlock()
	if curr, ok := t.topicTrie.summary[top.hash]; ok {
		curr.topics.addUnique(top)
		return ok
	}
	return false
}
