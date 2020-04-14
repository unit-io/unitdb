package tracedb

import (
	"sync"

	"github.com/unit-io/tracedb/message"
)

const (
	nul = 0x0
)

type key struct {
	query     uint32
	wildchars uint8
}

type part struct {
	k         key
	depth     uint8
	parent    *part
	children  map[key]*part
	offset    int64
	topicHash uint64
}

func (p *part) orphan() {
	if p.parent == nil {
		return
	}

	delete(p.parent.children, p.k)
	if len(p.parent.children) == 0 {
		p.parent.orphan()
	}
}

// partTrie represents an efficient collection of Trie with lookup capability.
type partTrie struct {
	summary map[uint64]*part // summary is map of topichash to node of tree.
	root    *part            // The root node of the tree.
}

// newPartTrie creates a new part Trie.
func newPartTrie(cacheCap uint32) *partTrie {
	return &partTrie{
		summary: make(map[uint64]*part),
		root: &part{
			children: make(map[key]*part),
		},
	}
}

// trie trie data structure to store topic parts
type trie struct {
	sync.RWMutex
	mutex
	partTrie *partTrie
}

// NewTrie new trie creates a Trie with an initialized Trie.
// Mutex is used to lock concurent read/write on a contract, and it does not lock entire trie.
func newTrie(cacheCap uint32) *trie {
	return &trie{
		mutex:    newMutex(),
		partTrie: newPartTrie(cacheCap),
	}
}

// Count returns the number of topics in the Trie.
func (t *trie) Count() int {
	t.RLock()
	defer t.RUnlock()
	return len(t.partTrie.summary)
}

// add adds a topic to trie.
func (t *trie) add(contract uint64, topicHash uint64, parts []message.Part, depth uint8) (added bool) {
	// Get mutex
	mu := t.getMutex(contract)
	mu.Lock()
	defer mu.Unlock()
	if _, ok := t.partTrie.summary[topicHash]; ok {
		return true
	}
	curr := t.partTrie.root
	for _, p := range parts {
		k := key{
			query:     p.Query,
			wildchars: p.Wildchars,
		}
		t.RLock()
		child, ok := curr.children[k]
		t.RUnlock()
		if !ok {
			child = &part{
				k:         k,
				parent:    curr,
				children:  make(map[key]*part),
				topicHash: topicHash,
			}
			t.Lock()
			curr.children[k] = child
			t.Unlock()
		}
		curr = child
	}
	t.Lock()
	t.partTrie.summary[topicHash] = curr
	t.Unlock()
	added = true
	curr.depth = depth
	return
}

// lookup returns window entry set for given topic.
func (t *trie) lookup(contract uint64, parts []message.Part, limit uint32) (topics []uint64, offs []int64) {
	t.RLock()
	mu := t.getMutex(contract)
	mu.Lock()
	defer func() {
		t.RUnlock()
		mu.Unlock()
	}()

	t.ilookup(contract, parts, uint8(len(parts)-1), &topics, &offs, t.partTrie.root, limit)
	return topics, offs
}

func (t *trie) ilookup(contract uint64, parts []message.Part, depth uint8, topics *[]uint64, offs *[]int64, part *part, limit uint32) {
	l := limit
	// Add window entry set from the current branch
	if part.depth == depth || (part.depth >= message.TopicMaxDepth && depth > part.depth-message.TopicMaxDepth) {
		*topics = append(*topics, part.topicHash)
		*offs = append(*offs, part.offset)
	}

	// If we're not yet done, continue
	if len(parts) > 0 {
		// Go through the exact match branch
		for k, p := range part.children {
			if k.query == parts[0].Query && uint8(len(parts)) >= k.wildchars+1 {
				t.ilookup(contract, parts[k.wildchars+1:], depth, topics, offs, p, l)
			}
		}
	}
}

func (t *trie) getOffset(topicHash uint64) (off int64, ok bool) {
	t.RLock()
	defer t.RUnlock()
	if curr, ok := t.partTrie.summary[topicHash]; ok {
		return curr.offset, ok
	}
	return off, ok
}

func (t *trie) setOffset(topicHash uint64, off int64) (ok bool) {
	t.Lock()
	defer t.Unlock()
	if curr, ok := t.partTrie.summary[topicHash]; ok {
		if curr.offset < off {
			curr.offset = off
		}
		return ok
	}
	return false
}
