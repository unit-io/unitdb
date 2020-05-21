package unitdb

import (
	"sync"

	"github.com/unit-io/unitdb/message"
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
func newPartTrie() *partTrie {
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
func newTrie() *trie {
	return &trie{
		mutex:    newMutex(),
		partTrie: newPartTrie(),
	}
}

// Count returns the number of topics in the Trie.
func (t *trie) Count() int {
	t.RLock()
	defer t.RUnlock()
	return len(t.partTrie.summary)
}

// add adds a topic to trie.
func (t *trie) add(topicHash uint64, parts []message.Part, depth uint8) (added bool) {
	// Get mutex
	mu := t.getMutex(topicHash)
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
func (t *trie) lookup(query []message.Part, depth uint8) (tops topics) {
	t.RLock()
	defer t.RUnlock()
	// fmt.Println("trie.lookup: depth, parts ", depth, query)
	t.ilookup(query, depth, &tops, t.partTrie.root)
	return
}

func (t *trie) ilookup(query []message.Part, depth uint8, tops *topics, currpart *part) {
	// Add window entry set from the current branch
	var p *part
	var k key
	if currpart.depth == depth || (currpart.depth >= message.TopicMaxDepth && depth > currpart.depth-message.TopicMaxDepth) {
		topic := topic{hash: currpart.topicHash, offset: currpart.offset}
		tops.addUnique(topic)
	}

	// If we're not yet done, continue
	if len(query) > 0 {
		q := query[0]
		// Go through the exact match branch
		for k, p = range currpart.children {
			if k.query == q.Query && q.Wildchars == k.wildchars {
				// fmt.Println("trie.ilookup: topicHash, wildchars, depth, queryHash, partHash ", p.topicHash, k.wildchars, depth, q.Query, k.query)
				t.ilookup(query[1:], depth, tops, p)
			}
			if k.query == q.Query && uint8(len(query)) >= k.wildchars+1 {
				// fmt.Println("trie.ilookup: topicHash,wildchars,  depth, queryHash, partHash ", p.topicHash, k.wildchars, depth, q.Query, k.query)
				t.ilookup(query[k.wildchars+1:], depth, tops, p)
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
