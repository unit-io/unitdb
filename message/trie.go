package message

import (
	"sync"
)

const nul = 0x0

// MID represents a message id set which can contain only unique values.
type MID []uint32

// addUnique adds a message id to the set.
func (m *MID) addUnique(value uint32) (added bool) {
	if m.contains(value) == false {
		*m = append(*m, value)
		added = true
	}
	return
}

// remove a message id from the set.
func (m *MID) remove(value uint32) (removed bool) {
	for i, v := range *m {
		//if bytes.Equal(v, value) {
		if v == value {
			a := *m
			a[i] = a[len(a)-1]
			//a[len(a)-1] = nil
			a = a[:len(a)-1]
			*m = a
			removed = true
			return
		}
	}
	return
}

// contains checks whether a message id is in the set.
func (m *MID) contains(value uint32) bool {
	for _, v := range *m {
		// if bytes.Equal(v, value) {
		// 	return true
		// }
		if v == value {
			return true
		}
	}
	return false
}

type key struct {
	query     uint32
	wildchars uint8
}

type part struct {
	k        key
	depth    uint8
	mid      MID
	final    bool
	parent   *part
	children map[key]*part
}

func (p *part) orphan() {
	if p.parent == nil {
		return
	}

	delete(p.parent.children, p.k)
	p.parent.final = true
	if len(p.parent.mid) == 0 && len(p.parent.children) == 0 {
		p.parent.orphan()
	}
}

// partTrie represents an efficient collection of Trie with lookup capability.
type partTrie struct {
	root *part // The root node of the tree.
}

// NewPartTrie creates a new matcher for the Trie.
func NewpartTrie() *partTrie {
	return &partTrie{
		root: &part{
			mid:      MID{},
			final:    false,
			children: make(map[key]*part),
		},
	}
}

type Trie struct {
	sync.RWMutex
	partTrie *partTrie
	count    int // Number of Trie in the Trie.
}

// Creates a Trie with an initialized Trie.
func NewTrie() *Trie {
	return &Trie{
		partTrie: NewpartTrie(),
	}
}

// Count returns the number of Trie.
func (t *Trie) Count() int {
	t.RLock()
	defer t.RUnlock()
	return t.count
}

// add the message id to the topic.
func (t *Trie) Add(parts []Part, depth uint8, id uint32) error {
	t.Lock()
	defer t.Unlock()
	curr := t.partTrie.root
	for _, p := range parts {
		k := key{
			query:     p.Query,
			wildchars: p.Wildchars,
		}
		child, ok := curr.children[k]
		if !ok {
			child = &part{
				k:        k,
				mid:      MID{},
				final:    false,
				parent:   curr,
				children: make(map[key]*part),
			}
			curr.children[k] = child
		}
		curr = child
	}
	curr.final = true
	if ok := curr.mid.addUnique(id); ok {
		curr.depth = depth
		t.count++
	}

	return nil
}

// remove the message id for the topic.
func (t *Trie) Remove(parts []Part, id uint32) error {
	t.Lock()
	defer t.Unlock()
	curr := t.partTrie.root

	for _, part := range parts {
		k := key{
			query:     part.Query,
			wildchars: part.Wildchars,
		}
		child, ok := curr.children[k]
		if !ok {
			// message id doesn't exist.
			return nil
		}
		curr = child
	}
	// Remove the message id and decrement the counter
	if ok := curr.mid.remove(id); ok {
		t.count--
	}
	// Remove orphans
	if len(curr.mid) == 0 && len(curr.children) == 0 {
		curr.orphan()
	}
	return nil
}

// Lookup returns the message Ids for the given topic.
func (t *Trie) Lookup(query Ssid) (mid MID) {
	t.RLock()
	defer t.RUnlock()

	t.ilookup(query, uint8(len(query)-1), &mid, t.partTrie.root)
	return
}

func (t *Trie) ilookup(query Ssid, depth uint8, mid *MID, part *part) {
	// Add message ids from the current branch
	for _, s := range part.mid {
		if part.depth == depth || (part.depth >= TopicMaxDepth && depth > part.depth-TopicMaxDepth) {
			mid.addUnique(s)
		}
	}

	// If we're not yet done, continue
	if len(query) > 0 {
		// Go through the exact match branch
		for k, p := range part.children {
			if k.query == query[0] && uint8(len(query)) >= k.wildchars+1 {
				t.ilookup(query[k.wildchars+1:], depth, mid, p)
			}
		}
	}
}
