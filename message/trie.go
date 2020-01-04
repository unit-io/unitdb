package message

import (
	"sync"
)

const nul = 0x0

// MID represents a message id set which can contain only unique values.
type MID []uint64

// addUnique adds a message id to the set.
func (mid *MID) addUnique(value uint64) (added bool) {
	if mid.contains(value) == false {
		*mid = append(*mid, value)
		added = true
	}
	return
}

// remove a message id from the set.
func (mid *MID) remove(value uint64) (removed bool) {
	for i, v := range *mid {
		// if bytes.Equal(v, value) {
		if v == value {
			a := *mid
			a[i] = a[len(a)-1]
			//a[len(a)-1] = nil
			a = a[:len(a)-1]
			*mid = a
			removed = true
			return
		}
	}
	return
}

// contains checks whether a message id is in the set.
func (mid *MID) contains(value uint64) bool {
	for _, v := range *mid {
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
	parent   *part
	children map[key]*part
}

func (p *part) orphan() {
	if p.parent == nil {
		return
	}

	delete(p.parent.children, p.k)
	if len(p.parent.mid) == 0 && len(p.parent.children) == 0 {
		p.parent.orphan()
	}
}

// partTrie represents an efficient collection of Trie with lookup capability.
type partTrie struct {
	root *part // The root node of the tree.
}

// NewPartTrie creates a new matcher for the Trie.
func NewPartTrie() *partTrie {
	return &partTrie{
		root: &part{
			mid:      MID{},
			children: make(map[key]*part),
		},
	}
}

// Trie trie data structure to store topic parts
type Trie struct {
	sync.RWMutex
	partTrie *partTrie
	count    int // Number of Trie in the Trie.
}

// NewTrie new trie creates a Trie with an initialized Trie.
func NewTrie() *Trie {
	return &Trie{
		partTrie: NewPartTrie(),
	}
}

// Count returns the number of Trie.
func (t *Trie) Count() int {
	t.RLock()
	defer t.RUnlock()
	return t.count
}

// Add adds the message seq to the topic trie.
func (t *Trie) Add(parts []Part, depth uint8, seq uint64) (added bool) {
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
				parent:   curr,
				children: make(map[key]*part),
			}
			curr.children[k] = child
		}
		curr = child
	}
	// if ok := curr.mid.addUnique(seq); ok {
	curr.mid = append(curr.mid, seq)
	added = true
	curr.depth = depth
	t.count++
	// }

	return
}

// Remove remove the message seq of the topic trie
func (t *Trie) Remove(parts []Part, seq uint64) (removed bool) {
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
			removed = false
			// message id doesn't exist.
			return
		}
		curr = child
	}
	// Remove the message id and decrement the counter
	if ok := curr.mid.remove(seq); ok {
		removed = true
		t.count--
	}
	// Remove orphans
	if len(curr.mid) == 0 && len(curr.children) == 0 {
		curr.orphan()
	}
	return
}

// Lookup returns the message Ids for the given topic.
func (t *Trie) Lookup(parts []Part) (mid MID) {
	t.RLock()
	defer t.RUnlock()
	t.ilookup(parts, uint8(len(parts)-1), &mid, t.partTrie.root)
	return
}

func (t *Trie) ilookup(parts []Part, depth uint8, mid *MID, part *part) {
	// Add message ids from the current branch
	for _, s := range part.mid {
		if part.depth == depth || (part.depth >= TopicMaxDepth && depth > part.depth-TopicMaxDepth) {
			// mid.addUnique(s)
			*mid = append(*mid, s)
		}
	}

	// If we're not yet done, continue
	if len(parts) > 0 {
		// Go through the exact match branch
		for k, p := range part.children {
			if k.query == parts[0].Query && uint8(len(parts)) >= k.wildchars+1 {
				t.ilookup(parts[k.wildchars+1:], depth, mid, p)
			}
		}
	}
}
