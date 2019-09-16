package tracedb

import (
	"bytes"
	"sync"
)

const nul = 0x0

// MID represents a message Id set which can contain only unique valuet.
type MID []ID

// AddUnique adds a subscriber to the set.
func (m *MID) addUnique(value ID) (added bool) {
	if m.contains(value) == false {
		*m = append(*m, value)
		added = true
	}
	return
}

// Remove removes a subscriber from the set.
func (m *MID) remove(value ID) (removed bool) {
	for i, v := range *m {
		if bytes.Equal(v, value) {
			a := *m
			a[i] = a[len(a)-1]
			a[len(a)-1] = nil
			a = a[:len(a)-1]
			*m = a
			removed = true
			return
		}
	}
	return
}

// Contains checks whether a subscriber is in the set.
func (m *MID) contains(value ID) bool {
	for _, v := range *m {
		if bytes.Equal(v, value) {
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
	depth    uint8
	mid      MID
	parent   *part
	children map[key]*part
}

// Trie represents an efficient collection of trie with lookup capability.
type partTrie struct {
	root *part // The root node of the tree.
}

// NewTrie creates a new matcher for the trie.
func NewpartTrie() *partTrie {
	return &partTrie{
		root: &part{
			mid:      MID{},
			children: make(map[key]*part),
		},
	}
}

type trie struct {
	sync.RWMutex
	partTrie *partTrie
	count    int // Number of trie in the trie.
}

// Creates a trie with an initialized trie.
func newtrie() *trie {
	return &trie{
		partTrie: NewpartTrie(),
	}
}

// Count returns the number of trie.
func (t *trie) Count() int {
	t.RLock()
	defer t.RUnlock()
	return t.count
}

// Subscribe adds the Subscriber to the topic and returns a Subscription.
func (t *trie) add(parts []Part, depth uint8, id ID) error {
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
				mid:      MID{},
				parent:   curr,
				children: make(map[key]*part),
			}
			curr.children[k] = child
		}
		curr = child
	}
	if ok := curr.mid.addUnique(id); ok {
		curr.depth = depth
		t.count++
	}

	return nil
}

// Unsubscribe remove the subscription for the topic.
func (t *trie) remove(parts []Part, id ID) error {
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
			// Subscription doesn't exist.
			return nil
		}
		curr = child
	}
	// Remove the subscriber and decrement the counter
	if ok := curr.mid.remove(id); ok {
		t.count--
	}
	return nil
}

// Lookup returns the Subscribers for the given topic.
func (t *trie) lookup(query Ssid) (mid MID) {
	t.RLock()
	defer t.RUnlock()

	t.ilookup(query, uint8(len(query)-1), &mid, t.partTrie.root)
	return
}

func (t *trie) ilookup(query Ssid, depth uint8, mid *MID, part *part) {
	// Add subscribers from the current branch
	for _, s := range part.mid {
		if part.depth == depth || (part.depth >= 23 && depth > part.depth-23) {
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
