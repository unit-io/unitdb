package tracedb

import (
	"sync"

	"github.com/unit-io/tracedb/message"
)

const (
	nul = 0x0
)

// ssid represents a sequence set which can contain only unique values.
type ssid []uint64

// new returns seq set of given cap.
func newSsid(cap uint32) ssid {
	return make([]uint64, 0, cap)
}

// extend extends the cap of seq set.
func (ss *ssid) extend(cap uint32) {
	if cap < ss.len() {
		return
	}
	l := cap - ss.len()
	*ss = append(*ss, make([]uint64, l)...)
}

// shrink shrinks the cap of seq set.
func (ss *ssid) shrink(cap uint32) {
	newssid := make([]uint64, 0, ss.len())
	copy(newssid, *ss)
	*ss = newssid
}

// addUnique adds a seq to the set.
func (ss *ssid) addUnique(value uint64) (added bool) {
	if ss.contains(value) == false {
		*ss = append(*ss, value)
		added = true
	}
	return
}

// remove a seq from the set.
func (ss *ssid) remove(value uint64) (removed bool) {
	for i, v := range *ss {
		// if bytes.Equal(v, value) {
		if v == value {
			a := *ss
			a[i] = a[len(a)-1]
			//a[len(a)-1] = nil
			a = a[:len(a)-1]
			*ss = a
			removed = true
			return
		}
	}
	return
}

// contains checks whether a seq is in the set.
func (ss *ssid) contains(value uint64) bool {
	for _, v := range *ss {
		// if bytes.Equal(v, value) {
		// 	return true
		// }
		if v == value {
			return true
		}
	}
	return false
}

// len length of seq set.
func (ss *ssid) len() uint32 {
	return uint32(len(*ss))
}

type key struct {
	query     uint32
	wildchars uint8
}

type part struct {
	k        key
	depth    uint8
	cap      uint32
	ss       ssid
	parent   *part
	children map[key]*part
}

func (p *part) orphan() {
	if p.parent == nil {
		return
	}

	delete(p.parent.children, p.k)
	if len(p.parent.ss) == 0 && len(p.parent.children) == 0 {
		p.parent.orphan()
	}
}

// partTrie represents an efficient collection of Trie with lookup capability.
type partTrie struct {
	root *part // The root node of the tree.
}

// newPartTrie creates a new matcher for the Trie.
func newPartTrie(cacheCap uint32) *partTrie {
	return &partTrie{
		root: &part{
			cap:      cacheCap,
			ss:       newSsid(cacheCap),
			children: make(map[key]*part),
		},
	}
}

// trie trie data structure to store topic parts
type trie struct {
	sync.RWMutex
	mutex
	partTrie *partTrie
	count    int // Number of Trie in the Trie.
}

// NewTrie new trie creates a Trie with an initialized Trie.
// Mutex is used to lock concurent read/write on a contract, and it does not lock entire trie.
func newTrie(cacheCap uint32) *trie {
	return &trie{
		mutex:    newMutex(),
		partTrie: newPartTrie(cacheCap),
	}
}

// Count returns the number of entries in Trie.
func (t *trie) Count() int {
	t.RLock()
	defer t.RUnlock()
	return t.count
}

// add adds a message seq to topic trie.
func (t *trie) add(contract uint64, parts []message.Part, depth uint8, seq uint64) (added bool) {
	// Get mutex
	mu := t.getMutex(contract)
	mu.Lock()
	defer mu.Unlock()
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
				k:        k,
				cap:      t.partTrie.root.cap,
				ss:       newSsid(t.partTrie.root.cap),
				parent:   curr,
				children: make(map[key]*part),
			}
			t.Lock()
			curr.children[k] = child
			t.Unlock()
		}
		curr = child
	}
	if curr.ss.len() >= curr.cap {
		curr.ss = curr.ss[:curr.ss.len()-1]
	}
	// curr.ss = append([]uint64{seq}, curr.ss...)
	curr.ss = append(curr.ss, seq)
	added = true
	curr.depth = depth
	t.count++

	return
}

// remove removes a message seq from topic trie
func (t *trie) remove(contract uint64, parts []message.Part, seq uint64) (removed bool) {
	mu := t.getMutex(contract)
	mu.Lock()
	defer mu.Unlock()
	curr := t.partTrie.root

	for _, part := range parts {
		k := key{
			query:     part.Query,
			wildchars: part.Wildchars,
		}
		t.RLock()
		child, ok := curr.children[k]
		t.RUnlock()
		if !ok {
			removed = false
			// message seq doesn't exist.
			return
		}
		curr = child
	}
	// Remove a message seq and decrement the counter
	if ok := curr.ss.remove(seq); ok {
		removed = true
		// adjust the cap of the seq set
		if curr.ss.len() > t.partTrie.root.cap {
			curr.cap = curr.ss.len()
			curr.ss.shrink(curr.cap)
		}
		t.count--
	}
	// Remove orphans
	t.Lock()
	defer t.Unlock()
	if len(curr.ss) == 0 && len(curr.children) == 0 {
		curr.orphan()
	}
	return
}

// lookup returns seq set for given topic.
func (t *trie) lookup(contract uint64, parts []message.Part, limit uint32) (ss ssid) {
	t.RLock()
	defer t.RUnlock()
	t.ilookup(contract, parts, uint8(len(parts)-1), &ss, t.partTrie.root, limit)
	return
}

func (t *trie) ilookup(contract uint64, parts []message.Part, depth uint8, ss *ssid, part *part, limit uint32) {
	// Add seq set from the current branch
	if part.depth == depth || (part.depth >= message.TopicMaxDepth && depth > part.depth-message.TopicMaxDepth) {
		var l uint32
		// for _, s := range part.ss {
		// 	*ss = append(*ss, s)
		// }
		*ss = append(*ss, part.ss...)
		// on lookup cap increased to 10 folds of current cap of the seq set of the part
		if ss.len() > limit {
			l = limit
		}
		if part.cap < 2*l {
			part.cap = 2 * l
			part.ss.extend(part.cap)
		}
	}

	// If we're not yet done, continue
	if len(parts) > 0 {
		// Go through the exact match branch
		for k, p := range part.children {
			if k.query == parts[0].Query && uint8(len(parts)) >= k.wildchars+1 {
				t.ilookup(contract, parts[k.wildchars+1:], depth, ss, p, limit)
			}
		}
	}
}
