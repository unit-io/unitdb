package tracedb

import (
	"sync"

	"github.com/unit-io/tracedb/message"
)

const (
	nul = 0x0
)

// timeEntries represents a time entry set which can contain only unique values.
type timeEntries []timeEntry

// new returns time entry set of given cap.
func newTimeEntries(cap uint32) timeEntries {
	return make([]timeEntry, 0, cap)
}

// extend extends the cap of time entry set.
func (ts *timeEntries) extend(cap uint32) {
	if cap < ts.len() {
		return
	}
	l := cap - ts.len()
	*ts = append(*ts, make([]timeEntry, l)...)
}

// shrink shrinks the cap of seq set.
func (ts *timeEntries) shrink(cap uint32) {
	newts := make([]timeEntry, 0, ts.len())
	copy(newts, *ts)
	*ts = newts
}

// addUnique adds a seq to the set.
func (ts *timeEntries) addUnique(value timeEntry) (added bool) {
	if ts.contains(value) == false {
		*ts = append(*ts, value)
		added = true
	}
	return
}

// remove a seq from the set.
func (ts *timeEntries) remove(value timeEntry) (removed bool) {
	for i, v := range *ts {
		// if bytes.Equal(v, value) {
		if v == value {
			a := *ts
			a[i] = a[len(a)-1]
			//a[len(a)-1] = nil
			a = a[:len(a)-1]
			*ts = a
			removed = true
			return
		}
	}
	return
}

// contains checks whether a seq is in the set.
func (ts *timeEntries) contains(value timeEntry) bool {
	for _, v := range *ts {
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
func (ts *timeEntries) len() uint32 {
	return uint32(len(*ts))
}

type key struct {
	query     uint32
	wildchars uint8
}

type part struct {
	k        key
	depth    uint8
	cap      uint32
	ts       timeEntries
	parent   *part
	children map[key]*part
	offset   int64
}

func (p *part) orphan() {
	if p.parent == nil {
		return
	}

	delete(p.parent.children, p.k)
	if len(p.parent.ts) == 0 && len(p.parent.children) == 0 {
		p.parent.orphan()
	}
}

// partTrie represents an efficient collection of Trie with lookup capability.
type partTrie struct {
	summary map[uint64]*part // summary is map of topichash to part
	root    *part            // The root node of the tree.
}

// newPartTrie creates a new matcher for the Trie.
func newPartTrie(cacheCap uint32) *partTrie {
	return &partTrie{
		summary: make(map[uint64]*part),
		root: &part{
			cap:      cacheCap,
			ts:       newTimeEntries(cacheCap),
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
func (t *trie) add(contract uint64, topicHash uint64, parts []message.Part, depth uint8, te timeEntry) (added bool) {
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
				ts:       newTimeEntries(t.partTrie.root.cap),
				parent:   curr,
				children: make(map[key]*part),
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
	if curr.ts.len() >= curr.cap {
		curr.ts = curr.ts[1:] // remove first if capacity has reached
	}
	if te.seq > 0 {
		curr.ts = append(curr.ts, te)
	}
	added = true
	curr.depth = depth
	t.count++

	return added
}

// remove removes a message seq from topic trie
func (t *trie) remove(contract uint64, parts []message.Part, e timeEntry) (removed bool) {
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
	if ok := curr.ts.remove(e); ok {
		removed = true
		// adjust cap of the seq set
		if curr.ts.len() > t.partTrie.root.cap {
			curr.cap = curr.ts.len()
			curr.ts.shrink(curr.cap)
		}
		t.count--
	}
	// Remove orphans
	t.Lock()
	defer t.Unlock()
	if len(curr.ts) == 0 && len(curr.children) == 0 {
		curr.orphan()
	}
	return
}

// lookup returns seq set for given topic.
func (t *trie) lookup(contract uint64, parts []message.Part, limit uint32) (ts timeEntries, offs []int64, fanout bool) {
	t.RLock()
	defer t.RUnlock()
	t.ilookup(contract, parts, uint8(len(parts)-1), &ts, &offs, t.partTrie.root, limit)
	return ts, offs, ts.len() < limit
}

func (t *trie) ilookup(contract uint64, parts []message.Part, depth uint8, ts *timeEntries, offs *[]int64, part *part, limit uint32) {
	// Add seq set from the current branch
	if part.depth == depth || (part.depth >= message.TopicMaxDepth && depth > part.depth-message.TopicMaxDepth) {
		var l uint32
		*offs = append(*offs, part.offset)
		*ts = append(*ts, part.ts...)
		// on lookup cap increased to 2 folds of current cap of the seq set
		if ts.len() > limit {
			l = limit
		}
		if part.cap < 2*l {
			part.cap = 2 * l
			part.ts.extend(part.cap)
		}
	}

	// If we're not yet done, continue
	if len(parts) > 0 {
		// Go through the exact match branch
		for k, p := range part.children {
			if k.query == parts[0].Query && uint8(len(parts)) >= k.wildchars+1 {
				t.ilookup(contract, parts[k.wildchars+1:], depth, ts, offs, p, limit)
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
	return 0, ok
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
