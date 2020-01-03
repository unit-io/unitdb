// Package hash provides a minimal-memory AnchorHash consistent-hash implementation for Go.
//
// [AnchorHash: A Scalable Consistent Hash]: https://arxiv.org/abs/1812.09674
package hash

// Consistent a minimal-memory AnchorHash implementation.
type Consistent struct {
	// We use an integer array A of size a to represent the Anchor.
	//
	// Each bucket b ∈ {0, 1, ..., a−1} is represented by A[b] that either equals 0 if b
	// is a working bucket (i.e., A[b] = 0 if b ∈ W), or else equals the size of the working
	// set just after its removal (i.e., A[b] = |Wb| if b ∈ R).
	A []uint32
	// K stores the successor for each removed bucket b (i.e. the bucket that replaced it in W).
	K []uint32
	// W always contains the current set of working blocks in their desired order.
	W []uint32
	// L stores the most recent location for each bucket within W.
	L []uint32
	// R saves removed blocks in a LIFO order for possible future bucket additions.
	R []uint32
	// N is the current length of W
	N uint32
}

// InitConsistent a new anchor with a given capacity and initial size.
//
// 	INITANCHOR(a, w)
// 	A[b] ← 0 for b = 0, 1, ..., a−1    ◃ |Wb| ← 0 for b ∈ A
// 	R ← ∅                              ◃ Empty stack
// 	N ← w                              ◃ Number of initially working blocks
// 	K[b] ← L[b] ← W[b] ← b for b = 0, 1, ..., a−1
// 	for b = a−1 downto w do            ◃ Remove initially unused blocks
// 	  REMOVEBUCKET(b)
func InitConsistent(blocks, used int) *Consistent {
	c := &Consistent{
		A: make([]uint32, blocks),
		K: make([]uint32, blocks),
		W: make([]uint32, blocks),
		L: make([]uint32, blocks),
		R: make([]uint32, blocks-used, blocks),
		N: uint32(used),
	}
	for b := uint32(0); b < uint32(used); b++ {
		c.K[b], c.W[b], c.L[b] = b, b, b
	}
	for b, r := uint32(blocks)-1, 0; b >= uint32(used); b, r = b-1, r+1 {
		c.A[b], c.R[r] = b, b
	}
	return c
}

// FindBlock find the blocks which a hash-key is assigned to.
//
// If the path for a given key contains any non-working blocks, the path (and in turn,
// the assigned bucket for the key) will be determined by the order in which the non-working
// blocks were removed. To maintain consistency in a distributed system, all agents must
// reach consensus on the ordering of changes to the working set. For more information,
// see Section III, Theorem 1 in the paper.
//
// 	FINDBLOCK(k)
// 	b ← hash(k) mod a
// 	while A[b] > 0 do          ◃ b is removed
// 	  h ← hb(k)                ◃ hb(k) ≡ hash(k) mod A[b]
// 	  while A[h] ≥ A[b] do     ◃ Wb[h] != h, b removed prior to h
// 	    h ← K[h]               ◃ search for Wb[h]
// 	  b ← h
// 	return b
func (c *Consistent) FindBlock(key uint32) uint32 {
	A, K := c.A, c.K
	ha, hb, hc, hd := fleaInit(uint64(key))
	b := FastMod(uint64(hd), uint64(len(A)))
	for A[b] > 0 {
		ha, hb, hc, hd = fleaRound(ha, hb, hc, hd)
		h := FastMod(uint64(hd), uint64(A[b]))
		for A[h] >= A[b] {
			h = K[h]
		}
		b = h
	}
	return b
}

// FindPreviousBlock find the block recently removed.
func (c *Consistent) FindPreviousBlock(key uint32) uint32 {
	A, K, W, R, N := c.A, c.K, c.W, c.R, c.N-1
	cb := R[len(R)-1]
	A[cb] = N
	K[cb] = W[N]

	ha, hb, hc, hd := fleaInit(uint64(key))
	b := FastMod(uint64(hd), uint64(len(A)))
	for A[b] > 0 {
		ha, hb, hc, hd = fleaRound(ha, hb, hc, hd)
		h := FastMod(uint64(hd), uint64(A[b]))
		for A[h] >= A[b] {
			h = K[h]
		}
		b = h
	}
	return b
}

// GetPath get the path to the bucket which a hash-key is assigned to.
//
// The returned path will contain all blocks traversed while searching for a
// working bucket. The final bucket in the path will be the assigned bucket for
// the given key.
//
// Blocks will be appended to the provided buffer, though a different slice will
// be returned if the length of the path exceeds the capacity of the buffer.
//
// If the path for a given key contains any non-working blocks, the path (and in turn,
// the assigned bucket for the key) will be determined by the order in which the non-working
// blocks were removed. To maintain consistency in a distributed system, all agents must
// reach consensus on the ordering of changes to the working set. For more information,
// see Section III, Theorem 1 in the paper.
//
// 	GETPATH(k, P)
// 	b ← hash(k) mod a
// 	P.push(b)
// 	while A[b] > 0 do          ◃ b is removed
// 	  h ← hb(k)                ◃ hb(k) ≡ hash(k) mod A[b]
// 	  P.push(h)
// 	  while A[h] ≥ A[b] do     ◃ Wb[h] != h, b removed prior to h
// 	    h ← K[h]               ◃ search for Wb[h]
// 	    P.push(h)
// 	  b ← h
// 	return P
func (c *Consistent) GetPath(key uint32, pathBuffer []uint32) []uint32 {
	A, K := c.A, c.K
	ha, hb, hc, hd := fleaInit(uint64(key))
	b := FastMod(uint64(hd), uint64(len(A)))
	pathBuffer = append(pathBuffer, b)
	for A[b] > 0 {
		ha, hb, hc, hd = fleaRound(ha, hb, hc, hd)
		h := FastMod(uint64(hd), uint64(A[b]))
		pathBuffer = append(pathBuffer, h)
		for A[h] >= A[b] {
			h = K[h]
			pathBuffer = append(pathBuffer, h)
		}
		b = h
	}
	return pathBuffer
}

// AddBlock add a block to the anchor.
//
// 	ADDBLOCK()
// 	b ← R.pop()
// 	A[b] ← 0       ◃ W ← W ∪ {b}, delete Wb
// 	L[W[N]] ← N
// 	W[L[b]] ← K[b] ← b
// 	N ← N + 1
// 	return b
func (c *Consistent) AddBlock() uint32 {
	A, K, W, L, R, N := c.A, c.K, c.W, c.L, c.R, c.N
	b := R[len(R)-1]
	c.R = R[:len(R)-1]
	A[b] = 0
	L[W[N]] = N
	W[L[b]], K[b] = b, b
	c.N++
	return b
}

// RemoveBlock remove a block from the anchor.
//
// 	REMOVEBLOCK(b)
// 	R.push(b)
// 	N ← N − 1
// 	A[b] ← N       ◃ Wb ← W \ b, A[b] ← |Wb|
// 	W[L[b]] ← K[b] ← W[N]
// 	L[W[N]] ← L[b]
func (c *Consistent) RemoveBlock(b uint32) {
	if c.A[b] != 0 {
		return
	}
	c.N--
	A, K, W, L, N := c.A, c.K, c.W, c.L, c.N
	c.R = append(c.R, b)
	A[b] = N
	W[L[b]], K[b] = W[N], W[N]
	L[W[N]] = L[b]
}
