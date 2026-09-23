package filter

const (
	bloomHashes uint64 = 7
	bloomBits   uint64 = 160000
)

// Generator bloom filter generator.
type Generator struct {
	filter *Filter
}

// NewFilterGenerator returns a new filter generator.
func NewFilterGenerator() *Generator {
	return &Generator{filter: newFilter(bloomBits, bloomHashes)}
}

// Append adds a key to the filter block.
func (b *Generator) Append(h uint64) {
	b.filter.Add(h)
}

// Finish finishes building the filter block and returns a slice to its contents.
func (b *Generator) Finish() []byte {
	return b.filter.Bytes()
}

// Bytes returns a slice to filter block contents.
func (b *Generator) Bytes() []byte {
	return b.filter.Bytes()
}

// NewFilterGeneratorFromBytes restores a generator from bytes returned by Bytes,
// so entries can keep being appended to a persisted filter.
func NewFilterGeneratorFromBytes(b []byte) *Generator {
	return &Generator{filter: newFilterFromBytes(b, bloomBits, bloomHashes)}
}

// Test returns false if h was definitely never appended.
func (b *Generator) Test(h uint64) bool {
	return b.filter.Test(h)
}

// Size returns the length in bytes of an encoded filter.
func Size() int {
	return int(Uint64Bytes * (bloomHashes + (bloomBits+63)/64))
}

// Block is a filter block
type Block struct {
	filter *Filter
}

// NewFilterBlock returns new filter block, it is used to test key presence in the filter.
func NewFilterBlock(b []byte) *Block {
	return &Block{
		filter: newFilterFromBytes(b, bloomBits, bloomHashes),
	}
}

// Test is used to test for key presence in the filter.
func (b *Block) Test(h uint64) bool {
	return b.filter.Test(h)
}
