package filter

const (
	bloomHashes uint64 = 7
	bloomBits   uint64 = 160000
)

type FilterGenerator struct {
	filter *Filter
}

// NewFilterGenerator returns a new filter generator.
func NewFilterGenerator() *FilterGenerator {
	return &FilterGenerator{filter: newFilter(bloomBits, bloomHashes)}
}

// Append adds a key to the filter block.
func (b *FilterGenerator) Append(h uint64) {
	b.filter.Add(h)
}

// Finish finishes building the filter block and returns a slice to its contents.
func (b *FilterGenerator) Finish() []byte {
	return b.filter.Bytes()
}

type FilterBlock struct {
	filter *Filter
}

// NewFilterBlock returns new filter block, it is used to test key presense in the filter.
func NewFilterBlock(b []byte) *FilterBlock {
	return &FilterBlock{
		filter: newFilterFromBytes(b, bloomBits, bloomHashes),
	}
}

// Test is used to test for key presense in the filter
func (b *FilterBlock) Test(h uint64) bool {
	return b.filter.Test(h)
}
