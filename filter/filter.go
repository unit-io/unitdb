package filter

const (
	bloomHashes uint64 = 7
	bloomBits   uint64 = 160000
)

type FilterGenerator struct {
	filter *Filter
}

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

func NewFilterBlock(b []byte) *FilterBlock {
	return &FilterBlock{
		filter: newFilterFromBytes(b, bloomBits, bloomHashes),
	}
}

func (b *FilterBlock) Test(h uint64) bool {
	return b.filter.Test(h)
}
