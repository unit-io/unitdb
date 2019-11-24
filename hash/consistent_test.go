package hash

import (
	"math"
	"reflect"
	"testing"
)

func TestAnchor(t *testing.T) {
	const (
		blocks = 7
		used   = 7
	)

	a := InitConsistent(blocks, used)

	// Fig. 2(d)
	a.RemoveBlock(6)
	a.RemoveBlock(5)

	// Ex. 13
	a.RemoveBlock(1)

	if !reflect.DeepEqual(a.W, []uint32{0, 4, 2, 3, 4, 5, 6}) {
		t.Fatalf("W = %#+v", a.W)
	}
	if !reflect.DeepEqual(a.L, []uint32{0, 1, 2, 3, 1, 5, 6}) {
		t.Fatalf("L = %#+v", a.L)
	}

	// Ex. 14/15
	a.RemoveBlock(0)

	if !reflect.DeepEqual(a.K, []uint32{3, 4, 2, 3, 4, 5, 6}) {
		t.Fatalf("K = %#+v", a.K)
	}

	// Restore
	a.AddBlock() // 0
	a.AddBlock() // 1
	a.AddBlock() // 5
	a.AddBlock() // 6

	if !reflect.DeepEqual(a.W, []uint32{0, 1, 2, 3, 4, 5, 6}) {
		t.Fatalf("W = %#+v", a.W)
	}
	if !reflect.DeepEqual(a.L, []uint32{0, 1, 2, 3, 4, 5, 6}) {
		t.Fatalf("L = %#+v", a.L)
	}
	if !reflect.DeepEqual(a.K, []uint32{0, 1, 2, 3, 4, 5, 6}) {
		t.Fatalf("K = %#+v", a.K)
	}
}

func TestPaths(t *testing.T) {
	const (
		blocks = 10
		used   = 5
	)
	a := InitConsistent(blocks, used)
	path := make([]uint32, 0, 64)

	const count = 1e6
	sum := 0
	for i := uint32(0); i < count; i++ {
		path = a.GetPath(i, path)
		sum += len(path)
		path = path[:0]
	}
	t.Logf("avg trace   = %v\n", float64(sum)/count)
	t.Logf("1 + ln(a/w) = %v\n", float64(1)+math.Log(float64(blocks)/float64(used)))
}

func TestOrdering(t *testing.T) {
	const (
		blocks = 5
		used   = 5
	)
	a := InitConsistent(blocks, used)

	counts := make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.RemoveBlock(4)
	a.RemoveBlock(3)
	a.RemoveBlock(2)
	t.Logf("removed b=4,3,2\n")

	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.AddBlock()
	a.AddBlock()
	a.AddBlock()
	t.Logf("added b=2,3,4\n")

	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.RemoveBlock(2)
	a.RemoveBlock(3)
	a.RemoveBlock(4)
	t.Logf("removed b=2,3,4\n")

	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.AddBlock()
	a.AddBlock()
	a.AddBlock()
	t.Logf("added b=4,3,2\n")

	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)
}

func TestPreviousBlock(t *testing.T) {
	const (
		blocks = 7
		used   = 1
	)
	a := InitConsistent(blocks, used)
	for i := uint32(0); i < 1e1; i++ {
		t.Logf("key:%d block:%d\n", i, a.FindBlock(i))
	}
	counts := make([]int, blocks)
	for i := uint32(0); i < 1e1; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.AddBlock()
	for i := uint32(0); i < 1e1; i++ {
		cb := a.FindBlock(i)
		pb := a.FindPreviousBlock(i)
		t.Logf("key:%d block:%d\n", i, cb)
		t.Logf("key:%d previousblock:%d\n", i, pb)
	}
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e1; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.AddBlock()
	a.AddBlock()
	a.AddBlock()
	a.AddBlock()
	for i := uint32(0); i < 1e1; i++ {
		cb := a.FindBlock(i)
		pb := a.FindPreviousBlock(i)
		t.Logf("key:%d block:%d\n", i, cb)
		t.Logf("key:%d previousblock:%d\n", i, pb)
	}
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e1; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

}

func TestDistributionSimple(t *testing.T) {
	const (
		blocks = 7
		used   = 1
	)
	a := InitConsistent(blocks, used)

	counts := make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.AddBlock()
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)
	a.AddBlock()
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)
	a.AddBlock()
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)
	a.AddBlock()
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)
	a.AddBlock()
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.RemoveBlock(6)
	a.RemoveBlock(5)
	a.RemoveBlock(4)
	a.RemoveBlock(3)
	a.RemoveBlock(2)

	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)
}

func TestDistributionExtended(t *testing.T) {
	const (
		blocks = 10
		used   = 10
	)
	a := InitConsistent(blocks, used)

	counts := make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.RemoveBlock(9)
	t.Logf("removed b=9\n")
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.RemoveBlock(5)
	t.Logf("removed b=5\n")
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.RemoveBlock(3)
	t.Logf("removed b=3\n")
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.AddBlock()
	t.Logf("re-added b=3\n")
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.AddBlock()
	t.Logf("re-added b=5\n")
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)

	a.AddBlock()
	t.Logf("re-added b=9\n")
	counts = make([]int, blocks)
	for i := uint32(0); i < 1e6; i++ {
		counts[a.FindBlock(i)]++
	}
	t.Logf("%#+v\n", counts)
}
