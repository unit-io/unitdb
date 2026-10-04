//go:build lockcheck

package lockcheck

import "testing"

func TestOrder(t *testing.T) {
	Acquire(1, "a")
	Acquire(2, "b")
	Release(2)
	Release(1)

	Acquire(2, "b")
	defer Release(2)
	defer func() {
		if recover() == nil {
			t.Fatal("a taken holding b, of higher rank: no panic")
		}
	}()
	Acquire(1, "a")
}
