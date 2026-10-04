package store_test

import (
	"testing"

	"github.com/unit-io/unitdb/server/internal/store"
)

// TestProbe checks that the health probe writes and reads back, repeatedly,
// sealed and not.
func TestProbe(t *testing.T) {
	for _, seal := range []bool{false, true} {
		newStore(t, 0, ring(0), seal)
		for i := 0; i < 3; i++ {
			if err := store.Probe(); err != nil {
				t.Fatalf("sealed %v: %v", seal, err)
			}
		}
		store.Close()
	}
}
