package internal

import (
	"os"
	"sync"
	"testing"

	"github.com/unit-io/unitdb/server/internal/store"
)

var openStoreOnce sync.Once

// openTestStore opens the store, once, in a temporary directory, for the
// tests that use it directly, unless another test opened it.
func openTestStore(t *testing.T) {
	t.Helper()
	openStoreOnce.Do(func() {
		if store.IsOpen() {
			return // opened by another test
		}
		dir, err := os.MkdirTemp("", "unitdb-test")
		if err != nil {
			t.Fatal(err)
		}
		conf := `{"reset": true, "adapters": {"unitdb": {"database": "test", "mem_size": 1000000}}}`
		if err := store.Open(dir, conf, true); err != nil {
			t.Fatal(err)
		}
	})
}
