package store_test

import (
	"testing"

	"github.com/unit-io/unitdb/server/internal/store"
)

func TestCanary(t *testing.T) {
	newStore(t, 0, ring(0), true)
	for _, run := range []string{"20261004T213000Z", "20261005T213000Z"} {
		if err := store.WriteCanary(run); err != nil {
			t.Fatal(err)
		}
	}
	msg, rec, err := store.ReadCanary()
	if err != nil || msg != "20261005T213000Z" || rec != "20261005T213000Z" {
		t.Errorf("canary: message %q, record %q, %v; want the newest run", msg, rec, err)
	}
}
