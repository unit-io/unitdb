package hash

import (
	"fmt"
	"testing"
)

func TestRingGetN(t *testing.T) {
	ring := NewRing(20, nil)
	ring.Add("one", "two", "three")

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		for n := 1; n <= 4; n++ {
			items := ring.GetN(key, n)
			want := n
			if want > 3 {
				want = 3
			}
			if len(items) != want {
				t.Fatalf("GetN(%q, %d) returned %d items, want %d: %v", key, n, len(items), want, items)
			}
			if items[0] != ring.Get(key) {
				t.Fatalf("GetN(%q, %d)[0] = %q, want Get's %q", key, n, items[0], ring.Get(key))
			}
			seen := map[string]bool{}
			for _, item := range items {
				if seen[item] {
					t.Fatalf("GetN(%q, %d) repeats %q: %v", key, n, item, items)
				}
				seen[item] = true
			}
			// A shorter list is a prefix of a longer one.
			if longer := ring.GetN(key, n+1); fmt.Sprint(longer[:len(items)]) != fmt.Sprint(items) {
				t.Fatalf("GetN(%q, %d) = %v is not a prefix of GetN(%q, %d) = %v", key, n, items, key, n+1, longer)
			}
		}
	}

	if items := NewRing(20, nil).GetN("key", 2); items != nil {
		t.Fatalf("GetN on an empty ring = %v, want nil", items)
	}
}

// TestRingSpreadsSimilarKeys checks that keys differing only in their last
// characters, such as consecutive session ids or numbered topics, get owners
// as good as independent: a key's owner repeats the previous key's about as
// often as chance would have it, and each node owns a fair share.
func TestRingSpreadsSimilarKeys(t *testing.T) {
	ring := NewRing(20, nil)
	ring.Add("one", "two", "three")
	for _, format := range []string{"session/%d", "3376684800/groups.x.t%d"} {
		n := 3000
		counts := make(map[string]int)
		same, prev := 0, ""
		for i := 0; i < n; i++ {
			owner := ring.Get(fmt.Sprintf(format, 205217347+i))
			counts[owner]++
			if owner == prev {
				same++
			}
			prev = owner
		}
		t.Logf("%s: consecutive keys share an owner %.0f%% of the time; shares %v of %d", format, 100*float64(same)/float64(n-1), counts, n)
		if frac := float64(same) / float64(n-1); frac > 0.5 {
			t.Errorf("%s: consecutive keys share an owner %.0f%% of the time, want about a third", format, 100*frac)
		}
		for _, node := range []string{"one", "two", "three"} {
			if share := float64(counts[node]) / float64(n); share < 0.15 || share > 0.55 {
				t.Errorf("%s: %s owns %.0f%% of the keys", format, node, 100*share)
			}
		}
	}
}
