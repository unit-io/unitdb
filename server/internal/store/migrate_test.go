package store_test

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/store"
)

// legacyStore is what a v0.6.0 node stored for a test: replicas of topics, a
// hint, seen ids, a record of the security state and a subscription, under
// the fixed ids v0.6.0 used.
type legacyStore struct {
	topics   []string
	replicas map[string][]string // by topic, oldest first
	hintID   []byte
	seen     []string
	secID    []byte
	subID    []byte
}

const legacyNode = "node-b"

func writeLegacy(t *testing.T, marker string) *legacyStore {
	t.Helper()
	l := &legacyStore{replicas: make(map[string][]string)}
	future := time.Now().Add(time.Hour).Unix()
	for i := 0; i < 3; i++ {
		topic := fmt.Sprintf("groups.legacy.%s.t%d", marker, i)
		l.topics = append(l.topics, topic)
		for j := 0; j < 4; j++ {
			payload := fmt.Sprintf("%s-replica-%d-%d", marker, i, j)
			expiresAt := int64(0)
			if j%2 == 1 {
				expiresAt = future
			}
			if err := store.PutLegacyReplicaForTest(contract, topic, []byte(payload), expiresAt); err != nil {
				t.Fatal(err)
			}
			l.replicas[topic] = append(l.replicas[topic], payload)
		}
	}
	// An expired replica is not moved.
	if err := store.PutLegacyReplicaForTest(contract, l.topics[0], []byte(marker+"-expired"), time.Now().Add(-time.Minute).Unix()); err != nil {
		t.Fatal(err)
	}
	// A topic indexed twice, as v0.6.0 did after a restart.
	if err := store.PutLegacyIndexForTest(contract, l.topics[1]); err != nil {
		t.Fatal(err)
	}
	var err error
	if l.hintID, err = store.Hint.NewID(); err != nil {
		t.Fatal(err)
	}
	if err := store.PutLegacyHintForTest(legacyNode, l.hintID, []byte(marker+"-hint")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("%s-seen-%d", marker, i)
		if err := store.PutLegacySeenForTest(id); err != nil {
			t.Fatal(err)
		}
		l.seen = append(l.seen, id)
	}
	if l.secID, err = store.Security.NewID(); err != nil {
		t.Fatal(err)
	}
	if err := store.PutLegacySecurityForTest(l.secID, []byte(`{"contracts":{}}`)); err != nil {
		t.Fatal(err)
	}
	if l.subID, err = store.Subscription.NewID(); err != nil {
		t.Fatal(err)
	}
	if err := store.PutLegacySubscriptionForTest(contract, l.subID, l.topics[0], []byte(marker+"-sub")); err != nil {
		t.Fatal(err)
	}
	if err := store.Flush(); err != nil {
		t.Fatal(err)
	}
	return l
}

// check checks that the store reads what l wrote, each record once, from
// where it is kept now, and that the old namespaces the store moves itself
// are empty.
func (l *legacyStore) check(t *testing.T, sealed bool) {
	t.Helper()
	for _, topic := range l.topics {
		msgs, err := store.Message.GetAll(contract, topic, "")
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, m := range msgs {
			got = append(got, string(m.Payload))
		}
		sort.Strings(got)
		want := append([]string(nil), l.replicas[topic]...)
		sort.Strings(want)
		if strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("%s: replicas %q, want %q", topic, got, want)
		}
		hist, err := store.Message.History(contract, topic)
		if err != nil || len(hist) != len(want) {
			t.Errorf("%s: history of %d, want %d (%v)", topic, len(hist), len(want), err)
		}
		for _, h := range hist {
			if strings.HasSuffix(string(h.Payload), "1") || strings.HasSuffix(string(h.Payload), "3") {
				if h.ExpiresAt == 0 {
					t.Errorf("%s: %s lost its expiry", topic, h.Payload)
				}
			}
		}
		// Kept under $sys.replica, in the contract's namespace.
		raw, err := store.GetRawForTest(contract, store.SysTopicForTest("replica", topic))
		if err != nil || len(raw) != len(want) {
			t.Errorf("%s: %d records under $sys.replica, want %d (%v)", topic, len(raw), len(want), err)
		}
		for _, b := range raw {
			if (sealedWith(b) == 0) != sealed {
				t.Errorf("%s: a moved replica is sealed with %d", topic, sealedWith(b))
			}
		}
	}
	refs := map[store.TopicRef]bool{}
	for _, ref := range store.Message.Topics() {
		refs[ref] = true
	}
	for _, topic := range l.topics {
		if !refs[store.TopicRef{Contract: contract, Topic: topic}] {
			t.Errorf("%s is not in the topic index: %v", topic, refs)
		}
	}
	index, err := store.GetForTest(store.SysContract, store.SysTopicForTest("index", "topics"))
	if err != nil || len(index) != len(l.topics) {
		t.Errorf("%d entries under $sys.index, want %d (%v)", len(index), len(l.topics), err)
	}
	recent, err := store.Seen.Recent(100)
	if err != nil {
		t.Fatal(err)
	}
	count := map[string]int{}
	for _, id := range recent {
		count[id]++
	}
	for _, id := range l.seen {
		if count[id] != 1 {
			t.Errorf("seen id %s read %d times, want once", id, count[id])
		}
	}
	// Subscriptions are of connections a restart closed: not moved, and
	// not read.
	if subs, err := store.Subscription.Get(contract, l.topics[0]); err != nil || len(subs) != 0 {
		t.Errorf("an old subscription is read: %q %v", subs, err)
	}
	left := store.LegacyLeftForTest(contract, l.topics, []string{legacyNode})
	for _, kind := range []string{"index", "replica", "seen"} {
		if left[kind] != 0 {
			t.Errorf("%d %s records left in the old namespaces", left[kind], kind)
		}
	}
	// Moved by the cluster and the security state's loader.
	if left["hint"] != 1 || left["security"] != 1 {
		t.Errorf("old hints %d and security records %d, want 1 each: the store does not move them", left["hint"], left["security"])
	}
	ids, payloads, err := store.Hint.Legacy(legacyNode)
	if err != nil || len(ids) != 1 || string(ids[0][8:]) != string(l.hintID[8:]) || !strings.HasSuffix(string(payloads[0]), "-hint") {
		t.Errorf("old hint: %q %q %v", ids, payloads, err)
	}
	if err := store.Hint.DeleteLegacy(legacyNode, ids[0]); err != nil {
		t.Fatal(err)
	}
	secIDs, _, err := store.Security.Legacy()
	if err != nil || len(secIDs) != 1 {
		t.Fatalf("old security records: %d %v", len(secIDs), err)
	}
	if err := store.Security.DeleteLegacy(secIDs[0]); err != nil {
		t.Fatal(err)
	}
	left = store.LegacyLeftForTest(contract, l.topics, []string{legacyNode})
	if left["hint"] != 0 || left["security"] != 0 {
		t.Errorf("old hints %d and security records %d after they were deleted", left["hint"], left["security"])
	}
}

// TestMigrateFromV060 opens a store as v0.6.0 left it, its own records under
// fixed ids, sealed or not, and checks that they are moved under $sys topics
// and read from there, once each, and again after another restart.
func TestMigrateFromV060(t *testing.T) {
	for _, seal := range []bool{false, true} {
		t.Run(fmt.Sprintf("seal=%t", seal), func(t *testing.T) {
			dir := newStore(t, 0, ring(0), seal)
			l := writeLegacy(t, fmt.Sprintf("v060-%t", seal))
			closeStore(t)
			openStore(t, dir)
			l.check(t, seal)
			closeStore(t)
			openStore(t, dir)
			for _, topic := range l.topics {
				if msgs, _ := store.Message.GetAll(contract, topic, ""); len(msgs) != len(l.replicas[topic]) {
					t.Errorf("%s: %d replicas after another restart, want %d", topic, len(msgs), len(l.replicas[topic]))
				}
			}
		})
	}
}

// TestMigrateInterrupted stops the move of a v0.6.0 store, as a crash
// would, after its copies are written and before the old records are
// deleted, at each step; and checks that the next open moves the rest, with
// no record twice.
func TestMigrateInterrupted(t *testing.T) {
	for crashAt := 0; crashAt < 4; crashAt++ { // three topics' replicas, and the seen ids
		t.Run(fmt.Sprintf("crash=%d", crashAt), func(t *testing.T) {
			dir := newStore(t, 0, ring(0), true)
			l := writeLegacy(t, fmt.Sprintf("crash-%d", crashAt))
			closeStore(t)
			store.CrashMoveAfterForTest(crashAt)
			t.Cleanup(func() { store.CrashMoveAfterForTest(-1) })
			store.ForgetTopicsForTest()
			if err := store.Open(dir, storeConf, false); err == nil {
				t.Fatal("the move was not stopped")
			}
			closeStore(t)
			store.CrashMoveAfterForTest(-1)
			openStore(t, dir)
			l.check(t, true)
		})
	}
}

// TestNewStoreLayout checks that records written now are kept under $sys
// topics, and nothing under the old fixed ids.
func TestNewStoreLayout(t *testing.T) {
	newStore(t, 0, ring(0), false)
	r := newRecords(t, "new-layout-marker", 7)
	r.write(t)
	for what, at := range map[string]struct {
		contract uint32
		topic    string
	}{
		"replica":      {contract, store.SysTopicForTest("replica", r.topic+".replica")},
		"subscription": {contract, store.SysTopicForTest("sub", r.topic)},
		"hint":         {store.SysContract, store.HintTopicForTest("node-b")},
		"seen":         {store.SysContract, store.SysTopicForTest("seen", "seen")},
	} {
		raw, err := store.GetRawForTest(at.contract, at.topic)
		found := false
		for _, b := range raw {
			found = found || strings.Contains(string(b), r.marker)
		}
		if err != nil || !found {
			t.Errorf("%s is not under %s in contract %d: %v", what, at.topic, at.contract, err)
		}
	}
	index, err := store.GetForTest(store.SysContract, store.SysTopicForTest("index", "topics"))
	if err != nil || len(index) != 2 {
		t.Errorf("%d entries under $sys.index, want 2 (%v)", len(index), err)
	}
	left := store.LegacyLeftForTest(contract, []string{r.topic + ".replica"}, []string{"node-b"})
	for kind, n := range left {
		if n != 0 {
			t.Errorf("%d %s records under an old fixed id", n, kind)
		}
	}
	// A wildcard subscription matches below $sys.sub as it did at the top.
	id, _ := store.Subscription.NewID()
	if err := store.Subscription.Put(contract, id, "groups.wild.*", []byte("wild")); err != nil {
		t.Fatal(err)
	}
	id2, _ := store.Subscription.NewID()
	if err := store.Subscription.Put(contract, id2, "groups...", []byte("multi")); err != nil {
		t.Fatal(err)
	}
	// A first part of "*" is a wildcard too, below $sys.sub: up to v0.6.0
	// it matched a first part of "*" only.
	id3, _ := store.Subscription.NewID()
	if err := store.Subscription.Put(contract, id3, "*.wild.x", []byte("first")); err != nil {
		t.Fatal(err)
	}
	subs, err := store.Subscription.Get(contract, "groups.wild.x")
	if err != nil || len(subs) != 3 {
		t.Errorf("subscriptions of groups.wild.x: %q %v, want the three wildcard ones", subs, err)
	}
}

// TestReservedContracts checks that no contract is drawn as 0, which the
// node's own records are kept under, or as an id v0.6.0 kept them under.
func TestReservedContracts(t *testing.T) {
	for _, id := range append(store.LegacyStoreIDs(), store.SysContract, 3376684800) {
		if !uid.IsReservedContract(id) {
			t.Errorf("contract %d may be drawn", id)
		}
	}
}

// TestMigrateSharedNamespace opens a v0.6.0 store where contract B is
// contract A XOR the old replica id, so A's replicas and B's own messages
// of a topic were kept together: they can't be told apart, and are left
// where they are, B's messages B's.
func TestMigrateSharedNamespace(t *testing.T) {
	dir := newStore(t, 0, ring(0), true)
	a := uint32(0x5ea1ed10)
	b := store.LegacyReplicaContractForTest(a)
	topic := "groups.shared.namespace"
	if err := store.PutLegacyMessageForTest(b, topic, []byte("b-own")); err != nil {
		t.Fatal(err)
	}
	if err := store.PutLegacyReplicaForTest(a, topic, []byte("a-replica"), 0); err != nil {
		t.Fatal(err)
	}
	// A topic of A that shares nothing moves.
	if err := store.PutLegacyReplicaForTest(a, topic+".alone", []byte("a-alone"), 0); err != nil {
		t.Fatal(err)
	}
	closeStore(t)
	openStore(t, dir)
	own, err := store.Message.Get(b, topic, "")
	if err != nil || len(own) != 2 {
		t.Errorf("B's namespace holds %d messages, want its own and A's replica left there (%v)", len(own), err)
	}
	if raw, _ := store.GetRawForTest(a, store.SysTopicForTest("replica", topic)); len(raw) != 0 {
		t.Errorf("%d records of the shared namespace moved to A's replicas", len(raw))
	}
	if all, _ := store.Message.GetAll(a, topic+".alone", ""); len(all) != 1 || string(all[0].Payload) != "a-alone" {
		t.Errorf("A's other topic: %v", all)
	}
	if left := store.LegacyLeftForTest(a, nil, nil); left["index"] != 0 {
		t.Errorf("%d entries left in the old index", left["index"])
	}
}
