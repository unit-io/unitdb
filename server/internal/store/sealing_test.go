package store_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/unit-io/unitdb/server/internal/config"
	_ "github.com/unit-io/unitdb/server/internal/db/unitdb"
	"github.com/unit-io/unitdb/server/internal/store"
)

const storeConf = `{"adapters": {"unitdb": {"mem_size": 1000000}}}`

// storeKey returns the store subkey of key id, as the server derives it.
func storeKey(id uint8) []byte {
	key := []byte(fmt.Sprintf("sealing-test-key-%02d-0123456789ab", id))
	return config.Key{ID: id, Key: key}.Subkey(config.SubkeyStore)
}

// ring returns the store subkeys of key ids.
func ring(ids ...uint8) map[uint8][]byte {
	m := make(map[uint8][]byte)
	for _, id := range ids {
		m[id] = storeKey(id)
	}
	return m
}

func setSealing(t *testing.T, issue uint8, keys map[uint8][]byte, seal bool) {
	t.Helper()
	if err := store.SetSealing(issue, keys, seal); err != nil {
		t.Fatal(err)
	}
}

// openStore opens the store in dir, as a restarted server does: the topic
// index is read from the store.
func openStore(t *testing.T, dir string) {
	t.Helper()
	store.ForgetTopicsForTest()
	if err := store.Open(dir, storeConf, false); err != nil {
		t.Fatal(err)
	}
}

func closeStore(t *testing.T) {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
}

// newStore opens a new store, closed when the test ends, with sealing set as
// given.
func newStore(t *testing.T, issue uint8, keys map[uint8][]byte, seal bool) string {
	t.Helper()
	dir := t.TempDir()
	setSealing(t, issue, keys, seal)
	openStore(t, dir)
	t.Cleanup(func() {
		if store.IsOpen() {
			store.Close()
		}
		// Off, for the next test.
		store.SetSealing(0, nil, false)
	})
	return dir
}

const contract uint32 = 0x5ea1ed00

// records is one record of each kind the server stores, holding marker.
type records struct {
	marker  string
	topic   string
	session uint64 // a session row's key
	logKey  uint64 // a session log entry's key
	hintID  []byte
	subID   []byte
}

func newRecords(t *testing.T, marker string, n uint64) *records {
	t.Helper()
	r := &records{marker: marker, topic: fmt.Sprintf("groups.sealing.t%d", n), session: 0x1000 + n, logKey: n<<32 + 0x1000 + n}
	var err error
	if r.hintID, err = store.Hint.NewID(); err != nil {
		t.Fatal(err)
	}
	if r.subID, err = store.Subscription.NewID(); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *records) row() []byte {
	b := make([]byte, 12+len(r.marker))
	binary.LittleEndian.PutUint32(b, uint32(r.session))
	copy(b[12:], r.marker)
	return b
}

func (r *records) write(t *testing.T) {
	t.Helper()
	m := []byte(r.marker)
	for name, err := range map[string]error{
		"message":      store.Message.Put(contract, r.topic, m, "1h"),
		"replica":      store.Message.PutReplica(contract, r.topic+".replica", m, 0),
		"hint":         store.Hint.Put("node-b", r.hintID, m, ""),
		"seen":         store.Seen.Put(r.marker, 0),
		"session":      store.Session.Put(r.session, r.row()),
		"subscription": store.Subscription.Put(contract, r.subID, r.topic, m),
	} {
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	store.Log.Apply(store.LogOp{Key: r.logKey, Raw: m})
}

// check checks that every record of r reads back, and returns what did not.
func (r *records) check(t *testing.T) {
	t.Helper()
	contains := func(list [][]byte) bool {
		for _, b := range list {
			if string(b) == r.marker {
				return true
			}
		}
		return false
	}
	msgs, err := store.Message.Get(contract, r.topic, "")
	if err != nil || len(msgs) != 1 || string(msgs[0].Payload) != r.marker {
		t.Errorf("%s: message: %v %v", r.marker, msgs, err)
	}
	all, err := store.Message.GetAll(contract, r.topic+".replica", "")
	if err != nil || len(all) != 1 || string(all[0].Payload) != r.marker {
		t.Errorf("%s: replica: %v %v", r.marker, all, err)
	}
	hist, err := store.Message.History(contract, r.topic)
	if err != nil || len(hist) != 1 || string(hist[0].Payload) != r.marker || !hist[0].Known {
		t.Errorf("%s: history: %+v %v", r.marker, hist, err)
	}
	if hints, err := store.Hint.Get("node-b"); err != nil || !contains(hints) {
		t.Errorf("%s: hint: %v %v", r.marker, len(hints), err)
	}
	seen, err := store.Seen.Recent(1000)
	found := false
	for _, id := range seen {
		found = found || id == r.marker
	}
	if err != nil || !found {
		t.Errorf("%s: seen: %v %v", r.marker, seen, err)
	}
	if row, err := store.Session.Get(r.session); err != nil || !bytes.Equal(row, r.row()) {
		t.Errorf("%s: session: %q %v", r.marker, row, err)
	}
	if raw := store.Log.Raw(r.logKey); string(raw) != r.marker {
		t.Errorf("%s: log: %q", r.marker, raw)
	}
	if subs, err := store.Subscription.Get(contract, r.topic); err != nil || !contains(subs) {
		t.Errorf("%s: subscription: %v %v", r.marker, len(subs), err)
	}
}

// dirContains reports whether a file under dir holds needle.
func dirContains(t *testing.T, dir string, needle []byte) bool {
	t.Helper()
	found := false
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() || found {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		found = bytes.Contains(b, needle)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// sealedWith returns the key id the raw record b is sealed with, or -1 if it
// is not sealed.
func sealedWith(b []byte) int {
	if len(b) < store.SealOverhead || !bytes.Equal(b[:4], []byte{0xE5, 0x7A, 0x1C, 0x5E}) {
		return -1
	}
	return int(b[4])
}

// TestSealingRoundTrip writes each kind of record sealed, reads it back,
// before and after a restart, and checks it is sealed on disk.
func TestSealingRoundTrip(t *testing.T) {
	dir := newStore(t, 0, ring(0), true)
	r := newRecords(t, "sealing-round-trip-marker-payload", 1)
	r.write(t)
	r.check(t)
	for name, get := range map[string]func() ([]byte, error){
		"session": func() ([]byte, error) { return store.GetMessageRawForTest(r.session) },
		"log":     func() ([]byte, error) { return store.GetMessageRawForTest(r.logKey) },
		"message": func() ([]byte, error) {
			raw, err := store.GetRawForTest(contract, r.topic)
			if len(raw) != 1 {
				return nil, fmt.Errorf("%d records", len(raw))
			}
			return raw[0], err
		},
	} {
		b, err := get()
		if err != nil || sealedWith(b) != 0 {
			t.Errorf("%s on disk: sealed with %d, %v; want key 0", name, sealedWith(b), err)
		}
	}
	closeStore(t)
	openStore(t, dir)
	r.check(t)
	if topics := store.Message.Topics(); len(topics) != 2 {
		t.Errorf("topic index after a restart: %v, want the topic and its replica", topics)
	}
}

// TestSealingOnDisk checks that a payload stored with sealing on is nowhere
// in the store's files, and, as a control, that one stored with it off is.
func TestSealingOnDisk(t *testing.T) {
	for _, seal := range []bool{false, true} {
		t.Run(fmt.Sprintf("seal=%t", seal), func(t *testing.T) {
			dir := newStore(t, 0, ring(0), seal)
			marker := fmt.Sprintf("sealing-on-disk-marker-%t-payload", seal)
			newRecords(t, marker, 2).write(t)
			closeStore(t)
			if found := dirContains(t, dir, []byte(marker)); found == seal {
				t.Fatalf("the payload is in the store's files: %t, with sealing %t", found, seal)
			}
		})
	}
}

// TestSealingTurnedOn turns sealing on for a store that holds plain records,
// and then off again: every record reads throughout, the topic index too.
func TestSealingTurnedOn(t *testing.T) {
	dir := newStore(t, 0, ring(0), false)
	plain := newRecords(t, "sealing-turned-on-plain-marker", 3)
	plain.write(t)
	closeStore(t)

	setSealing(t, 0, ring(0), true)
	openStore(t, dir)
	plain.check(t)
	sealed := newRecords(t, "sealing-turned-on-sealed-marker", 4)
	sealed.write(t)
	plain.check(t)
	sealed.check(t)
	closeStore(t)

	wantTopics := func(when string) {
		t.Helper()
		got := make(map[string]bool)
		for _, ref := range store.Message.Topics() {
			if ref.Contract != contract {
				t.Errorf("%s: a topic of contract %x in the index: %+v", when, ref.Contract, ref)
			}
			got[ref.Topic] = true
		}
		for _, r := range []*records{plain, sealed} {
			if !got[r.topic] || !got[r.topic+".replica"] {
				t.Errorf("%s: %s is not in the topic index: %v", when, r.topic, got)
			}
		}
		if len(got) != 4 {
			t.Errorf("%s: topic index %v, want 4 topics", when, got)
		}
	}
	openStore(t, dir)
	plain.check(t)
	sealed.check(t)
	wantTopics("on, after a restart")
	closeStore(t)

	// Off again: sealed records still open, new ones are plain.
	setSealing(t, 0, ring(0), false)
	openStore(t, dir)
	plain.check(t)
	sealed.check(t)
	wantTopics("off, after a restart")
	after := newRecords(t, "sealing-turned-off-plain-marker", 5)
	after.write(t)
	after.check(t)
	if b, _ := store.GetMessageRawForTest(after.session); sealedWith(b) != -1 {
		t.Error("a record written with sealing off was sealed")
	}
}

// TestSealingRotation seals records with key 0, rotates to key 1 keeping 0
// to read with, then removes key 0: its records are refused with an error
// naming the reason, while plain records and key 1's still read.
func TestSealingRotation(t *testing.T) {
	newStore(t, 0, ring(0), false)
	plain := newRecords(t, "sealing-rotation-plain-marker", 6)
	plain.write(t)
	setSealing(t, 0, ring(0), true)
	old := newRecords(t, "sealing-rotation-key-0-marker", 7)
	old.write(t)

	setSealing(t, 1, ring(0, 1), true)
	current := newRecords(t, "sealing-rotation-key-1-marker", 8)
	current.write(t)
	for _, r := range []*records{plain, old, current} {
		r.check(t)
	}
	for r, want := range map[*records]int{plain: -1, old: 0, current: 1} {
		if b, _ := store.GetMessageRawForTest(r.session); sealedWith(b) != want {
			t.Errorf("%s: sealed with %d, want %d", r.marker, sealedWith(b), want)
		}
	}

	// Key 0 removed.
	setSealing(t, 1, ring(1), true)
	plain.check(t)
	current.check(t)
	if row, err := store.Session.Get(old.session); !errors.Is(err, store.ErrUnknownSealKey) || row != nil {
		t.Errorf("a session row of a removed key: %q, %v; want %v", row, err, store.ErrUnknownSealKey)
	}
	if raw := store.Log.Raw(old.logKey); raw != nil {
		t.Errorf("a log entry of a removed key read as %q", raw)
	}
	if msgs, err := store.Message.Get(contract, old.topic, ""); err != nil || len(msgs) != 0 {
		t.Errorf("messages of a removed key: %v, %v; want them skipped", msgs, err)
	}
	if subs, err := store.Subscription.Get(contract, old.topic); err != nil || len(subs) != 0 {
		t.Errorf("subscriptions of a removed key: %d, %v; want them skipped", len(subs), err)
	}
}

// TestSealingTampered checks that a sealed record changed on disk, or moved
// to another key or contract, is refused rather than read.
func TestSealingTampered(t *testing.T) {
	newStore(t, 0, ring(0), true)
	r := newRecords(t, "sealing-tampered-marker", 9)
	r.write(t)
	b, err := store.GetMessageRawForTest(r.session)
	if err != nil {
		t.Fatal(err)
	}
	flipped := append([]byte(nil), b...)
	flipped[len(flipped)-20] ^= 1
	store.PutMessageRawForTest(0x77, flipped)
	store.PutMessageRawForTest(0x78, b)
	for key, what := range map[uint64]string{0x77: "changed", 0x78: "moved to another key"} {
		if got, err := store.Session.Get(key); !errors.Is(err, store.ErrSealBroken) || got != nil {
			t.Errorf("a record %s: %q, %v; want %v", what, got, err, store.ErrSealBroken)
		}
	}
	raw, err := store.GetRawForTest(contract, r.topic)
	if err != nil || len(raw) != 1 {
		t.Fatal(raw, err)
	}
	if err := store.PutRawForTest(contract+1, r.topic, raw[0]); err != nil {
		t.Fatal(err)
	}
	if msgs, err := store.Message.Get(contract+1, r.topic, ""); err != nil || len(msgs) != 0 {
		t.Errorf("a message moved to another contract: %v, %v; want it skipped", msgs, err)
	}
}

// TestSetSealingRefuses checks that sealing needs the issue key.
func TestSetSealingRefuses(t *testing.T) {
	if err := store.SetSealing(1, ring(0), true); err == nil {
		t.Error("sealing without the issue key's subkey was set")
	}
	if err := store.SetSealing(0, map[uint8][]byte{0: []byte("short")}, false); err == nil {
		t.Error("a short key was taken")
	}
}
