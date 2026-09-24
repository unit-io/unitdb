package uid

import (
	"testing"

	"github.com/unit-io/unitdb/server/internal/pkg/crypto"
)

func newMAC(t *testing.T) *crypto.MAC {
	t.Helper()
	mac, err := crypto.New([]byte("4BWm1vZletvrCDGWsF6mex8oBSd59m6I"))
	if err != nil {
		t.Fatal(err)
	}
	return mac
}

func TestClientIDEncodeDecode(t *testing.T) {
	mac := newMAC(t)
	id, err := NewClientID(1)
	if err != nil {
		t.Fatal(err)
	}
	if !id.IsPrimary() {
		t.Fatal("new client id must be primary")
	}

	encoded := id.Encode(mac)
	if len(encoded) != 52 {
		t.Fatalf("encoded length %d, want 52", len(encoded))
	}

	// Decode works in place, so hand it a copy.
	decoded, err := Decode([]byte(encoded), mac)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Epoch() != id.Epoch() || decoded.Contract() != id.Contract() || decoded.Permissions() != id.Permissions() {
		t.Fatalf("decoded %v, want %v", decoded, id)
	}
}

func TestClientIDDecodeInvalid(t *testing.T) {
	mac := newMAC(t)
	if _, err := Decode([]byte("too-short"), mac); err == nil {
		t.Fatal("expected error for short client id")
	}

	id, _ := NewClientID(1)
	encoded := []byte(id.Encode(mac))
	encoded[20] ^= 0x01
	if _, err := Decode(encoded, mac); err == nil {
		t.Fatal("expected error for tampered client id")
	}

	other, err := crypto.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode([]byte(id.Encode(mac)), other); err == nil {
		t.Fatal("expected error for client id encoded with another key")
	}
}

func TestSecondaryClientID(t *testing.T) {
	primary, _ := NewClientID(1)
	secondary, err := NewSecondaryClientID(primary)
	if err != nil {
		t.Fatal(err)
	}
	if secondary.IsPrimary() {
		t.Fatal("secondary client id must not be primary")
	}
	if secondary.Contract() != primary.Contract() {
		t.Fatalf("contract %d, want %d", secondary.Contract(), primary.Contract())
	}

	cached, err := CachedClientID(primary.Contract())
	if err != nil {
		t.Fatal(err)
	}
	if cached.IsPrimary() || cached.Contract() != primary.Contract() {
		t.Fatalf("unexpected cached client id %v", cached)
	}
}

func TestClientIDFields(t *testing.T) {
	id := ID(make([]byte, rawLen))
	id.SetEpoch(0xDEADBEEF)
	id.SetContract(0x01020304)
	id.SetPermissions(AllowMaster)
	if id.Epoch() != 0xDEADBEEF {
		t.Fatalf("epoch %x", id.Epoch())
	}
	if id.Contract() != 0x01020304 {
		t.Fatalf("contract %x", id.Contract())
	}
	if !id.IsPrimary() {
		t.Fatal("expected primary")
	}
}

func TestNewLIDIsUnique(t *testing.T) {
	seen := make(map[LID]bool)
	for i := 0; i < 1000; i++ {
		id := NewLID()
		if seen[id] {
			t.Fatalf("duplicate LID %d", id)
		}
		seen[id] = true
	}
}
