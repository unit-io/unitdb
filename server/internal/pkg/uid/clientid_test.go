package uid

import (
	"fmt"
	"testing"

	"github.com/unit-io/unitdb/server/internal/pkg/crypto"
	"github.com/unit-io/unitdb/server/internal/v1test"
)

var testKey = []byte("test-only-key-do-not-use-0000000")

func newMAC(t *testing.T) *crypto.MAC {
	t.Helper()
	mac, err := crypto.New(testKey)
	if err != nil {
		t.Fatal(err)
	}
	return mac
}

func TestClientIDEncodeDecodeV1(t *testing.T) {
	mac := newMAC(t)
	id, err := NewClientID(1)
	if err != nil {
		t.Fatal(err)
	}
	if !id.IsPrimary() {
		t.Fatal("new client id must be primary")
	}

	encoded := v1test.ClientID(id, testKey)
	if len(encoded) != 52 {
		t.Fatalf("encoded length %d, want 52", len(encoded))
	}

	// Decode works in place, so hand it a copy.
	decoded, err := DecodeV1([]byte(encoded), mac)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Epoch() != id.Epoch() || decoded.Contract() != id.Contract() || decoded.Permissions() != id.Permissions() {
		t.Fatalf("decoded %v, want %v", decoded, id)
	}
}

func TestClientIDDecodeInvalid(t *testing.T) {
	mac := newMAC(t)
	if _, err := DecodeV1([]byte("too-short"), mac); err == nil {
		t.Fatal("expected error for short client id")
	}

	id, _ := NewClientID(1)
	encoded := []byte(v1test.ClientID(id, testKey))
	encoded[20] ^= 0x01
	if _, err := DecodeV1(encoded, mac); err == nil {
		t.Fatal("expected error for tampered client id")
	}

	other, err := crypto.New([]byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DecodeV1([]byte(v1test.ClientID(id, testKey)), other); err == nil {
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

func TestServicePermission(t *testing.T) {
	mac := newMAC(t)
	service, err := MintClientID(0x5e41ce01, true)
	if err != nil {
		t.Fatal(err)
	}
	back, err := DecodeV1([]byte(v1test.ClientID(service, testKey)), mac)
	if err != nil {
		t.Fatal(err)
	}
	if !back.IsService() || !back.IsPrimary() || back.Contract() != 0x5e41ce01 {
		t.Fatalf("service id decoded with permissions %d, contract %x", back.Permissions(), back.Contract())
	}

	primary, err := MintClientID(0, false)
	if err != nil {
		t.Fatal(err)
	}
	if primary.IsService() || !primary.IsPrimary() || primary.Contract() == 0 {
		t.Fatalf("primary id with permissions %d, contract %x", primary.Permissions(), primary.Contract())
	}
	// Ids the server issues are never a service's.
	secondary, _ := NewSecondaryClientID(service)
	cached, _ := CachedClientID(service.Contract())
	for _, id := range []ID{secondary, cached} {
		if id.IsService() || id.IsPrimary() {
			t.Fatalf("an id the server issues has permissions %d", id.Permissions())
		}
	}
	if (ID(make([]byte, rawLen))).HasPermission(AllowNone) {
		t.Fatal("HasPermission(AllowNone) is true")
	}
}

// TestRandomIDs checks that contracts and unique numbers come from
// crypto/rand: NewUnique was math/rand seeded with the time in seconds, so
// calls in the same second returned the same number.
func TestRandomIDs(t *testing.T) {
	uniques := make(map[uint32]bool)
	contracts := make(map[uint32]bool)
	for i := 0; i < 1000; i++ {
		uniques[NewUnique()] = true
		c, err := NewContract()
		if err != nil {
			t.Fatal(err)
		}
		if c == 0 {
			t.Fatal("contract 0")
		}
		contracts[c] = true
	}
	// 1000 draws of 32 bits repeat one with a chance of about 1 in 10^4.
	if len(uniques) < 999 || len(contracts) < 999 {
		t.Fatalf("%d distinct unique numbers and %d distinct contracts of 1000", len(uniques), len(contracts))
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

// TestClientIDEncodingIsStable checks that a v1 client ID, as v0.3 sealed
// it, still opens: mintid -from seals such ids again as v2 ones. The
// expected values were produced by v0.3's code.
func TestClientIDEncodingIsStable(t *testing.T) {
	mac, err := crypto.New([]byte("test-only-key-do-not-use-0000000"))
	if err != nil {
		t.Fatal(err)
	}
	id := ID(make([]byte, rawLen))
	id.SetEpoch(0x01020304)
	id.SetPrimary(0xbeef)
	id.SetPermissions(AllowMaster)
	id.SetContract(0x6e7c0de0)
	if raw := fmt.Sprintf("%x", []byte(id)); raw != "0102030400beef016e7c0de0" {
		t.Fatalf("raw ID %s", raw)
	}
	const encoded = "AEBAEBUTQJGYWRbOFeTVTSIZIMcfPGQGQQSbaLAHEeOOCUFXHUPQ"
	back, err := DecodeV1([]byte(encoded), mac)
	if err != nil {
		t.Fatal(err)
	}
	if back.Primary() != 0xbeef || back.Contract() != 0x6e7c0de0 || back.Epoch() != 0x01020304 || !back.IsPrimary() {
		t.Fatalf("decoded %x", []byte(back))
	}
}
