package uid

import (
	"bytes"
	"strings"
	"testing"
)

func testSealers(t *testing.T) (map[uint8]*Sealer, func(uint8) *Sealer) {
	t.Helper()
	sealers := map[uint8]*Sealer{}
	for id, key := range map[uint8]string{0: "sealer-test-key-0-0123456789abcd", 7: "sealer-test-key-7-0123456789abcd"} {
		s, err := NewSealer(id, []byte(key))
		if err != nil {
			t.Fatal(err)
		}
		sealers[id] = s
	}
	return sealers, func(id uint8) *Sealer { return sealers[id] }
}

func TestClientIDV2RoundTrip(t *testing.T) {
	sealers, lookup := testSealers(t)
	id, err := MintClientID(0x5e41ce01, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(id) != idLenV2 || id.Uuid() == 0 {
		t.Fatalf("a new id has no uuid: %x", []byte(id))
	}
	text, err := sealers[7].Seal(id, 1000, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if len(text) != EncodedLenV2 || !IsV2([]byte(text)) || len(text) == EncodedLenV1 {
		t.Fatalf("length %d, want %d", len(text), EncodedLenV2)
	}
	// base64url: nothing that splits a topic, its key or its options.
	if strings.ContainsAny(text, "/.?&=+") {
		t.Fatalf("%q has a character a topic or path parser splits on", text)
	}
	got, claims, err := OpenV2([]byte(text), lookup)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, id) || claims != (Claims{KeyID: 7, IssuedAt: 1000, ExpiresAt: 2000}) {
		t.Errorf("opened %x %+v, want %x with key 7, 1000, 2000", got, claims, id)
	}
	if !got.IsService() || !got.IsPrimary() || got.Contract() != 0x5e41ce01 {
		t.Errorf("opened permissions %d, contract %x", got.Permissions(), got.Contract())
	}
	again, _ := sealers[7].Seal(id, 1000, 2000)
	if again == text {
		t.Error("sealing the same id twice gave the same text: the nonce is not random")
	}
	if !claims.Expired(2000) || claims.Expired(1999) || (Claims{}).Expired(1<<40) {
		t.Error("Expired")
	}
}

// TestClientIDV2FromV1 checks that a v1 id, without a uuid, is sealed as v2
// and opens to the same 12 bytes.
func TestClientIDV2FromV1(t *testing.T) {
	sealers, lookup := testSealers(t)
	mac := newMAC(t)
	id, _ := NewSecondaryClientID(ID(make([]byte, rawLen)))
	v1, err := Decode([]byte(id.Encode(mac)), mac)
	if err != nil {
		t.Fatal(err)
	}
	if len(v1) != rawLen || v1.Uuid() != 0 {
		t.Fatalf("a v1 id opened to %x", []byte(v1))
	}
	text, err := sealers[0].Seal(v1, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	got, _, err := OpenV2([]byte(text), lookup)
	if err != nil || !bytes.Equal(got, v1) {
		t.Fatalf("opened %x (%v), want %x", []byte(got), err, []byte(v1))
	}
}

// TestSecondaryIDsDiffer checks that secondary ids of one contract issued in
// the same second differ, by their uuids: v1 ones are the same.
func TestSecondaryIDsDiffer(t *testing.T) {
	primary, _ := NewClientID(1)
	a, _ := NewSecondaryClientID(primary)
	b, _ := NewSecondaryClientID(primary)
	if bytes.Equal(a, b) || a.Uuid() == b.Uuid() {
		t.Fatal("two secondary ids share a uuid")
	}
}

func TestClientIDV2Tamper(t *testing.T) {
	sealers, lookup := testSealers(t)
	id, _ := NewClientID(1)
	text, _ := sealers[0].Seal(id, 1, 0)
	buf, _ := idEncoding.DecodeString(text)
	for i := range buf {
		b := append([]byte(nil), buf...)
		b[i] ^= 0x01
		if _, _, err := OpenV2([]byte(idEncoding.EncodeToString(b)), lookup); err == nil {
			t.Fatalf("an id with byte %d changed was opened", i)
		}
	}
	// Another key id, with the same bytes otherwise.
	b := append([]byte(nil), buf...)
	b[1] = 7
	if _, _, err := OpenV2([]byte(idEncoding.EncodeToString(b)), lookup); err == nil {
		t.Fatal("an id relabelled with another key id was opened")
	}
	if _, _, err := OpenV2([]byte(text), func(uint8) *Sealer { return nil }); err == nil {
		t.Fatal("an id was opened without its key")
	}
	other, _ := NewSealer(0, []byte("another-key-for-the-same-key-id!"))
	if _, _, err := OpenV2([]byte(text), func(uint8) *Sealer { return other }); err == nil {
		t.Fatal("an id was opened with another key")
	}
	// OpenV2 leaves its input alone.
	in := []byte(text)
	OpenV2(in, lookup)
	if string(in) != text {
		t.Fatal("OpenV2 changed its input")
	}
}

func FuzzOpenV2(f *testing.F) {
	sealers := map[uint8]*Sealer{}
	s, _ := NewSealer(0, []byte("sealer-test-key-0-0123456789abcd"))
	sealers[0] = s
	id, _ := NewClientID(1)
	text, _ := s.Seal(id, 1, 2)
	f.Add([]byte(text))
	f.Add(make([]byte, EncodedLenV2))
	f.Fuzz(func(t *testing.T, b []byte) {
		OpenV2(b, func(k uint8) *Sealer { return sealers[k] })
	})
}
