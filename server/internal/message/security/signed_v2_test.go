package security

import (
	"strings"
	"testing"
)

func TestKeyV2(t *testing.T) {
	s := NewSignerV2(3, []byte("signer-v2-test-key-0123456789abc"))
	lookup := func(id uint8) *SignerV2 {
		if id == 3 {
			return s
		}
		return nil
	}
	const contract, topic = 0x1234, "groups.v2.x.message"
	text, err := s.GenerateKey(contract, topic, AllowRead, 100, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(text) != KeyLenV2 || len(text) == V1KeyLen || len(text) == encodedLen {
		t.Fatalf("key length %d, want %d and unlike v1 keys", len(text), KeyLenV2)
	}
	// The key parses off its topic as v1 keys do.
	if p := ParseKey(text + "/" + topic + "?last=1h"); p.Key != text || p.Topic[:p.Size] != topic {
		t.Fatalf("parsed %q as key %q, topic %q", text+"/"+topic, p.Key, p.Topic[:p.Size])
	}
	if strings.ContainsAny(text, "/.?") {
		t.Fatalf("%q has a character topics are split on", text)
	}
	k, err := DecodeKeyV2(contract, text, topic, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if k.KeyID != 3 || !k.HasPermission(AllowRead) || k.HasPermission(AllowWrite) || k.Wildcard || k.IssuedAt != 100 || k.ExpiresAt != 200 || k.Uuid == 0 {
		t.Errorf("decoded %+v", k)
	}
	if !k.Expired(200) || k.Expired(199) || (KeyV2{}).Expired(1<<40) {
		t.Error("Expired")
	}
	if KeyUuidV2(text) != k.Uuid || KeyUuidV2("x") != 0 {
		t.Error("KeyUuidV2")
	}
	for name, check := range map[string]func() error{
		"another topic":    func() error { _, err := DecodeKeyV2(contract, text, "groups.v2.y.message", lookup); return err },
		"a longer topic":   func() error { _, err := DecodeKeyV2(contract, text, topic+".x", lookup); return err },
		"a shorter topic":  func() error { _, err := DecodeKeyV2(contract, text, "groups.v2.x", lookup); return err },
		"every topic":      func() error { _, err := DecodeKeyV2(contract, text, "...", lookup); return err },
		"another contract": func() error { _, err := DecodeKeyV2(contract+1, text, topic, lookup); return err },
		"no signer": func() error {
			_, err := DecodeKeyV2(contract, text, topic, func(uint8) *SignerV2 { return nil })
			return err
		},
		"another signer": func() error {
			o := NewSignerV2(3, []byte("another-signer-key-0123456789abc"))
			_, err := DecodeKeyV2(contract, text, topic, func(uint8) *SignerV2 { return o })
			return err
		},
	} {
		if check() != ErrInvalidSignature {
			t.Errorf("%s: accepted, or refused for the wrong reason", name)
		}
	}
	raw, _ := keyEncodingV2.DecodeString(text)
	for i := range raw {
		b := append([]byte(nil), raw...)
		b[i] ^= 0x01
		if _, err := DecodeKeyV2(contract, keyEncodingV2.EncodeToString(b), topic, lookup); err == nil {
			t.Fatalf("a key with byte %d changed was accepted", i)
		}
	}
	for pattern, wild := range map[string]bool{"a.b...": true, "a.*.c": true, "...": true, "*": true, "a.b": false} {
		text, _ := s.GenerateKey(contract, pattern, AllowRead, 1, 0)
		k, err := DecodeKeyV2(contract, text, pattern, lookup)
		if err != nil || k.Wildcard != wild {
			t.Errorf("%q: wildcard %t (%v), want %t", pattern, k.Wildcard, err, wild)
		}
	}
	if _, err := s.GenerateKey(contract, "a.b.c.d.e.f.g.h.i.j.k.l.m.n.o.p.q.r.s.t.u.v.w.x", AllowRead, 1, 0); err != ErrTargetTooLong {
		t.Errorf("a topic of 24 parts: %v, want ErrTargetTooLong", err)
	}
	if _, err := DecodeKeyV2(contract, "short", topic, lookup); err != ErrInvalidKey {
		t.Errorf("a short key: %v", err)
	}
}

// TestKeyV2AnyTopic checks that a v2 key for "..." opens every topic of its
// contract, as a v1 key for "..." does, as a wildcard's.
func TestKeyV2AnyTopic(t *testing.T) {
	s := NewSignerV2(0, []byte("signer-v2-test-key-0123456789abc"))
	lookup := func(uint8) *SignerV2 { return s }
	all, _ := s.GenerateKey(7, "...", AllowRead, 1, 0)
	for _, topic := range []string{"a", "a.b.c", "..."} {
		if k, err := DecodeKeyV2(7, all, topic, lookup); err != nil || !k.Wildcard {
			t.Errorf("a key for ... on %q: %+v %v", topic, k, err)
		}
	}
	if _, err := DecodeKeyV2(8, all, "a", lookup); err != ErrInvalidSignature {
		t.Error("a key for ... opened a topic of another contract")
	}
	// Other wildcard keys open their pattern only.
	ab, _ := s.GenerateKey(7, "a.b...", AllowRead, 1, 0)
	if _, err := DecodeKeyV2(7, ab, "a.b.c", lookup); err != ErrInvalidSignature {
		t.Error("a key for a.b... opened a.b.c")
	}
}

func FuzzDecodeKeyV2(f *testing.F) {
	s := NewSignerV2(0, []byte("signer-v2-test-key-0123456789abc"))
	text, _ := s.GenerateKey(1, "a.b", AllowRead, 1, 0)
	f.Add(text, "a.b")
	f.Fuzz(func(t *testing.T, text, topic string) {
		DecodeKeyV2(1, text, topic, func(uint8) *SignerV2 { return s })
	})
}
