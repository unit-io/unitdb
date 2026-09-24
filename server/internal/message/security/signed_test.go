package security

import (
	"strings"
	"testing"
)

func TestSignedKey(t *testing.T) {
	s := NewSigner([]byte("4BWm1vZletvrCDGWsF6mex8oBSd59m6I"))
	key, err := s.GenerateKey(testContract, "teams.alpha", AllowRead)
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != SignedKeyLen {
		t.Fatalf("key length %d, want %d", len(key), SignedKeyLen)
	}
	if strings.ContainsAny(key, "/?.") {
		t.Fatalf("key %q contains a topic separator", key)
	}

	k, err := s.DecodeKey(testContract, key)
	if err != nil {
		t.Fatal(err)
	}
	if !k.HasPermission(AllowRead) || k.HasPermission(AllowWrite) {
		t.Fatalf("permissions %d", k.Permissions())
	}
	if ok, _ := k.ValidateTopic(testContract, "teams.alpha"); !ok {
		t.Fatal("signed key does not validate its topic")
	}
}

func TestSignedKeyRejectsTampering(t *testing.T) {
	s := NewSigner([]byte("4BWm1vZletvrCDGWsF6mex8oBSd59m6I"))
	key, _ := s.GenerateKey(testContract, "teams.alpha", AllowRead)

	// Upgrade the permission byte and re-encode, keeping the old tag.
	buf, _ := keyEncoding.DecodeString(key)
	buf[0] = byte(AllowReadWrite)
	if _, err := s.DecodeKey(testContract, keyEncoding.EncodeToString(buf)); err != ErrInvalidSignature {
		t.Fatalf("edited permissions: err = %v, want %v", err, ErrInvalidSignature)
	}

	// Every single character change is rejected.
	for i := range key {
		b := []byte(key)
		if b[i] == 'A' {
			b[i] = 'B'
		} else {
			b[i] = 'A'
		}
		if _, err := s.DecodeKey(testContract, string(b)); err == nil {
			t.Fatalf("key with character %d changed was accepted", i)
		}
	}

	// A key is only valid for the contract it was issued for.
	if _, err := s.DecodeKey(testContract+1, key); err != ErrInvalidSignature {
		t.Fatalf("other contract: err = %v, want %v", err, ErrInvalidSignature)
	}

	// A key signed with another secret is rejected.
	other, _ := NewSigner([]byte("another secret")).GenerateKey(testContract, "teams.alpha", AllowRead)
	if _, err := s.DecodeKey(testContract, other); err != ErrInvalidSignature {
		t.Fatalf("other secret: err = %v, want %v", err, ErrInvalidSignature)
	}

	// Unsigned keys and garbage are not signed keys.
	unsigned, _ := GenerateKey(testContract, "teams.alpha", AllowRead)
	for _, k := range []string{unsigned, "", strings.Repeat("a", SignedKeyLen), strings.Repeat("1", SignedKeyLen)} {
		if _, err := s.DecodeKey(testContract, k); err == nil {
			t.Fatalf("DecodeKey(%q) succeeded", k)
		}
	}
}

func TestSignedKeyTargetTooLong(t *testing.T) {
	s := NewSigner([]byte("secret"))
	topic := strings.TrimSuffix(strings.Repeat("a.", 24), ".")
	if _, err := s.GenerateKey(testContract, topic, AllowRead); err != ErrTargetTooLong {
		t.Fatalf("err = %v, want %v", err, ErrTargetTooLong)
	}
}
