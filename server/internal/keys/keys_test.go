package keys

import (
	"bytes"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/config"
	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
)

var (
	oldKey = []byte("keys-test-old-key-0123456789abcd")
	newKey = []byte("keys-test-new-key-0123456789abcd")
)

func set(t *testing.T, keys ...config.Key) *Set {
	t.Helper()
	s, err := New(&config.Keyring{Keys: keys})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// TestRotation checks a key rotation: a new issue key, the old one kept to
// read with, then removed.
func TestRotation(t *testing.T) {
	before := set(t, config.Key{ID: 0, Key: oldKey, Use: config.KeyIssue})
	during := set(t, config.Key{ID: 1, Key: newKey, Use: config.KeyIssue}, config.Key{ID: 0, Key: oldKey, Use: config.KeyRead})
	after := set(t, config.Key{ID: 1, Key: newKey, Use: config.KeyIssue})

	id, _ := uid.NewSecondaryClientID(uid.ID(make([]byte, 12)))
	v1 := before.EncodeClientIDV1(id)
	v2Old, _ := before.SealClientID(id, 0)
	v2New, _ := during.SealClientID(id, time.Hour)
	const contract, topic = 0x7e57, "rotation.t"
	keyV1, _ := before.TopicKeyV1(contract, topic, security.AllowRead)
	keyOld, _ := before.TopicKey(contract, topic, security.AllowRead, 0)
	keyNew, _ := during.TopicKey(contract, topic, security.AllowRead, 0)

	opens := func(s *Set, text string) (*uid.Claims, bool) {
		got, claims, err := s.OpenClientID([]byte(text))
		if err != nil {
			return nil, false
		}
		n := len(got)
		if !bytes.Equal(got, id[:n]) {
			t.Fatalf("%q opened to %x, want %x", text, []byte(got), []byte(id))
		}
		return claims, true
	}
	signs := func(s *Set, text string) bool {
		if len(text) == security.SignedKeyLen {
			_, err := s.DecodeTopicKeyV1(contract, text)
			return err == nil
		}
		_, err := s.DecodeTopicKeyV2(contract, text, topic)
		return err == nil
	}

	for name, tc := range map[string]struct {
		s                *Set
		v1, v2Old, v2New bool
		kV1, kOld, kNew  bool
	}{
		"before": {before, true, true, false, true, true, false},
		"during": {during, true, true, true, true, true, true},
		"after":  {after, false, false, true, false, false, true},
	} {
		if _, ok := opens(tc.s, v1); ok != tc.v1 {
			t.Errorf("%s: v1 id opens %t, want %t", name, ok, tc.v1)
		}
		if _, ok := opens(tc.s, v2Old); ok != tc.v2Old {
			t.Errorf("%s: v2 id of the old key opens %t, want %t", name, ok, tc.v2Old)
		}
		if _, ok := opens(tc.s, v2New); ok != tc.v2New {
			t.Errorf("%s: v2 id of the new key opens %t, want %t", name, ok, tc.v2New)
		}
		if ok := signs(tc.s, keyV1); ok != tc.kV1 {
			t.Errorf("%s: v1 topic key %t, want %t", name, ok, tc.kV1)
		}
		if ok := signs(tc.s, keyOld); ok != tc.kOld {
			t.Errorf("%s: v2 topic key of the old key %t, want %t", name, ok, tc.kOld)
		}
		if ok := signs(tc.s, keyNew); ok != tc.kNew {
			t.Errorf("%s: v2 topic key of the new key %t, want %t", name, ok, tc.kNew)
		}
	}

	// What the rotated set issues names the new key.
	if claims, _ := opens(during, v2New); claims == nil || claims.KeyID != 1 || claims.ExpiresAt == 0 {
		t.Errorf("issued during the rotation: %+v", claims)
	}
	if claims, _ := opens(during, v2Old); claims == nil || claims.KeyID != 0 {
		t.Errorf("old v2 id during the rotation: %+v", claims)
	}
	// v1 ids and keys are issued with the issue key.
	if _, ok := opens(after, during.EncodeClientIDV1(id)); !ok {
		t.Error("a v1 id issued during the rotation does not open with the new key")
	}
	k, _ := during.TopicKeyV1(contract, topic, security.AllowRead)
	if !signs(after, k) {
		t.Error("a v1 key issued during the rotation is not signed with the new key")
	}
	if _, _, err := after.OpenClientID([]byte("not an id")); err == nil {
		t.Error("garbage opened")
	}
}

// TestV1IDsStillOpen checks that a v1 id issued by v0.3's code, with the
// single key as encryption_config held it, opens with that key in a
// keyring, issue or read key.
func TestV1IDsStillOpen(t *testing.T) {
	legacy := []byte("test-only-key-do-not-use-0000000")
	const v1 = "AEBAEBUTQJGYWRbOFeTVTSIZIMcfPGQGQQSbaLAHEeOOCUFXHUPQ"
	for name, s := range map[string]*Set{
		"single key": set(t, config.Key{ID: 0, Key: legacy, Use: config.KeyIssue}),
		"read key":   set(t, config.Key{ID: 5, Key: newKey, Use: config.KeyIssue}, config.Key{ID: 0, Key: legacy, Use: config.KeyRead}),
	} {
		id, claims, err := s.OpenClientID([]byte(v1))
		if err != nil || claims != nil || id.Contract() != 0x6e7c0de0 || !id.IsPrimary() || id.Uuid() != 0 {
			t.Errorf("%s: %x %+v %v", name, []byte(id), claims, err)
		}
	}
}
