package keys

import (
	"bytes"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/config"
	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/v1test"
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
	v2Old, _ := before.SealClientID(id, 0)
	v2New, _ := during.SealClientID(id, time.Hour)
	const contract, topic = 0x7e57, "rotation.t"
	keyOld, _ := before.TopicKey(contract, topic, security.AllowRead, 0)
	keyNew, _ := during.TopicKey(contract, topic, security.AllowRead, 0)

	opens := func(s *Set, text string) (uid.Claims, bool) {
		got, claims, err := s.OpenClientID([]byte(text))
		if err != nil {
			return uid.Claims{}, false
		}
		if !bytes.Equal(got, id) {
			t.Fatalf("%q opened to %x, want %x", text, []byte(got), []byte(id))
		}
		return claims, true
	}
	signs := func(s *Set, text string) bool {
		_, err := s.DecodeTopicKeyV2(contract, text, topic)
		return err == nil
	}

	for name, tc := range map[string]struct {
		s            *Set
		v2Old, v2New bool
		kOld, kNew   bool
	}{
		"before": {before, true, false, true, false},
		"during": {during, true, true, true, true},
		"after":  {after, false, true, false, true},
	} {
		if _, ok := opens(tc.s, v2Old); ok != tc.v2Old {
			t.Errorf("%s: v2 id of the old key opens %t, want %t", name, ok, tc.v2Old)
		}
		if _, ok := opens(tc.s, v2New); ok != tc.v2New {
			t.Errorf("%s: v2 id of the new key opens %t, want %t", name, ok, tc.v2New)
		}
		if ok := signs(tc.s, keyOld); ok != tc.kOld {
			t.Errorf("%s: v2 topic key of the old key %t, want %t", name, ok, tc.kOld)
		}
		if ok := signs(tc.s, keyNew); ok != tc.kNew {
			t.Errorf("%s: v2 topic key of the new key %t, want %t", name, ok, tc.kNew)
		}
	}

	// What the rotated set issues names the new key.
	if claims, ok := opens(during, v2New); !ok || claims.KeyID != 1 || claims.ExpiresAt == 0 {
		t.Errorf("issued during the rotation: %+v", claims)
	}
	if claims, ok := opens(during, v2Old); !ok || claims.KeyID != 0 {
		t.Errorf("old v2 id during the rotation: %+v", claims)
	}
	if _, _, err := after.OpenClientID([]byte("not an id")); err == nil {
		t.Error("garbage opened")
	}
}

// TestV1Refused checks that v1 client ids and topic keys, and unsigned
// keys, don't open, whatever key sealed or signed them: the server refuses
// them since v0.7.0.
func TestV1Refused(t *testing.T) {
	s := set(t, config.Key{ID: 0, Key: oldKey, Use: config.KeyIssue})
	id, _ := uid.NewSecondaryClientID(uid.ID(make([]byte, 12)))
	if _, _, err := s.OpenClientID([]byte(v1test.ClientID(id, oldKey))); err != ErrV1ClientID {
		t.Errorf("a v1 id: %v, want %v", err, ErrV1ClientID)
	}
	const contract, topic = 0x7e57, "v1.t"
	for name, k := range map[string]string{
		"v1 signed key": v1test.SignedTopicKey(oldKey, contract, topic, security.AllowRead),
		"unsigned key":  v1test.UnsignedTopicKey(contract, topic, security.AllowRead),
	} {
		if _, err := s.DecodeTopicKeyV2(contract, k, topic); err != security.ErrInvalidKey {
			t.Errorf("a %s: %v, want %v", name, err, security.ErrInvalidKey)
		}
	}
}

// TestV1IDsStillOpenForMintID checks that a v1 id issued by v0.3's code,
// with the single key as encryption_config held it, opens with that key in a
// keyring, issue or read key, for mintid -from to seal it again as v2.
func TestV1IDsStillOpenForMintID(t *testing.T) {
	legacy := []byte("test-only-key-do-not-use-0000000")
	const v1 = "AEBAEBUTQJGYWRbOFeTVTSIZIMcfPGQGQQSbaLAHEeOOCUFXHUPQ"
	for name, kr := range map[string]*config.Keyring{
		"single key": {Keys: []config.Key{{ID: 0, Key: legacy, Use: config.KeyIssue}}},
		"read key":   {Keys: []config.Key{{ID: 5, Key: newKey, Use: config.KeyIssue}, {ID: 0, Key: legacy, Use: config.KeyRead}}},
	} {
		id, err := OpenV1ClientID(kr, []byte(v1))
		if err != nil || id.Contract() != 0x6e7c0de0 || !id.IsPrimary() || id.Uuid() != 0 {
			t.Errorf("%s: %x %v", name, []byte(id), err)
		}
	}
	other := &config.Keyring{Keys: []config.Key{{ID: 0, Key: newKey, Use: config.KeyIssue}}}
	if _, err := OpenV1ClientID(other, []byte(v1)); err == nil {
		t.Error("a v1 id opened with another key")
	}
}

// TestStoreKeys checks that the store subkeys are each key's own, by id,
// and the same ones whatever key issues.
func TestStoreKeys(t *testing.T) {
	old := config.Key{ID: 0, Key: oldKey, Use: config.KeyIssue}
	during := set(t, config.Key{ID: 1, Key: newKey, Use: config.KeyIssue}, config.Key{ID: 0, Key: oldKey, Use: config.KeyRead}).StoreKeys()
	if len(during) != 2 || !bytes.Equal(during[0], old.Subkey(config.SubkeyStore)) || bytes.Equal(during[0], during[1]) {
		t.Fatal("store subkeys are not each key's own")
	}
	if before := set(t, old).StoreKeys(); !bytes.Equal(before[0], during[0]) {
		t.Fatal("a key's store subkey changed with the issue key")
	}
}
