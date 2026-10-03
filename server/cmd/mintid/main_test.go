package main

import (
	"bytes"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/unit-io/unitdb/server/internal/config"
	"github.com/unit-io/unitdb/server/internal/keys"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/v1test"
)

const testKey = "mintid-test-key-0123456789abcdef"

// keySet returns the keys of the single key key, as the server has them.
func keySet(t *testing.T, key string) *keys.Set {
	t.Helper()
	s, err := keys.New(&config.Keyring{Keys: []config.Key{{ID: 0, Key: []byte(key), Use: config.KeyIssue}}})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// mintText runs mintid with args and returns the id it printed.
func mintText(t *testing.T, args ...string) string {
	t.Helper()
	var out bytes.Buffer
	if err := run(args, &out); err != nil {
		t.Fatalf("mintid %v: %v", args, err)
	}
	for _, line := range strings.Split(out.String(), "\n") {
		if v, ok := strings.CutPrefix(line, "client id: "); ok {
			return v
		}
	}
	t.Fatalf("mintid printed no id:\n%s", out.String())
	return ""
}

// mint runs mintid with args and returns the id it printed, opened with
// set, and its claims.
func mint(t *testing.T, set *keys.Set, args ...string) (uid.ID, uid.Claims) {
	t.Helper()
	text := mintText(t, args...)
	id, claims, err := set.OpenClientID([]byte(text))
	if err != nil {
		t.Fatalf("mintid printed %q, which does not open: %v", text, err)
	}
	return id, claims
}

func TestMintID(t *testing.T) {
	t.Setenv(config.EncryptionKeyEnv, testKey)
	set := keySet(t, testKey)

	primary, claims := mint(t, set)
	if !primary.IsPrimary() || primary.IsService() || primary.Contract() == 0 {
		t.Errorf("primary id: permissions %d, contract %d", primary.Permissions(), primary.Contract())
	}
	if claims.ExpiresAt != 0 || primary.Uuid() == 0 {
		t.Errorf("a v2 id that never expires, with a uuid, was wanted: claims %+v, uuid %d", claims, primary.Uuid())
	}
	if other, _ := mint(t, set); other.Contract() == primary.Contract() || other.Uuid() == primary.Uuid() {
		t.Errorf("two ids without -contract share contract %d or uuid %d", other.Contract(), other.Uuid())
	}

	service, _ := mint(t, set, "-contract", "123456789", "-service")
	if !service.IsService() || !service.IsPrimary() || service.Contract() != 123456789 {
		t.Errorf("service id: permissions %d, contract %d", service.Permissions(), service.Contract())
	}
	if plain, _ := mint(t, set, "-contract", "123456789"); plain.IsService() || plain.Contract() != 123456789 {
		t.Errorf("id of a contract: permissions %d, contract %d", plain.Permissions(), plain.Contract())
	}

	_, claims = mint(t, set, "-ttl", "1h")
	if now := uint32(time.Now().Unix()); claims.ExpiresAt < now+3590 || claims.ExpiresAt > now+3610 {
		t.Errorf("-ttl 1h: claims %+v", claims)
	}
}

// TestMintIDFrom checks that -from seals a v1 id again as a v2 one, the same
// id, and a v2 one of a key being retired with the issue key.
func TestMintIDFrom(t *testing.T) {
	old := base64.StdEncoding.EncodeToString([]byte(testKey))
	next := base64.StdEncoding.EncodeToString([]byte("mintid-next-key-0123456789abcdef"))
	t.Setenv(config.EncryptionKeyEnv, testKey)
	set := keySet(t, testKey)

	// A service's v1 id, as v0.6.0 and before issued it.
	v1, err := uid.MintClientID(42, true)
	if err != nil {
		t.Fatal(err)
	}
	v1 = v1[:12] // a v1 id carries no uuid
	v1Text := v1test.ClientID(v1, []byte(testKey))
	if _, _, err := set.OpenClientID([]byte(v1Text)); err != keys.ErrV1ClientID {
		t.Fatalf("the server's keys open a v1 id: %v", err)
	}
	again, claims := mint(t, set, "-from", v1Text, "-ttl", "24h")
	if !bytes.Equal(again, v1) || claims.ExpiresAt == 0 || again.Uuid() != 0 || !again.IsService() || again.Contract() != 42 {
		t.Errorf("-from a v1 id: %x %+v, want %x as v2", again, claims, v1)
	}
	// One sealed with another key than the keyring's is refused.
	if err := run([]string{"-from", v1test.ClientID(v1, []byte("mintid-other-key-0123456789abcde"))}, &bytes.Buffer{}); err == nil {
		t.Error("-from took a v1 id of a key the keyring lacks")
	}

	// Rotation: key 1 issues, key 0 reads.
	v2Text := mintText(t, "-contract", "43")
	t.Setenv(config.KeyringEnv, `[{"id": 1, "key": "`+next+`", "use": "issue"}, {"id": 0, "key": "`+old+`", "use": "read"}]`)
	kr, err := (&config.Config{}).Keyring()
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := keys.New(kr)
	if err != nil {
		t.Fatal(err)
	}
	before, beforeClaims, err := rotated.OpenClientID([]byte(v2Text))
	if err != nil || beforeClaims.KeyID != 0 {
		t.Fatalf("an id of the read key: %v %+v", err, beforeClaims)
	}
	moved, movedClaims := mint(t, rotated, "-from", v2Text)
	if !bytes.Equal(moved, before) || movedClaims.KeyID != 1 {
		t.Errorf("-from an id of the read key: %x %+v, want %x with key 1", moved, movedClaims, before)
	}
	// A v1 id of the read key, sealed again with the issue key.
	if fromRead, claims := mint(t, rotated, "-from", v1Text); !bytes.Equal(fromRead, v1) || claims.KeyID != 1 {
		t.Errorf("-from a v1 id of the read key: %x %+v, want %x with key 1", fromRead, claims, v1)
	}
	if _, _, err := set.OpenClientID([]byte(mintText(t, "-from", v2Text))); err == nil {
		t.Error("an id sealed with the new key opens with the old one alone")
	}
}

func TestMintIDKeyFromConfig(t *testing.T) {
	t.Setenv(config.EncryptionKeyEnv, "")
	conf := filepath.Join(t.TempDir(), "unitdb.conf")
	// A comment, as unitdb.conf has: the config is read as the server reads
	// it.
	if err := os.WriteFile(conf, []byte(`{
	// the key
	"encryption_config": {"key": "`+testKey+`"}
}`), 0600); err != nil {
		t.Fatal(err)
	}
	if id, _ := mint(t, keySet(t, testKey), "-config", conf, "-service"); !id.IsService() {
		t.Errorf("service id from the config's key: permissions %d", id.Permissions())
	}

	// A keyring file.
	ring := filepath.Join(t.TempDir(), "keyring.json")
	other := "mintid-ring-key-0123456789abcdef"
	if err := os.WriteFile(ring, []byte(`[{"id": 0, "key": "`+base64.StdEncoding.EncodeToString([]byte(other))+`", "use": "issue"}]`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(conf, []byte(`{"encryption_config": {"key": "`+testKey+`", "keyring_file": "`+ring+`"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if id, _ := mint(t, keySet(t, other), "-config", conf); !id.IsPrimary() {
		t.Error("an id from the keyring file")
	}
}

func TestMintIDRefuses(t *testing.T) {
	for name, tc := range map[string]struct {
		key  string
		args []string
	}{
		"no key":          {"", nil},
		"the sample key":  {"4BWm1vZletvrCDGWsF6mex8oBSd59m6I", nil},
		"a short key":     {"short", nil},
		"a wide contract": {testKey, []string{"-contract", "4294967296"}},
		"stray arguments": {testKey, []string{"service"}},
		"a negative ttl":  {testKey, []string{"-ttl", "-1h"}},
		"-v1, gone":       {testKey, []string{"-v1"}},
		"garbage -from":   {testKey, []string{"-from", "not-a-client-id"}},
		"-from -service":  {testKey, []string{"-from", "x", "-service"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(config.EncryptionKeyEnv, tc.key)
			if err := run(tc.args, &bytes.Buffer{}); err == nil {
				t.Error("mintid issued an id")
			}
		})
	}
}
