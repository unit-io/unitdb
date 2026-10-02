package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/unit-io/unitdb/server/internal/config"
	"github.com/unit-io/unitdb/server/internal/pkg/crypto"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
)

const testKey = "mintid-test-key-0123456789abcdef"

// mint runs mintid with args and returns the id it printed, decoded with
// key.
func mint(t *testing.T, key string, args ...string) uid.ID {
	t.Helper()
	var out bytes.Buffer
	if err := run(args, &out); err != nil {
		t.Fatalf("mintid %v: %v", args, err)
	}
	var text string
	for _, line := range strings.Split(out.String(), "\n") {
		if v, ok := strings.CutPrefix(line, "client id: "); ok {
			text = v
		}
	}
	mac, err := crypto.New([]byte(key))
	if err != nil {
		t.Fatal(err)
	}
	id, err := uid.Decode([]byte(text), mac)
	if err != nil {
		t.Fatalf("mintid printed %q, which does not decode: %v\n%s", text, err, out.String())
	}
	return id
}

func TestMintID(t *testing.T) {
	t.Setenv(config.EncryptionKeyEnv, testKey)

	primary := mint(t, testKey)
	if !primary.IsPrimary() || primary.IsService() || primary.Contract() == 0 {
		t.Errorf("primary id: permissions %d, contract %d", primary.Permissions(), primary.Contract())
	}
	if other := mint(t, testKey); other.Contract() == primary.Contract() {
		t.Errorf("two ids without -contract share contract %d", other.Contract())
	}

	service := mint(t, testKey, "-contract", "123456789", "-service")
	if !service.IsService() || !service.IsPrimary() || service.Contract() != 123456789 {
		t.Errorf("service id: permissions %d, contract %d", service.Permissions(), service.Contract())
	}
	if plain := mint(t, testKey, "-contract", "123456789"); plain.IsService() || plain.Contract() != 123456789 {
		t.Errorf("id of a contract: permissions %d, contract %d", plain.Permissions(), plain.Contract())
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
	if id := mint(t, testKey, "-config", conf, "-service"); !id.IsService() {
		t.Errorf("service id from the config's key: permissions %d", id.Permissions())
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
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(config.EncryptionKeyEnv, tc.key)
			if err := run(tc.args, &bytes.Buffer{}); err == nil {
				t.Error("mintid issued an id")
			}
		})
	}
}
