package config

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestKeyring(t *testing.T) {
	good := base64.StdEncoding.EncodeToString([]byte("keyring-test-key-0123456789abcde"))
	other := base64.StdEncoding.EncodeToString([]byte("keyring-test-key-other-012345678"))
	sample := base64.StdEncoding.EncodeToString([]byte(sampleKey))
	const legacyKey = "legacy-key-0123456789abcdefghijk"
	tests := []struct {
		name    string
		env     string // UNITDB_KEYRING, if set
		legacy  string // encryption_config.key
		wantErr string
		issue   uint8
		keys    int
	}{
		{name: "legacy key", legacy: legacyKey, keys: 1},
		{name: "legacy sample key", legacy: sampleKey, wantErr: "sample key"},
		{name: "legacy short key", legacy: "short", wantErr: "has 5 bytes"},
		{name: "legacy empty key", legacy: "", wantErr: "no encryption key"},
		{name: "keyring", env: `[{"id": 3, "key": "` + good + `", "use": "issue"}, {"id": 0, "key": "` + other + `", "use": "read"}]`, legacy: sampleKey, issue: 3, keys: 2},
		{name: "keyring sample key", env: `[{"id": 0, "key": "` + sample + `", "use": "issue"}]`, wantErr: "sample key"},
		{name: "keyring no issue key", env: `[{"id": 0, "key": "` + good + `", "use": "read"}]`, wantErr: "exactly one issue key"},
		{name: "keyring two issue keys", env: `[{"id": 0, "key": "` + good + `", "use": "issue"}, {"id": 1, "key": "` + other + `", "use": "issue"}]`, wantErr: "exactly one issue key"},
		{name: "keyring repeated id", env: `[{"id": 1, "key": "` + good + `", "use": "issue"}, {"id": 1, "key": "` + other + `", "use": "read"}]`, wantErr: "repeated"},
		{name: "keyring id out of range", env: `[{"id": 256, "key": "` + good + `", "use": "issue"}]`, wantErr: "0..255"},
		{name: "keyring bad use", env: `[{"id": 0, "key": "` + good + `", "use": "sign"}]`, wantErr: "use"},
		{name: "keyring raw key", env: `[{"id": 0, "key": "keyringtestkey0123456789abcdefgh", "use": "issue"}]`, wantErr: "is 24 bytes"},
		{name: "keyring not base64", env: `[{"id": 0, "key": "!!!", "use": "issue"}]`, wantErr: "not base64"},
		{name: "keyring not json", env: `key`, wantErr: "JSON"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(KeyringEnv, tt.env)
			t.Setenv(EncryptionKeyEnv, "")
			encr, _ := json.Marshal(EncryptionConfig{Key: tt.legacy})
			kr, err := (&Config{EncryptionConfig: encr}).Keyring()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(kr.Keys) != tt.keys || kr.Issue().ID != tt.issue || len(kr.Issue().Key) != KeyLen {
				t.Errorf("keyring %+v, want %d keys issuing with key %d", kr, tt.keys, tt.issue)
			}
			if tt.env == "" && string(kr.Issue().Key) != tt.legacy {
				t.Errorf("the single key is not key 0 as it is")
			}
		})
	}
}

// TestKeyringFile checks that keyring_file is read, and that UNITDB_KEYRING
// takes its place.
func TestKeyringFile(t *testing.T) {
	t.Setenv(KeyringEnv, "")
	key := base64.StdEncoding.EncodeToString([]byte("keyring-file-key-0123456789abcde"))
	file := filepath.Join(t.TempDir(), "keyring.json")
	if err := os.WriteFile(file, []byte(`[{"id": 9, "key": "`+key+`", "use": "issue"}]`), 0600); err != nil {
		t.Fatal(err)
	}
	c := &Config{EncryptionConfig: json.RawMessage(`{"keyring_file": "` + file + `"}`)}
	kr, err := c.Keyring()
	if err != nil || kr.Issue().ID != 9 {
		t.Fatalf("keyring file: %+v %v", kr, err)
	}
	t.Setenv(KeyringEnv, `[{"id": 4, "key": "`+key+`", "use": "issue"}]`)
	if kr, err := c.Keyring(); err != nil || kr.Issue().ID != 4 {
		t.Fatalf("UNITDB_KEYRING over keyring_file: %+v %v", kr, err)
	}
	t.Setenv(KeyringEnv, "")
	missing := &Config{EncryptionConfig: json.RawMessage(`{"keyring_file": "` + file + `.missing"}`)}
	if _, err := missing.Keyring(); err == nil {
		t.Fatal("a missing keyring file was taken")
	}
}

func TestSubkeys(t *testing.T) {
	k := Key{ID: 0, Key: []byte("subkey-test-key-0123456789abcdef")}
	a, b := k.Subkey(SubkeyClientID), k.Subkey(SubkeyTopicKey)
	if len(a) != KeyLen || len(b) != KeyLen || bytes.Equal(a, b) || bytes.Equal(a, k.Key) {
		t.Fatal("subkeys are not distinct 32-byte keys")
	}
	if !bytes.Equal(a, k.Subkey(SubkeyClientID)) {
		t.Fatal("a subkey is not the same each time")
	}
}

func TestTTLs(t *testing.T) {
	c := &Config{}
	if client, primary, key, err := c.TTLs(); err != nil || client != 0 || primary != 0 || key != 0 {
		t.Fatalf("default ttls %s %s %s %v, want 0", client, primary, key, err)
	}
	c = &Config{ClientIDTTL: "720h", PrimaryIDTTL: "0", TopicKeyTTL: "1h"}
	if client, primary, key, err := c.TTLs(); err != nil || client != 720*time.Hour || primary != 0 || key != time.Hour {
		t.Fatalf("ttls %s %s %s %v", client, primary, key, err)
	}
	for _, bad := range []*Config{{ClientIDTTL: "a month"}, {PrimaryIDTTL: "-1h"}, {TopicKeyTTL: "1"}} {
		if _, _, _, err := bad.TTLs(); err == nil {
			t.Errorf("%+v was taken", bad)
		}
	}
}
