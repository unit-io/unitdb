package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"

	jcr "github.com/DisposaBoy/JsonConfigReader"
)

// TestSampleConfig checks that the sample config reads as the server reads
// it, and does not allow insecure clients.
func TestSampleConfig(t *testing.T) {
	f, err := os.Open("../../unitdb.conf")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var c Config
	if err := json.NewDecoder(jcr.New(f)).Decode(&c); err != nil {
		t.Fatal(err)
	}
	if c.AllowInsecure || c.AcceptUnsignedKeys != nil {
		t.Errorf("the sample config allows insecure clients (%t) or sets accept_unsigned_keys", c.AllowInsecure)
	}
	if !c.SealsAtRest() {
		t.Error("the sample config does not seal stored records")
	}
	var on Config
	if err := json.Unmarshal([]byte(`{"allow_insecure": true}`), &on); err != nil || !on.AllowInsecure {
		t.Errorf("allow_insecure is not read: %v", err)
	}
}

// TestAcceptUnsignedKeysRemoved checks that accept_unsigned_keys, gone
// since v0.7.0, stops the server when set to true, and is ignored with a
// warning when set to false.
func TestAcceptUnsignedKeysRemoved(t *testing.T) {
	for conf, want := range map[string]struct{ warn, err bool }{
		`{}`:                              {false, false},
		`{"accept_unsigned_keys": null}`:  {false, false},
		`{"accept_unsigned_keys": false}`: {true, false},
		`{"accept_unsigned_keys": true}`:  {false, true},
	} {
		var c Config
		if err := json.Unmarshal([]byte(conf), &c); err != nil {
			t.Fatal(err)
		}
		warning, err := c.CheckRemoved()
		if (warning != "") != want.warn || (err != nil) != want.err {
			t.Errorf("%s: warning %q, error %v", conf, warning, err)
		}
		if err != nil && !strings.Contains(err.Error(), "accept_unsigned_keys") {
			t.Errorf("%s: the error does not name the setting: %v", conf, err)
		}
	}
}

// TestEncryptAtRestDefault checks that encrypt_at_rest is on unless set to
// false.
func TestEncryptAtRestDefault(t *testing.T) {
	for conf, want := range map[string]bool{
		`{}`:                         true,
		`{"encrypt_at_rest": null}`:  true,
		`{"encrypt_at_rest": true}`:  true,
		`{"encrypt_at_rest": false}`: false,
	} {
		var c Config
		if err := json.Unmarshal([]byte(conf), &c); err != nil {
			t.Fatal(err)
		}
		if got := c.SealsAtRest(); got != want {
			t.Errorf("%s: sealing %t, want %t", conf, got, want)
		}
	}
}

func TestEncryptionKey(t *testing.T) {
	const good = "test-only-key-do-not-use-0000000"
	conf := func(key string) *Config {
		return &Config{EncryptionConfig: json.RawMessage(`{"key":"` + key + `","identifier":"local"}`)}
	}
	for _, tc := range []struct {
		name, key, env, want string
	}{
		{name: "config", key: good, want: good},
		{name: "env overrides config", key: "short", env: good, want: good},
		{name: "env alone", env: good, want: good},
		{name: "missing"},
		{name: "sample key", key: sampleKey},
		{name: "sample key from env", key: good, env: sampleKey},
		{name: "too short", key: "0123456789"},
		{name: "too long", key: good + "0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(EncryptionKeyEnv, tc.env)
			c := conf(tc.key)
			if tc.key == "" {
				c.EncryptionConfig = nil
			}
			key, err := c.EncryptionKey()
			if tc.want == "" {
				if err == nil {
					t.Fatalf("EncryptionKey accepted %q", key)
				}
				if !strings.Contains(err.Error(), "openssl rand") {
					t.Errorf("the error does not say how to make a key: %v", err)
				}
				return
			}
			if err != nil || string(key) != tc.want {
				t.Fatalf("EncryptionKey = %q, %v, want %q", key, err, tc.want)
			}
		})
	}
}
