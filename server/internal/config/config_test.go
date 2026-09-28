package config

import (
	"encoding/json"
	"strings"
	"testing"
)

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
