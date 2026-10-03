/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package config

import (
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

// The server's keys are a keyring: each key has an id, and is used to issue
// (seal client ids and sign topic keys) or only to read what an older key
// issued. v2 client ids and topic keys name the key that issued them (v1
// ones, refused since v0.7.0, didn't: server/cmd/mintid -from reads one with
// every key, to seal it again as v2). Rotating the key is adding a new
// issue key, keeping the old one to read with until what it issued has been
// replaced, then removing it.

// KeyLen is the length of every key, in bytes.
const KeyLen = 32

// KeyringEnv names the environment variable that holds the keyring, as a
// JSON list of keys (see Keyring). It takes the place of keyring_file, and
// of the single key.
const KeyringEnv = "UNITDB_KEYRING"

// Key uses.
const (
	KeyIssue = "issue"
	KeyRead  = "read"
)

// Key is one key of the keyring.
type Key struct {
	ID uint8
	// Key is the key's 32 bytes. v2 client ids and topic keys are sealed
	// and signed with subkeys of it; v1 ones were, with it as it is.
	Key []byte
	Use string
}

// Subkey labels.
const (
	// SubkeyClientID seals v2 client ids.
	SubkeyClientID = "cid/v2"
	// SubkeyTopicKey signs v2 topic keys.
	SubkeyTopicKey = "tkey/v2"
	// SubkeyStore seals stored records (encrypt_at_rest).
	SubkeyStore = "store/v1"
)

// Subkey derives the key's subkey for one use: HKDF-SHA256 with the info
// "unitdb/<label>", so that no two uses share a key.
func (k Key) Subkey(label string) []byte {
	out, err := hkdf.Key(sha256.New, k.Key, nil, "unitdb/"+label, KeyLen)
	if err != nil {
		panic(err)
	}
	return out
}

// Keyring is the server's keys.
type Keyring struct {
	// Keys are all the keys, the issue key among them.
	Keys []Key
}

// Issue returns the key the server issues with.
func (k *Keyring) Issue() Key {
	for _, key := range k.Keys {
		if key.Use == KeyIssue {
			return key
		}
	}
	panic("config: keyring without an issue key")
}

// keyringEntry is a key as the keyring's JSON holds it.
type keyringEntry struct {
	ID  int    `json:"id"`
	Key string `json:"key"` // base64 of 32 bytes
	Use string `json:"use"`
}

// Keyring returns the server's keyring. It is read from the UNITDB_KEYRING
// environment variable, else from encryption_config's keyring_file, as a
// JSON list of {"id": 0-255, "key": "<base64 of 32 bytes>", "use": "issue"
// or "read"} with exactly one issue key. Without either, the single key of
// EncryptionKey (UNITDB_ENCRYPTION_KEY or encryption_config's key) is the
// keyring, as key 0. It refuses the sample key and keys that aren't 32
// bytes.
func (c *Config) Keyring() (*Keyring, error) {
	var raw []byte
	source := KeyringEnv
	if env := os.Getenv(KeyringEnv); env != "" {
		raw = []byte(env)
	} else if c.EncryptionConfig != nil {
		if file := c.Encryption(c.EncryptionConfig).KeyringFile; file != "" {
			b, err := os.ReadFile(file)
			if err != nil {
				return nil, fmt.Errorf("keyring: %v", err)
			}
			raw, source = b, file
		}
	}
	if raw == nil {
		key, err := c.EncryptionKey()
		if err != nil {
			return nil, err
		}
		return &Keyring{Keys: []Key{{ID: 0, Key: key, Use: KeyIssue}}}, nil
	}
	kr, err := parseKeyring(raw)
	if err != nil {
		return nil, fmt.Errorf("keyring %s: %v", source, err)
	}
	return kr, nil
}

func parseKeyring(raw []byte) (*Keyring, error) {
	var entries []keyringEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, fmt.Errorf("not a JSON list of keys: %v", err)
	}
	kr := &Keyring{}
	seen := make(map[int]bool)
	issue := 0
	for _, e := range entries {
		if e.ID < 0 || e.ID > 255 {
			return nil, fmt.Errorf("key id %d is not in 0..255", e.ID)
		}
		if seen[e.ID] {
			return nil, fmt.Errorf("key id %d is repeated", e.ID)
		}
		seen[e.ID] = true
		switch e.Use {
		case KeyIssue:
			issue++
		case KeyRead:
		default:
			return nil, fmt.Errorf("key %d: use %q is neither %q nor %q", e.ID, e.Use, KeyIssue, KeyRead)
		}
		key, err := base64.StdEncoding.DecodeString(e.Key)
		if err != nil {
			return nil, fmt.Errorf("key %d is not base64: %v", e.ID, err)
		}
		switch {
		case string(key) == sampleKey:
			return nil, fmt.Errorf("key %d is the published sample key, which anyone can sign client IDs and topic keys with: generate one with `openssl rand -base64 32`", e.ID)
		case len(key) != KeyLen:
			return nil, fmt.Errorf("key %d is %d bytes, not %d: generate one with `openssl rand -base64 32`", e.ID, len(key), KeyLen)
		}
		kr.Keys = append(kr.Keys, Key{ID: uint8(e.ID), Key: key, Use: e.Use})
	}
	if issue != 1 {
		return nil, errors.New("the keyring needs exactly one issue key")
	}
	return kr, nil
}

// TTLs returns the lifetimes of the v2 client ids the server issues that
// are not primary (unitdb/clientid), of primary ones, and of v2 topic keys
// whose keygen request gives no ttl; 0 never expires, the default.
func (c *Config) TTLs() (client, primary, topicKey time.Duration, err error) {
	parse := func(name, s string) (time.Duration, error) {
		if s == "" {
			return 0, nil
		}
		d, err := time.ParseDuration(s)
		if err != nil || d < 0 {
			return 0, fmt.Errorf("%s: %q is not a duration", name, s)
		}
		return d, nil
	}
	if client, err = parse("client_id_ttl", c.ClientIDTTL); err != nil {
		return
	}
	if primary, err = parse("primary_id_ttl", c.PrimaryIDTTL); err != nil {
		return
	}
	topicKey, err = parse("topic_key_ttl", c.TopicKeyTTL)
	return
}
