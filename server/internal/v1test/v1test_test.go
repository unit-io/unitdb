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

package v1test

import (
	"encoding/hex"
	"testing"

	"github.com/unit-io/unitdb/server/internal/message/security"
	"github.com/unit-io/unitdb/server/internal/pkg/crypto"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
)

var testKey = []byte("test-only-key-do-not-use-0000000")

// TestClientIDAsIssued checks that ClientID seals an id as v0.3 to v0.6.0
// sealed it: the expected text was produced by their code.
func TestClientIDAsIssued(t *testing.T) {
	raw, _ := hex.DecodeString("0102030400beef016e7c0de0")
	const want = "AEBAEBUTQJGYWRbOFeTVTSIZIMcfPGQGQQSbaLAHEeOOCUFXHUPQ"
	if got := ClientID(raw, testKey); got != want {
		t.Fatalf("ClientID = %s, want %s", got, want)
	}
	mac, err := crypto.New(testKey)
	if err != nil {
		t.Fatal(err)
	}
	id, _ := uid.NewClientID(1)
	back, err := uid.DecodeV1([]byte(ClientID(id, testKey)), mac)
	if err != nil || string(back) != string(id[:12]) {
		t.Fatalf("opened %x (%v), want %x", []byte(back), err, []byte(id[:12]))
	}
}

// TestSignedTopicKeyAsIssued checks that SignedTopicKey signs a key as
// v0.4 to v0.6.0 signed it: the expected text was produced by their code.
func TestSignedTopicKeyAsIssued(t *testing.T) {
	const want = "DDQAAACLBNMP3BYTELBJ4W7ZJI"
	if got := SignedTopicKey(testKey, 0x6e7c0de0, "teams.alpha", security.AllowReadWrite); got != want {
		t.Fatalf("SignedTopicKey = %s, want %s", got, want)
	}
	if got := UnsignedTopicKey(0x6e7c0de0, "teams.alpha", security.AllowReadWrite); len(got) != 13 {
		t.Fatalf("an unsigned key of %d characters", len(got))
	}
}
