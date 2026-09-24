package security

import (
	"strings"
	"testing"
)

const testContract = uint32(3376684800)

func TestGenerateAndDecodeKey(t *testing.T) {
	key, err := GenerateKey(testContract, "teams.alpha.ch1", AllowReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != encodedLen {
		t.Fatalf("key length %d, want %d", len(key), encodedLen)
	}

	k, err := DecodeKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if k.Permissions() != AllowReadWrite {
		t.Fatalf("permissions %d, want %d", k.Permissions(), AllowReadWrite)
	}
	if !k.HasPermission(AllowRead) || !k.HasPermission(AllowWrite) {
		t.Fatal("expected read and write permissions")
	}
	if k.HasPermission(AllowAdmin) {
		t.Fatal("unexpected admin permission")
	}
	if k.Encode() != key {
		t.Fatalf("re-encoded key %q, want %q", k.Encode(), key)
	}
}

func TestDecodeKeyInvalidLength(t *testing.T) {
	for _, key := range []string{"", "short", strings.Repeat("a", encodedLen+1)} {
		if _, err := DecodeKey(key); err == nil {
			t.Errorf("DecodeKey(%q): expected error", key)
		}
	}
}

func TestValidateTopic(t *testing.T) {
	tests := []struct {
		name     string
		keyTopic string
		contract uint32
		topic    string
		wantOK   bool
		wantWild bool
	}{
		{"static match", "teams.alpha.ch1", testContract, "teams.alpha.ch1", true, false},
		{"static mismatch", "teams.alpha.ch1", testContract, "teams.alpha.ch2", false, false},
		{"other contract", "teams.alpha.ch1", testContract + 1, "teams.alpha.ch1", false, false},
		{"multi wildcard key", "teams...", testContract, "teams...", true, true},
		{"wildcard key", "teams.*", testContract, "teams.*", true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := GenerateKey(testContract, tt.keyTopic, AllowRead)
			if err != nil {
				t.Fatal(err)
			}
			k, err := DecodeKey(key)
			if err != nil {
				t.Fatal(err)
			}
			ok, wildcard := k.ValidateTopic(tt.contract, tt.topic)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && wildcard != tt.wantWild {
				t.Fatalf("wildcard = %v, want %v", wildcard, tt.wantWild)
			}
		})
	}
}

func TestGenerateKeyTargetTooLong(t *testing.T) {
	topic := strings.TrimSuffix(strings.Repeat("a.", 24), ".")
	if _, err := GenerateKey(testContract, topic, AllowRead); err != ErrTargetTooLong {
		t.Fatalf("err = %v, want %v", err, ErrTargetTooLong)
	}
	topic = strings.TrimSuffix(strings.Repeat("a.", 23), ".")
	if _, err := GenerateKey(testContract, topic, AllowRead); err != nil {
		t.Fatalf("23 parts must be accepted: %v", err)
	}
}

func TestParseKey(t *testing.T) {
	tests := []struct {
		text      string
		wantKey   string
		wantTopic string // topic without options
		wantType  uint8
	}{
		{"teams.alpha", "", "teams.alpha", 0},
		{"KEY/teams.alpha", "KEY", "teams.alpha", 0},
		{"KEY/teams.alpha?ttl=1m", "KEY", "teams.alpha", 0},
		{"unitdb/keygen", "unitdb", "keygen", 0},
		{"KEY/?", "KEY", "", TopicInvalid},
		{"teams.alpha?last=1m", "", "teams.alpha", 0},
		{"KEY/?a=b", "KEY", "", TopicInvalid},
		{"?a=b", "", "", TopicInvalid},
	}
	for _, tt := range tests {
		topic := ParseKey(tt.text)
		if topic.Key != tt.wantKey {
			t.Errorf("ParseKey(%q).Key = %q, want %q", tt.text, topic.Key, tt.wantKey)
		}
		if topic.TopicType != tt.wantType {
			t.Errorf("ParseKey(%q).TopicType = %d, want %d", tt.text, topic.TopicType, tt.wantType)
		}
		if tt.wantType == TopicInvalid {
			continue
		}
		if got := topic.Topic[:topic.Size]; got != tt.wantTopic {
			t.Errorf("ParseKey(%q) topic = %q, want %q", tt.text, got, tt.wantTopic)
		}
	}
}

func TestTopicTypeConstantsDoNotCollideWithDefault(t *testing.T) {
	// ParseKey never sets TopicStatic, so a valid topic keeps the zero value.
	// The handlers only reject TopicInvalid, which must therefore be non-zero.
	if TopicInvalid == 0 {
		t.Fatal("TopicInvalid must not be the zero value")
	}
}

func TestTarget(t *testing.T) {
	// The special request hashes used by the connection handler.
	const (
		requestClientId = 2682859131
		requestKeygen   = 812942072
	)
	if got := ParseKey("unitdb/clientid").Target(); got != requestClientId {
		t.Fatalf("clientid target = %d, want %d", got, requestClientId)
	}
	if got := ParseKey("unitdb/keygen").Target(); got != requestKeygen {
		t.Fatalf("keygen target = %d, want %d", got, requestKeygen)
	}
}

func TestParseKeyEmptyTopic(t *testing.T) {
	for _, text := range []string{"", "/", "//"} {
		if topic := ParseKey(text); topic.TopicType != TopicInvalid {
			t.Errorf("ParseKey(%q).TopicType = %d, want TopicInvalid", text, topic.TopicType)
		}
	}
}
