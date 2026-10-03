package security

import (
	"strings"
	"testing"
)

const testContract = uint32(3376684800)

// TestGenerateKey checks that a key generated for a topic is the same each
// time: subscriptions without a topic key are counted under it.
func TestGenerateKey(t *testing.T) {
	key, err := GenerateKey(testContract, "teams.alpha.ch1", AllowReadWrite)
	if err != nil {
		t.Fatal(err)
	}
	if len(key) != UnsignedKeyLen {
		t.Fatalf("key length %d, want %d", len(key), UnsignedKeyLen)
	}
	again, _ := GenerateKey(testContract, "teams.alpha.ch1", AllowReadWrite)
	other, _ := GenerateKey(testContract, "teams.alpha.ch2", AllowReadWrite)
	if again != key || other == key {
		t.Fatalf("keys %q, %q for the same topic and %q for another", key, again, other)
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
