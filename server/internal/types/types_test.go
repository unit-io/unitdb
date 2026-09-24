package types

import (
	"testing"

	"github.com/unit-io/unitdb/server/internal/message/security"
)

func TestKeyGenRequestAccess(t *testing.T) {
	tests := []struct {
		typ  string
		want uint32
	}{
		{"", security.AllowNone},
		{"r", security.AllowRead},
		{"w", security.AllowWrite},
		{"rw", security.AllowReadWrite},
		{"a", security.AllowAdmin | security.AllowReadWrite},
		{"o", security.AllowOwner | security.AllowAdmin | security.AllowReadWrite},
		{"x", security.AllowNone},
	}
	for _, tt := range tests {
		req := &KeyGenRequest{Topic: "teams", Type: tt.typ}
		if got := req.Access(); got != tt.want {
			t.Errorf("Access(%q) = %d, want %d", tt.typ, got, tt.want)
		}
	}
}

func TestErrorImplementsError(t *testing.T) {
	var err error = ErrUnauthorized
	if err.Error() != ErrUnauthorized.Message {
		t.Fatalf("Error() = %q", err.Error())
	}
	if ErrUnauthorized.ErrrorCode() != 0x04 {
		t.Fatalf("ErrrorCode() = %d", ErrUnauthorized.ErrrorCode())
	}
}
