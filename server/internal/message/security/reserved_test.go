package security

import "testing"

func TestIsReserved(t *testing.T) {
	for topic, want := range map[string]bool{
		"$sys.users":       true,
		"$":                true,
		"$x?last=1h":       true,
		"$sys":             true,
		"a.$sys":           false,
		"groups.x.message": false,
		"saffat.users.1":   false,
		"...":              false,
		"*":                false,
		"x?last=$1":        false,
	} {
		if got := IsReserved(topic); got != want {
			t.Errorf("IsReserved(%q) = %t, want %t", topic, got, want)
		}
	}
}
