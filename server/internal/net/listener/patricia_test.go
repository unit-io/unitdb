package listener

import (
	"io"
	"strings"
	"testing"
	"time"
)

func TestMatchPrefix(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"GET / HTTP/1.1\r\n", true},
		{"GET", true},
		{"GEX", false},
		{"POST /", false},
		{"G", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := newPatriciaTreeString("GET").matchPrefix(strings.NewReader(tt.input)); got != tt.want {
			t.Errorf("matchPrefix(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestMatchPrefixMultiple(t *testing.T) {
	pt := newPatriciaTreeString("GET", "GEM", "POST")
	for input, want := range map[string]bool{"GET /": true, "GEM": true, "POST": true, "PUT": false, "GEX": false} {
		if got := pt.matchPrefix(strings.NewReader(input)); got != want {
			t.Errorf("matchPrefix(%q) = %v, want %v", input, got, want)
		}
	}
}

func TestMatchPrefixDoesNotWaitForUnneededBytes(t *testing.T) {
	// A 3 byte message that is not HTTP: the connection stays open, so any
	// read past those bytes would block.
	r, w := io.Pipe()
	defer w.Close()
	go w.Write([]byte{0x02, 0x08, 0x01})

	done := make(chan bool, 1)
	go func() { done <- newPatriciaTreeString("GET").matchPrefix(r) }()
	select {
	case got := <-done:
		if got {
			t.Fatal("matched a non HTTP message")
		}
	case <-time.After(time.Second):
		t.Fatal("matchPrefix blocked waiting for more bytes")
	}
}
