package backup

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"filippo.io/age"
)

func TestArchiveRoundTrip(t *testing.T) {
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "unitdb", "sub"), 0700)
	os.WriteFile(filepath.Join(src, "checkpoint.json"), []byte(`{"node":"a"}`), 0600)
	big := bytes.Repeat([]byte("unitdb "), 100000)
	os.WriteFile(filepath.Join(src, "unitdb", "sub", "data"), big, 0600)
	id, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := WriteArchive(&buf, src, id.Recipient()); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(buf.Bytes(), []byte(`"node":"a"`)) || bytes.Contains(buf.Bytes(), []byte("unitdb unitdb")) {
		t.Fatal("the archive isn't encrypted")
	}
	if buf.Len() > len(big)/10 {
		t.Errorf("the archive is %d bytes for %d compressible ones", buf.Len(), len(big))
	}

	dst := filepath.Join(t.TempDir(), "out")
	if err := ExtractArchive(bytes.NewReader(buf.Bytes()), id, dst); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "unitdb", "sub", "data")); !bytes.Equal(b, big) {
		t.Error("the extracted file differs")
	}
	if b, _ := os.ReadFile(filepath.Join(dst, "checkpoint.json")); string(b) != `{"node":"a"}` {
		t.Errorf("checkpoint.json: %q", b)
	}

	// Another key doesn't open it; a directory that isn't empty is refused.
	other, _ := age.GenerateX25519Identity()
	if err := ExtractArchive(bytes.NewReader(buf.Bytes()), other, filepath.Join(t.TempDir(), "x")); err == nil {
		t.Error("another key opened the archive")
	}
	if err := ExtractArchive(bytes.NewReader(buf.Bytes()), id, dst); err == nil {
		t.Error("extracted into a directory that isn't empty")
	}
}

func TestRunRetention(t *testing.T) {
	day := func(s string) time.Time { d, _ := time.Parse("2006-01-02T15:04", s); return d }
	for _, tc := range []struct {
		run  string
		tier string
		days int
	}{
		{"2026-10-01T21:30", "monthly", 396}, // the 1st, a Thursday
		{"2026-10-04T21:30", "weekly", 29},   // a Sunday
		{"2026-11-01T21:30", "monthly", 396}, // a Sunday and the 1st
		{"2026-10-06T21:30", "daily", 8},
	} {
		r := RunRetention(day(tc.run))
		if got := int(r.Until.Sub(day(tc.run)).Hours() / 24); r.Tier != tc.tier || got != tc.days {
			t.Errorf("%s: %s for %d days, want %s for %d", tc.run, r.Tier, got, tc.tier, tc.days)
		}
	}
}

func TestMergeKeyring(t *testing.T) {
	cur := `[{"id":1,"key":"k1","use":"read"},{"id":2,"key":"k2","use":"issue"}]`
	got, err := MergeKeyring(`[{"id":0,"key":"k0","use":"issue"},{"id":1,"key":"k1","use":"issue"}]`, cur)
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"id":0,"key":"k0","use":"read"},{"id":1,"key":"k1","use":"read"},{"id":2,"key":"k2","use":"issue"}]`
	if got != want {
		t.Errorf("merged %s\nwant   %s", got, want)
	}
	if _, err := MergeKeyring(`[{"id":2,"key":"other","use":"issue"}]`, cur); err == nil || !strings.Contains(err.Error(), "never reused") {
		t.Errorf("a key id reused: %v", err)
	}
	if got, err := MergeKeyring("", cur); err != nil || got != `[{"id":1,"key":"k1","use":"read"},{"id":2,"key":"k2","use":"issue"}]` {
		t.Errorf("into an empty escrow: %s %v", got, err)
	}
}

type fakeSecrets map[string]string

func (f fakeSecrets) Get(_ context.Context, name string) (string, bool, error) {
	v, ok := f[name]
	return v, ok, nil
}

func (f fakeSecrets) Put(_ context.Context, name, value string) error {
	f[name] = value
	return nil
}

func TestEscrowKeyring(t *testing.T) {
	ss := fakeSecrets{}
	ctx := context.Background()
	if err := EscrowKeyring(ctx, ss, "staging", `[{"id":0,"key":"k0","use":"issue"}]`); err != nil {
		t.Fatal(err)
	}
	// The key rotates: the old one stays in escrow, as a read key.
	if err := EscrowKeyring(ctx, ss, "staging", `[{"id":1,"key":"k1","use":"issue"}]`); err != nil {
		t.Fatal(err)
	}
	ids, err := EscrowedKeyIDs(ss[KeyringSecret("staging")])
	if err != nil || !ids[0] || !ids[1] {
		t.Errorf("escrowed %s (%v)", ss[KeyringSecret("staging")], err)
	}
}
