package unitdb

import (
	"bytes"
	"encoding/binary"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/unit-io/unitdb/message"
)

var updateFormat = flag.Bool("update-format", false, "rewrite testdata/format from the encoders: only for a deliberate format change")

// golden compares data, encoded, with the saved bytes of name.
func golden(t *testing.T, name string, data []byte) []byte {
	t.Helper()
	path := filepath.Join("testdata", "format", name)
	if *updateFormat {
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (go test -run TestFormat -update-format writes it)", err)
	}
	if !bytes.Equal(data, want) {
		t.Errorf("%s: the encoder writes other bytes than the saved ones: the format changed", name)
	}
	return want
}

func u16(b []byte, off int) uint16 { return binary.LittleEndian.Uint16(b[off:]) }
func u32(b []byte, off int) uint32 { return binary.LittleEndian.Uint32(b[off:]) }
func u64(b []byte, off int) uint64 { return binary.LittleEndian.Uint64(b[off:]) }

// TestFormat checks each format against saved bytes, both ways, and some
// fields at their offsets in format.go, written out here so that moving a
// field fails it.
func TestFormat(t *testing.T) {
	t.Run("info", func(t *testing.T) {
		inf := _DBInfo{header: _Header{signature: signature, version: 3}, encryption: 1, sequence: 0x0102030405060708, count: 42, syncing: 1_700_000_000_000_000_000}
		enc, _ := inf.MarshalBinary()
		raw := golden(t, "info-v3", enc)
		if len(raw) != 40 || raw[11] != 1 || u32(raw, 7) != 3 || u64(raw, 12) != 0x0102030405060708 || u64(raw, 20) != 42 || u64(raw, 28) != 1_700_000_000_000_000_000 || !matchesChecksum(raw, 36) {
			t.Errorf("info header fields not at their offsets: % x", raw)
		}
		var got _DBInfo
		got.UnmarshalBinary(raw)
		if got.header != inf.header || got.encryption != 1 || got.sequence != inf.sequence || got.count != 42 || got.syncing != inf.syncing || !got.validChecksum {
			t.Errorf("decoded %+v; want %+v", got, inf)
		}
	})
	t.Run("info-v2", func(t *testing.T) {
		// Format 2, built from its layout: no syncing, the checksum at 28.
		raw := make([]byte, 32)
		copy(raw, signature[:])
		binary.LittleEndian.PutUint32(raw[7:], 2)
		raw[11] = 1
		binary.LittleEndian.PutUint64(raw[12:], 99)
		binary.LittleEndian.PutUint64(raw[20:], 7)
		putChecksum(raw, 28)
		var got _DBInfo
		got.UnmarshalBinary(raw)
		if got.header.version != 2 || got.encryption != 1 || got.sequence != 99 || got.count != 7 || got.syncing != 0 || !got.validChecksum {
			t.Errorf("decoded %+v", got)
		}
	})
	t.Run("index", func(t *testing.T) {
		var b _IndexBlock
		b.entries[0] = _IndexEntry{seq: 256, topicSize: 11, valueSize: 300, msgOffset: 4096}
		b.entries[1] = _IndexEntry{seq: 258, valueSize: 0, topicSize: 11, msgOffset: 8192} // deleted, holding its topic
		b.entries[2] = _IndexEntry{seq: 300, msgOffset: -1}                                 // deleted
		b.entryIdx = 3
		raw := golden(t, "index-block", b.marshalBinary())
		if len(raw) != 4096 || u64(raw, 0) != 256 || u16(raw, 8+16) != 2+255 || u16(raw, 8+16+2) != 11 || u32(raw, 8+4) != 300 || int64(u64(raw, 8+16*2+8)) != -1 || u16(raw, 4088) != 3 || !matchesChecksum(raw, 4090) {
			t.Errorf("index block fields not at their offsets")
		}
		var got _IndexBlock
		got.unmarshalBinary(raw)
		for i := range b.entries {
			x, y := b.entries[i], got.entries[i]
			if x.seq != y.seq || x.topicSize != y.topicSize || x.valueSize != y.valueSize || x.msgOffset != y.msgOffset {
				t.Errorf("entry %d decoded %+v; want %+v", i, y, x)
			}
		}
		if !got.entries[1].deleted() || !got.entries[2].deleted() || got.entries[0].deleted() {
			t.Errorf("deleted entries not read deleted")
		}
	})
	t.Run("window", func(t *testing.T) {
		b := _WinBlock{topicHash: 0xabcdef, next: 4096, cutoffTime: 1_700_000_000, entryIdx: 2}
		b.entries[0] = newWinEntry(5, 0)
		b.entries[1] = newWinEntry(6, 1_800_000_000)
		raw := golden(t, "window-block", b.marshalBinary())
		if len(raw) != 4096 || u64(raw, 0) != 5 || u64(raw, 12) != 6 || u32(raw, 20) != 1_800_000_000 || u64(raw, 4020) != 1_700_000_000 || u64(raw, 4028) != 0xabcdef || u64(raw, 4036) != 4096 || u16(raw, 4044) != 2 || !matchesChecksum(raw, 4046) {
			t.Errorf("window block fields not at their offsets")
		}
		var got _WinBlock
		got.unmarshalBinary(raw)
		if got.entries != b.entries || got.topicHash != b.topicHash || got.next != b.next || got.cutoffTime != b.cutoffTime || got.entryIdx != 2 {
			t.Errorf("decoded %+v", got)
		}
	})
	t.Run("entry", func(t *testing.T) {
		e := _Entry{seq: 77, topicSize: 11, valueSize: 300, expiresAt: 1_800_000_000, topicHash: 0x1122334455667788}
		enc, _ := e.MarshalBinary()
		raw := golden(t, "entry", enc)
		if len(raw) != 26 || u64(raw, 0) != 77 || u16(raw, 8) != 11 || u32(raw, 10) != 300 || u32(raw, 14) != 1_800_000_000 || u64(raw, 18) != 0x1122334455667788 {
			t.Errorf("entry fields not at their offsets")
		}
		var got _Entry
		got.UnmarshalBinary(raw)
		if got.seq != e.seq || got.topicSize != e.topicSize || got.valueSize != e.valueSize || got.expiresAt != e.expiresAt || got.topicHash != e.topicHash {
			t.Errorf("decoded %+v; want %+v", got, e)
		}
	})
	t.Run("topic", func(t *testing.T) {
		top := message.Topic{Depth: 2, Parts: []message.Part{{Hash: 0x01020304}, {Hash: 0x05060708, Wildchars: 1}}}
		raw := golden(t, "topic", top.Marshal())
		if len(raw) != 11 || raw[0] != 2 || raw[1] != 0 || u32(raw, 2) != 0x01020304 || raw[6] != 1 || u32(raw, 7) != 0x05060708 {
			t.Errorf("topic fields not at their offsets: % x", raw)
		}
		var got message.Topic
		if err := got.Unmarshal(raw); err != nil || got.Depth != 2 || len(got.Parts) != 2 || got.Parts[1] != top.Parts[1] {
			t.Errorf("decoded %+v, %v", got, err)
		}
	})
	t.Run("topic-record", func(t *testing.T) {
		dir := t.TempDir()
		f, err := newFile(dir, 1, _FileDesc{fileType: typeTopics})
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		ts := newTopicNames(f)
		name := []byte{1, 0, 4, 3, 2, 1}
		if err := ts.name(0x0102030405060708, name, true); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(filePath(dir, _FileDesc{fileType: typeTopics}))
		if err != nil {
			t.Fatal(err)
		}
		raw = golden(t, "topic-record", raw)
		if len(raw) != 4+8+6+4 || u32(raw, 0) != 14 || u64(raw, 4) != 0x0102030405060708 || !bytes.Equal(raw[12:18], name) || !matchesChecksum(raw, 18) {
			t.Errorf("topic record fields not at their offsets: % x", raw)
		}
	})
	t.Run("free-list", func(t *testing.T) {
		fb := _FreeBlocks{fb: []_FreeBlock{{offset: 4096, size: 64}, {offset: 10000, size: 300}}}
		raw := golden(t, "free-blocks", fb.MarshalBinary())
		if len(raw) != 4+24 || u32(raw, 0) != 2 || u64(raw, 4) != 4096 || u32(raw, 12) != 64 || u64(raw, 16) != 10000 || u32(raw, 24) != 300 {
			t.Errorf("free list fields not at their offsets: % x", raw)
		}
		var got _FreeBlocks
		got.UnmarshalBinary(raw[4:], u32(raw, 0))
		if len(got.fb) != 2 || got.fb[0] != fb.fb[0] || got.fb[1] != fb.fb[1] {
			t.Errorf("decoded %+v", got.fb)
		}
	})
}
