package unitdb

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/unit-io/unitdb/message"
)

// FuzzDecode decodes the fuzzed bytes as each block and record the DB
// reads from disk: decoding must not panic, and a block decoded encodes to
// bytes that decode to it again.
func FuzzDecode(f *testing.F) {
	var ib _IndexBlock
	ib.entries[0] = _IndexEntry{seq: 1, topicSize: 3, valueSize: 5, msgOffset: 4096}
	ib.entryIdx = 1
	f.Add(ib.marshalBinary())
	wb := _WinBlock{topicHash: 7, entryIdx: 1}
	wb.entries[0] = newWinEntry(1, 0)
	f.Add(wb.marshalBinary())
	info, _ := _DBInfo{header: _Header{signature: signature, version: version}}.MarshalBinary()
	f.Add(info)
	f.Add((&message.Topic{Depth: 2, Parts: []message.Part{{Hash: 1}, {Hash: 2}}}).Marshal())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		block := make([]byte, blockSize)
		copy(block, data)

		var b _IndexBlock
		if err := b.unmarshalBinary(block); err == nil {
			var again _IndexBlock
			again.unmarshalBinary(b.marshalBinary())
			// Sequences are stored relative to the first's, and a block holds
			// those of one block index: others, in a corrupt block, don't
			// survive encoding. Readers find no entry by them; Verify reports
			// them.
			for i := range b.entries {
				x, y := b.entries[i], again.entries[i]
				if base := b.entries[0].seq; x.seq != 0 && (base == 0 || (x.seq-1)/entriesPerIndexBlock != (base-1)/entriesPerIndexBlock) {
					continue
				}
				if x.seq != y.seq || x.topicSize != y.topicSize || x.valueSize != y.valueSize || x.msgOffset != y.msgOffset {
					t.Fatalf("index block: entry %d decoded, encoded and decoded again is %+v; was %+v", i, y, x)
				}
			}
			if again.entryIdx != b.entryIdx {
				t.Errorf("index block: decoded, encoded and decoded again, it differs")
			}
		}
		var w _WinBlock
		if err := w.unmarshalBinary(block); err == nil {
			var again _WinBlock
			again.unmarshalBinary(w.marshalBinary())
			if again.entries != w.entries || again.topicHash != w.topicHash || again.next != w.next || again.entryIdx != w.entryIdx || again.cutoffTime != w.cutoffTime {
				t.Errorf("window block: decoded, encoded and decoded again, it differs")
			}
		}

		var inf _DBInfo
		inf.UnmarshalBinary(block[:fixed])
		inf.UnmarshalBinary(block[:fixedV2])
		var e _Entry
		e.UnmarshalBinary(block[:entrySize])
		var top message.Topic
		top.Unmarshal(data)
		for off := 0; off < len(data); {
			_, _, next, ok := topicRecord(data, off)
			if !ok || next <= off {
				break
			}
			off = next
		}
	})
}

// fuzzTemplate is a DB with entries on disk and in the WAL, made once and
// copied for each run of FuzzOpenCorrupt.
var fuzzTemplate struct {
	once sync.Once
	dir  string
	err  error
}

var fuzzTopics = [][]byte{[]byte("unit.fuzz.a"), []byte("unit.fuzz.b.c")}

func fuzzTemplateDir() (string, error) {
	fuzzTemplate.once.Do(func() {
		dir, err := os.MkdirTemp("", "unitdb-fuzz-")
		if err != nil {
			fuzzTemplate.err = err
			return
		}
		fuzzTemplate.dir = dir
		db, err := Open(dir, WithMutable(), WithBufferSize(1<<16), WithMemdbSize(1<<20))
		if err != nil {
			fuzzTemplate.err = err
			return
		}
		var ids [][]byte
		for i := 0; i < 600; i++ {
			id := db.NewID()
			if err := db.PutEntry(NewEntry(fuzzTopics[i%2], []byte(fmt.Sprintf("m%d", i))).WithID(id)); err != nil {
				fuzzTemplate.err = err
				return
			}
			ids = append(ids, id)
			if i == 500 {
				// Most on disk, the rest in the WAL only.
				for db.Count() < 400 {
					if err := db.Sync(); err != nil {
						fuzzTemplate.err = err
						return
					}
				}
			}
		}
		for i := 3; i < 600; i += 7 {
			db.Delete(ids[i], fuzzTopics[i%2])
		}
		db.Flush()
		fuzzTemplate.err = db.Close()
	})
	return fuzzTemplate.dir, fuzzTemplate.err
}

func copyDir(t *testing.T, from string) string {
	to := t.TempDir()
	err := filepath.Walk(from, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		if fi.IsDir() {
			return os.MkdirAll(filepath.Join(to, rel), 0755)
		}
		if filepath.Ext(path) == lockPostfix {
			return nil
		}
		src, err := os.Open(path)
		if err != nil {
			return err
		}
		defer src.Close()
		dst, err := os.Create(filepath.Join(to, rel))
		if err != nil {
			return err
		}
		defer dst.Close()
		_, err = io.Copy(dst, src)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return to
}

// FuzzOpenCorrupt writes the fuzzed bytes into a file of a DB, at a fuzzed
// offset, and puts back the checksum of the block or file written to: what
// a bug that writes bad data leaves. Opening the DB may fail; a DB that
// opens must answer queries, Verify, writes and syncs without panicking.
func FuzzOpenCorrupt(f *testing.F) {
	f.Add(uint8(0), uint32(0), []byte{0xff, 0xff, 0xff, 0xff})
	f.Add(uint8(1), uint32(8), []byte{1, 0, 2, 0, 0, 0, 0, 0})
	f.Add(uint8(2), uint32(12), []byte{0, 0, 0, 0, 0, 0, 0, 0x80})
	f.Add(uint8(3), uint32(4), []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f})
	f.Add(uint8(4), uint32(0), []byte{9})
	f.Fuzz(func(t *testing.T, which uint8, off uint32, data []byte) {
		tmpl, err := fuzzTemplateDir()
		if err != nil {
			t.Fatal(err)
		}
		dir := copyDir(t, tmpl)
		kinds := []_FileDesc{{fileType: typeIndex}, {fileType: typeTimeWindow}, {fileType: typeInfo}, {fileType: typeLease}, {fileType: typeData}}
		fd := kinds[int(which)%len(kinds)]
		path := filePath(dir, fd)
		raw, err := os.ReadFile(path)
		if err != nil || len(raw) == 0 {
			return
		}
		at := int(off) % len(raw)
		raw = append(raw[:at:at], append(append([]byte{}, data...), raw[min(len(raw), at+len(data)):]...)...)
		restamp(fd.fileType, raw)
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}

		db, err := Open(dir, WithMutable(), WithBufferSize(1<<16), WithMemdbSize(1<<20))
		if err != nil {
			return
		}
		defer db.Close()
		for _, topic := range fuzzTopics {
			db.Get(NewQuery(topic).WithLimit(1000))
		}
		db.Verify()
		db.Put(fuzzTopics[0], []byte("after"))
		db.Sync()
	})
}

// restamp puts back the checksums of the file's blocks, or of the file.
func restamp(ft _FileType, raw []byte) {
	switch ft {
	case typeIndex, typeTimeWindow:
		sumOff := indexChecksumOff
		if ft == typeTimeWindow {
			sumOff = windowChecksumOff
		}
		for off := 0; off+int(blockSize) <= len(raw); off += int(blockSize) {
			putChecksum(raw[off:off+int(blockSize)], sumOff)
		}
	case typeInfo:
		if len(raw) >= int(fixed) {
			putChecksum(raw[:fixed], infoChecksumOff)
		}
	case typeLease:
		if len(raw) > checksumSize {
			putChecksum(raw, len(raw)-checksumSize)
		}
	}
}

// TestMain removes the fuzz template, which outlives the tests that use it.
func TestMain(m *testing.M) {
	code := m.Run()
	if fuzzTemplate.dir != "" {
		os.RemoveAll(fuzzTemplate.dir)
	}
	os.Exit(code)
}
