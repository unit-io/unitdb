package adapter

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/unit-io/unitdb/memdb"

	dbapi "github.com/unit-io/unitdb/server/internal/db"
)

// A copy of a running store's files is not a backup: they are copied at
// different moments, and a copy taken while the DB syncs can pair a new index
// with old data. memdb writes its log in the background, so its files miss
// the latest records, and can hold one half written. A volume snapshot is
// like a power cut, which unitdb's recovery isn't tested for: the WAL isn't
// fsynced.
//
// Checkpoint copies the store between writes instead. It holds every write
// back, and waits until the DB can write out the latest: the DB keeps a new
// entry in memory until its time block has passed and its log has been
// committed, about 1.1s with unitdb's defaults (a 1s block, a 100ms commit
// interval), which the adapter uses. Then it syncs the DB and copies its
// files, and copies memdb's records, not its files, into a fresh memdb in
// dst, and fsyncs the copy. Reads go on throughout; writes wait, up to
// settleTime. The copy opens with db_path set to dst, as after a clean
// shutdown. Records sealed at rest stay sealed: it opens with the same
// keyring only.

// settleTime is how long after its last write the DB has written everything
// out, with unitdb's default block duration and commit interval.
const settleTime = 1500 * time.Millisecond

// Checkpoint writes a copy of the store into dst, which must not exist or be
// empty.
func (a *adapter) Checkpoint(dst string) error {
	fail := func(what string, err error) error {
		return fmt.Errorf("store checkpoint: %s: %v", what, err)
	}
	if a.db == nil {
		return fail("the store", fmt.Errorf("is not open"))
	}
	if entries, err := os.ReadDir(dst); err == nil && len(entries) > 0 {
		return fail(dst, fmt.Errorf("is not empty"))
	}
	if err := os.MkdirAll(dst, 0700); err != nil {
		return fail("make "+dst, err)
	}

	a.wmu.Lock()
	defer a.wmu.Unlock()

	if wait := settleTime - time.Since(time.Unix(0, a.lastWrite.Load())); wait > 0 {
		time.Sleep(wait)
	}
	// Twice: an entry whose log was committed during the first sync.
	for i := 0; i < 2; i++ {
		if err := a.db.Sync(); err != nil {
			return fail("sync", err)
		}
	}
	src := filepath.Join(a.path, defaultDatabase)
	if err := copyTree(src, filepath.Join(dst, defaultDatabase)); err != nil {
		return fail("copy the DB", err)
	}

	var opts []memdb.Options
	opts = append(opts, memdb.WithLogFilePath(dst))
	if a.config != nil && a.config.Size > 0 {
		opts = append(opts, memdb.WithBufferSize(a.config.Size))
	}
	mem, err := memdb.Open(opts...)
	if err != nil {
		return fail("open the copy's memdb", err)
	}
	for _, key := range a.Keys() {
		b, err := a.mem.Get(key)
		if err != nil {
			continue // deleted since Keys listed it
		}
		if _, err := mem.Put(key, b); err != nil {
			mem.Close()
			return fail("copy memdb", err)
		}
	}
	if err := mem.Close(); err != nil {
		return fail("close the copy's memdb", err)
	}
	if err := syncTree(dst); err != nil {
		return fail("fsync the copy", err)
	}
	return nil
}

// Stats returns the size of the store.
func (a *adapter) Stats() dbapi.Stats {
	var s dbapi.Stats
	if a.db == nil {
		return s
	}
	s.Messages = a.db.Count()
	s.DiskBytes, _ = a.db.FileSize()
	if a.mem != nil {
		s.MemEntries = a.mem.Size()
	}
	if a.config != nil {
		s.MemSize = a.config.Size
	}
	return s
}

// copyTree copies the files under src to dst, but for unitdb's lock file,
// which the copy's DB makes its own.
func copyTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		switch {
		case info.IsDir():
			return os.MkdirAll(target, 0700)
		case filepath.Ext(path) == ".lock":
			return nil
		case !info.Mode().IsRegular():
			return nil
		}
		return copyFile(path, target, info.Mode().Perm())
	})
}

func copyFile(src, dst string, perm os.FileMode) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, perm)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// syncTree fsyncs every file and directory under dir.
func syncTree(dir string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		return f.Sync()
	})
}
