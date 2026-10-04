package backup

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"filippo.io/age"
	"github.com/klauspost/compress/zstd"
)

// ParseRecipient parses the backup key's public half.
func ParseRecipient(s string) (age.Recipient, error) {
	r, err := age.ParseX25519Recipient(strings.TrimSpace(s))
	if err != nil {
		return nil, fmt.Errorf("backup: the backup key's public half: %v", err)
	}
	return r, nil
}

// ParseIdentity parses the backup key's private half, as age-keygen writes
// it (comment lines are skipped).
func ParseIdentity(s string) (age.Identity, error) {
	ids, err := age.ParseIdentities(strings.NewReader(s))
	if err != nil || len(ids) == 0 {
		return nil, fmt.Errorf("backup: the backup key's private half: %v", err)
	}
	return ids[0], nil
}

// WriteArchive writes the files under dir to w as a tar archive, compressed
// with zstd and encrypted to rcpt. Only regular files and directories go in.
func WriteArchive(w io.Writer, dir string, rcpt age.Recipient) error {
	enc, err := age.Encrypt(w, rcpt)
	if err != nil {
		return err
	}
	zw, err := zstd.NewWriter(enc)
	if err != nil {
		return err
	}
	tw := tar.NewWriter(zw)
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil || rel == "." {
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		hdr.Name = filepath.ToSlash(rel)
		if info.IsDir() {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	if err != nil {
		return err
	}
	if err := tw.Close(); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	return enc.Close()
}

// ExtractArchive decrypts r with id and extracts it into dst, which must
// not exist or be empty. A name that would land outside dst is refused.
func ExtractArchive(r io.Reader, id age.Identity, dst string) error {
	if entries, err := os.ReadDir(dst); err == nil && len(entries) > 0 {
		return fmt.Errorf("backup: %s is not empty", dst)
	}
	if err := os.MkdirAll(dst, 0700); err != nil {
		return err
	}
	dec, err := age.Decrypt(r, id)
	if err != nil {
		return fmt.Errorf("backup: decrypt: %v", err)
	}
	zr, err := zstd.NewReader(dec)
	if err != nil {
		return err
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("backup: archive: %v", err)
		}
		target := filepath.Join(dst, filepath.FromSlash(hdr.Name))
		if !strings.HasPrefix(target, filepath.Clean(dst)+string(os.PathSeparator)) {
			return fmt.Errorf("backup: archive: %q is outside it", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0700); err != nil {
				return err
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
				return err
			}
			f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, os.FileMode(hdr.Mode).Perm()|0600)
			if err != nil {
				return err
			}
			if _, err := io.Copy(f, tr); err != nil {
				f.Close()
				return err
			}
			if err := f.Close(); err != nil {
				return err
			}
		}
	}
}
