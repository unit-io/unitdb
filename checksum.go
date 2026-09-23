/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package unitdb

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sort"
)

// Since format version 2 every index block, window block and the info header
// carries a CRC32C in its spare bytes, and every message's CRC32C is stored in
// the checksum file at offset seq*checksumSize.
const (
	checksumSize = 4

	infoChecksumOff   = 28                                       // after count
	indexChecksumOff  = 8 + entriesPerIndexBlock*16 + 2          // after entryIdx
	windowChecksumOff = entriesPerWindowBlock*12 + 8 + 8 + 8 + 2 // after entryIdx
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

func checksum(b []byte) uint32 {
	return crc32.Checksum(b, crcTable)
}

// putChecksum stores the checksum of b[:off] at b[off:].
func putChecksum(b []byte, off int) {
	binary.LittleEndian.PutUint32(b[off:off+checksumSize], checksum(b[:off]))
}

// matchesChecksum reports whether b[:off] matches the checksum at b[off:].
func matchesChecksum(b []byte, off int) bool {
	return binary.LittleEndian.Uint32(b[off:off+checksumSize]) == checksum(b[:off])
}

// validChecksum is matchesChecksum for blocks: an all-zero block, allocated
// but never written, is also valid.
func validChecksum(b []byte, off int) bool {
	if matchesChecksum(b, off) {
		return true
	}
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// corrupted returns an errCorrupted naming what failed and where.
func corrupted(f *_File, off int64, what string) error {
	name := ""
	if f != nil && f.File != nil {
		name = f.Name()
	}
	return fmt.Errorf("%w: %s checksum mismatch in %s at offset %d", errCorrupted, what, name, off)
}

type _MessageChecksum struct {
	seq uint64
	sum uint32
}

// writeMessageChecksums writes message checksums, one WriteAt per run of
// consecutive sequences so existing checksums between runs are untouched.
func writeMessageChecksums(f *_File, sums []_MessageChecksum) error {
	sort.Slice(sums, func(i, j int) bool { return sums[i].seq < sums[j].seq })
	for i := 0; i < len(sums); {
		j := i + 1
		for j < len(sums) && sums[j].seq == sums[j-1].seq+1 {
			j++
		}
		buf := make([]byte, checksumSize*(j-i))
		for k := i; k < j; k++ {
			binary.LittleEndian.PutUint32(buf[checksumSize*(k-i):], sums[k].sum)
		}
		if _, err := f.WriteAt(buf, int64(sums[i].seq)*checksumSize); err != nil {
			return err
		}
		i = j
	}
	return nil
}

// verifyMessage checks message, the bytes of the entry for seq in the data file.
func verifyMessage(sumFile, dataFile *_File, seq uint64, off int64, message []byte) error {
	buf := make([]byte, checksumSize)
	if _, err := sumFile.ReadAt(buf, int64(seq)*checksumSize); err != nil {
		return corrupted(dataFile, off, fmt.Sprintf("message %d (no checksum: %v)", seq, err))
	}
	if binary.LittleEndian.Uint32(buf) != checksum(message) {
		return corrupted(dataFile, off, fmt.Sprintf("message %d", seq))
	}
	return nil
}

// checkFiles verifies the DB files when the DB is opened, or adds checksums
// to a format 1 DB, which has none.
func (db *DB) checkFiles() error {
	info := db.internal.dbInfo
	switch v := info.header.version; {
	case v > version:
		return fmt.Errorf("%w: unsupported file format version %d", errCorrupted, v)
	case v < 2:
		return db.upgradeChecksums()
	case !info.validChecksum:
		return corrupted(db.internal.info._File, 0, "info header")
	}
	return db.verifyFiles()
}

// forEachBlock calls fn with each whole block of f.
func forEachBlock(f *_File, fn func(off int64, buf []byte) error) error {
	size := f.currSize()
	for off := int64(0); off+int64(blockSize) <= size; off += int64(blockSize) {
		buf, err := f.slice(off, off+int64(blockSize))
		if err != nil {
			return err
		}
		if err := fn(off, buf); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) files() (win, index, data, sum *_File, err error) {
	if win, err = db.fs.getFile(_FileDesc{fileType: typeTimeWindow}); err != nil {
		return
	}
	if index, err = db.fs.getFile(_FileDesc{fileType: typeIndex}); err != nil {
		return
	}
	if data, err = db.fs.getFile(_FileDesc{fileType: typeData}); err != nil {
		return
	}
	sum, err = db.fs.getFile(_FileDesc{fileType: typeChecksum})
	return
}

// verifyFiles checks every window block, index block and live message.
func (db *DB) verifyFiles() error {
	winFile, indexFile, dataFile, sumFile, err := db.files()
	if err != nil {
		return err
	}
	if err := forEachBlock(winFile, func(off int64, buf []byte) error {
		if !validChecksum(buf, windowChecksumOff) {
			return corrupted(winFile, off, "window block")
		}
		return nil
	}); err != nil {
		return err
	}
	return forEachBlock(indexFile, func(off int64, buf []byte) error {
		if !validChecksum(buf, indexChecksumOff) {
			return corrupted(indexFile, off, "index block")
		}
		var b _IndexBlock
		if err := b.unmarshalBinary(buf); err != nil {
			return err
		}
		for _, e := range b.entries {
			if e.seq == 0 || e.deleted() {
				continue
			}
			message, err := dataFile.slice(e.msgOffset, e.msgOffset+int64(e.mSize()))
			if err != nil {
				return corrupted(dataFile, e.msgOffset, fmt.Sprintf("message %d (unreadable: %v)", e.seq, err))
			}
			if err := verifyMessage(sumFile, dataFile, e.seq, e.msgOffset, message); err != nil {
				return err
			}
		}
		return nil
	})
}

// upgradeChecksums adds checksums to the blocks and messages of a format 1
// DB, then stamps it format 2. It is idempotent, so a crash part way through
// just redoes it on the next open.
func (db *DB) upgradeChecksums() error {
	winFile, indexFile, dataFile, sumFile, err := db.files()
	if err != nil {
		return err
	}
	if err := forEachBlock(winFile, func(off int64, buf []byte) error {
		var b _WinBlock
		if err := b.unmarshalBinary(buf); err != nil {
			return err
		}
		_, err := winFile.WriteAt(b.marshalBinary(), off)
		return err
	}); err != nil {
		return err
	}
	if err := forEachBlock(indexFile, func(off int64, buf []byte) error {
		var b _IndexBlock
		if err := b.unmarshalBinary(buf); err != nil {
			return err
		}
		if b.entryIdx == 0 && b.entries[0].seq == 0 {
			return nil // never written
		}
		var sums []_MessageChecksum
		for _, e := range b.entries {
			if e.seq == 0 || e.deleted() {
				continue
			}
			message, err := dataFile.slice(e.msgOffset, e.msgOffset+int64(e.mSize()))
			if err != nil {
				return err
			}
			sums = append(sums, _MessageChecksum{seq: e.seq, sum: checksum(message)})
		}
		if err := writeMessageChecksums(sumFile, sums); err != nil {
			return err
		}
		_, err := indexFile.WriteAt(b.marshalBinary(), off)
		return err
	}); err != nil {
		return err
	}
	if err := db.fs.sync(); err != nil {
		return err
	}
	db.internal.dbInfo.header.version = version
	return db.writeInfo()
}
