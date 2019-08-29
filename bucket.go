package tracedb

import (
	"encoding/binary"

	"github.com/ayaz-mumtaz/dev18/db/kv/fs"
)

type entry struct {
	hash      uint32
	keySize   uint16
	valueSize uint32
	expiresAt uint32
	kvOffset  int64
}

func (sl entry) kvSize() uint32 {
	return uint32(sl.keySize) + sl.valueSize
}

type bucket struct {
	entries [entriesPerBucket]entry
	next    int64
}

type bucketHandle struct {
	bucket
	file   fs.FileManager
	offset int64
}

const (
	bucketSize uint32 = 512
)

func align512(n uint32) uint32 {
	return (n + 511) &^ 511
}

func (b bucket) MarshalBinary() ([]byte, error) {
	buf := make([]byte, bucketSize)
	data := buf
	for i := 0; i < entriesPerBucket; i++ {
		sl := b.entries[i]
		binary.LittleEndian.PutUint32(buf[:4], sl.hash)
		binary.LittleEndian.PutUint16(buf[4:6], sl.keySize)
		binary.LittleEndian.PutUint32(buf[6:10], sl.valueSize)
		binary.LittleEndian.PutUint32(buf[10:14], sl.expiresAt)
		binary.LittleEndian.PutUint64(buf[14:22], uint64(sl.kvOffset))
		buf = buf[22:]
	}
	binary.LittleEndian.PutUint64(buf[:8], uint64(b.next))
	return data, nil
}

func (b *bucket) UnmarshalBinary(data []byte) error {
	for i := 0; i < entriesPerBucket; i++ {
		_ = data[22] // bounds check hint to compiler; see golang.org/issue/14808
		b.entries[i].hash = binary.LittleEndian.Uint32(data[:4])
		b.entries[i].keySize = binary.LittleEndian.Uint16(data[4:6])
		b.entries[i].valueSize = binary.LittleEndian.Uint32(data[6:10])
		b.entries[i].expiresAt = binary.LittleEndian.Uint32(data[10:14])
		b.entries[i].kvOffset = int64(binary.LittleEndian.Uint64(data[14:22]))
		data = data[22:]
	}
	b.next = int64(binary.LittleEndian.Uint64(data[:8]))
	return nil
}

func (b *bucket) del(entryIdx int) {
	i := entryIdx
	for ; i < entriesPerBucket-1; i++ {
		b.entries[i] = b.entries[i+1]
	}
	b.entries[i] = entry{}
}

func (b *bucketHandle) read() error {
	buf, err := b.file.Slice(b.offset, b.offset+int64(bucketSize))
	if err != nil {
		return err
	}
	return b.UnmarshalBinary(buf)
}

func (b *bucketHandle) write() error {
	buf, err := b.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = b.file.WriteAt(buf, b.offset)
	return err
}

type entryWriter struct {
	bucket      *bucketHandle
	entryIdx    int
	prevBuckets []*bucketHandle
}

func (sw *entryWriter) insert(sl entry, db *DB) error {
	if sw.entryIdx == entriesPerBucket {
		nextBucket, err := db.createOverflowBucket()
		if err != nil {
			return err
		}
		sw.bucket.next = nextBucket.offset
		sw.prevBuckets = append(sw.prevBuckets, sw.bucket)
		sw.bucket = nextBucket
		sw.entryIdx = 0
	}
	sw.bucket.entries[sw.entryIdx] = sl
	sw.entryIdx++
	return nil
}

func (sw *entryWriter) write() error {
	for i := len(sw.prevBuckets) - 1; i >= 0; i-- {
		if err := sw.prevBuckets[i].write(); err != nil {
			return err
		}
	}
	return sw.bucket.write()
}
