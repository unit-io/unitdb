package tracedb

import (
	"encoding/binary"
	"time"

	"github.com/allegro/bigcache"
	"github.com/saffat-in/tracedb/fs"
)

type entry struct {
	hash      uint32
	topicSize uint16
	valueSize uint32
	expiresAt uint32
	kvOffset  int64
}

func (e entry) timeStamp() uint32 {
	return e.expiresAt
}

func (e entry) isExpired() bool {
	return e.expiresAt != 0 && e.expiresAt <= uint32(time.Now().Unix())
}

func (e entry) kvSize() uint32 {
	return uint32(e.topicSize) + keySize + e.valueSize
}

type block struct {
	entries [entriesPerBlock]entry
	next    int64
}

type blockHandle struct {
	block
	file   fs.FileManager
	offset int64

	updated bool
	cache   *bigcache.BigCache
	cacheID uint64
}

const (
	blockSize uint32 = 512
)

func align512(n uint32) uint32 {
	return (n + 511) &^ 511
}

func (b block) MarshalBinary() ([]byte, error) {
	buf := make([]byte, blockSize)
	data := buf
	for i := 0; i < entriesPerBlock; i++ {
		e := b.entries[i]
		binary.LittleEndian.PutUint32(buf[:4], e.hash)
		binary.LittleEndian.PutUint16(buf[4:6], e.topicSize)
		binary.LittleEndian.PutUint32(buf[6:10], e.valueSize)
		binary.LittleEndian.PutUint32(buf[10:14], e.expiresAt)
		binary.LittleEndian.PutUint64(buf[14:22], uint64(e.kvOffset))
		buf = buf[22:]
	}
	binary.LittleEndian.PutUint64(buf[:8], uint64(b.next))
	return data, nil
}

func (b *block) UnmarshalBinary(data []byte) error {
	for i := 0; i < entriesPerBlock; i++ {
		_ = data[22] // bounds check hint to compiler; see golang.org/issue/14808
		b.entries[i].hash = binary.LittleEndian.Uint32(data[:4])
		b.entries[i].topicSize = binary.LittleEndian.Uint16(data[4:6])
		b.entries[i].valueSize = binary.LittleEndian.Uint32(data[6:10])
		b.entries[i].expiresAt = binary.LittleEndian.Uint32(data[10:14])
		b.entries[i].kvOffset = int64(binary.LittleEndian.Uint64(data[14:22]))
		data = data[22:]
	}
	b.next = int64(binary.LittleEndian.Uint64(data[:8]))
	return nil
}

func (b *block) del(entryIdx int) {
	i := entryIdx
	for ; i < entriesPerBlock-1; i++ {
		b.entries[i] = b.entries[i+1]
	}
	b.entries[i] = entry{}
}

func (bh *blockHandle) read(fillCache bool) error {

	var cacheKey string
	if bh.cache != nil {
		var kb [8]byte
		binary.LittleEndian.PutUint64(kb[:8], bh.cacheID^uint64(bh.offset))
		cacheKey = string(kb[:])

		if data, _ := bh.cache.Get(cacheKey); data != nil && len(data) == int(blockSize) {
			return bh.UnmarshalBinary(data)
		}
	}

	buf, err := bh.file.Slice(bh.offset, bh.offset+int64(blockSize))
	if err != nil {
		return err
	}
	if bh.cache != nil && fillCache {
		bh.cache.Set(cacheKey, buf)
	}
	return bh.UnmarshalBinary(buf)
}

func (bh *blockHandle) write() error {
	buf, err := bh.MarshalBinary()
	if err != nil {
		return err
	}
	_, err = bh.file.WriteAt(buf, bh.offset)
	if bh.cache != nil {
		var kb [8]byte
		binary.LittleEndian.PutUint64(kb[:8], bh.cacheID^uint64(bh.offset))
		cacheKey := string(kb[:])
		bh.cache.Delete(cacheKey)
	}
	return err
}

type entryWriter struct {
	block      *blockHandle
	entryIdx   int
	prevblocks []*blockHandle
}

func (ew *entryWriter) insert(e entry, db *DB) error {
	if ew.entryIdx == entriesPerBlock {
		nextblock, err := db.createOverflowBlock()
		if err != nil {
			return err
		}
		ew.block.next = nextblock.offset
		ew.prevblocks = append(ew.prevblocks, ew.block)
		ew.block = nextblock
		ew.entryIdx = 0
	}
	ew.block.entries[ew.entryIdx] = e
	ew.entryIdx++
	return nil
}

func (ew *entryWriter) write() error {
	for i := len(ew.prevblocks) - 1; i >= 0; i-- {
		if err := ew.prevblocks[i].write(); err != nil {
			return err
		}
	}
	return ew.block.write()
}
