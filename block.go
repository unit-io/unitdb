package tracedb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/unit-io/bpool"
	"github.com/unit-io/tracedb/fs"
)

const (
	entrySize        = 26
	blockSize uint32 = 4096
)

type (
	entry struct {
		seq       uint64
		topicSize uint16
		valueSize uint32
		expiresAt uint32
		msgOffset int64

		// topicOffset int64
		cacheBlock []byte
	}

	block struct {
		entries  [entriesPerIndexBlock]entry
		next     uint32
		entryIdx uint16

		leased bool
	}

	blockHandle struct {
		block
		file   fs.FileManager
		offset int64
	}

	blockWriter struct {
		blocks map[int32]block // map[blockIdx]block

		*file
		buffer  *bpool.Buffer
		nBlocks int // non-leased block count
	}
)

func newBlockWriter(f *file, buf *bpool.Buffer) *blockWriter {
	return &blockWriter{blocks: make(map[int32]block), file: f, buffer: buf}
}

func (e entry) time() uint32 {
	return e.expiresAt
}

func startBlockIndex(seq uint64) int32 {
	return int32(float64(seq-1) / float64(entriesPerIndexBlock))
}

func blockOffset(idx int32) int64 {
	if idx == -1 {
		return int64(headerSize)
	}
	return int64(headerSize) + (int64(blockSize) * int64(idx))
}

func (e entry) Seq() uint64 {
	return e.seq
}

func (e entry) isExpired() bool {
	return e.expiresAt != 0 && e.expiresAt <= uint32(time.Now().Unix())
}

func (e entry) mSize() uint32 {
	return idSize + uint32(e.topicSize) + e.valueSize
}

// MarshalBinary serialized entry into binary data
func (e entry) MarshalBinary() ([]byte, error) {
	buf := make([]byte, entrySize)
	data := buf
	binary.LittleEndian.PutUint64(buf[:8], e.seq)
	binary.LittleEndian.PutUint16(buf[8:10], e.topicSize)
	binary.LittleEndian.PutUint32(buf[10:14], e.valueSize)
	binary.LittleEndian.PutUint32(buf[14:18], e.expiresAt)
	return data, nil
}

// MarshalBinary de-serialized entry from binary data
func (e *entry) UnmarshalBinary(data []byte) error {
	e.seq = binary.LittleEndian.Uint64(data[:8])
	e.topicSize = binary.LittleEndian.Uint16(data[8:10])
	e.valueSize = binary.LittleEndian.Uint32(data[10:14])
	e.expiresAt = binary.LittleEndian.Uint32(data[14:18])
	return nil
}

func (b block) validation(blockIdx int32) error {
	startBlockIdx := startBlockIndex(b.entries[0].seq)
	if startBlockIdx != blockIdx {
		return errors.New("block.write: validation failed")
	}
	return nil
}

// MarshalBinary serialized entries block into binary data
func (b block) MarshalBinary() []byte {
	buf := make([]byte, blockSize)
	data := buf
	for i := 0; i < entriesPerIndexBlock; i++ {
		e := b.entries[i]
		binary.LittleEndian.PutUint64(buf[:8], e.seq)
		binary.LittleEndian.PutUint16(buf[8:10], e.topicSize)
		binary.LittleEndian.PutUint32(buf[10:14], e.valueSize)
		binary.LittleEndian.PutUint32(buf[14:18], e.expiresAt)
		binary.LittleEndian.PutUint64(buf[18:26], uint64(e.msgOffset))
		buf = buf[entrySize:]
	}
	binary.LittleEndian.PutUint32(buf[:4], b.next)
	binary.LittleEndian.PutUint16(buf[4:6], b.entryIdx)
	return data
}

// UnmarshalBinary de-serialized entries block from binary data
func (b *block) UnmarshalBinary(data []byte) error {
	for i := 0; i < entriesPerIndexBlock; i++ {
		_ = data[entrySize] // bounds check hint to compiler; see golang.org/issue/14808
		b.entries[i].seq = binary.LittleEndian.Uint64(data[:8])
		b.entries[i].topicSize = binary.LittleEndian.Uint16(data[8:10])
		b.entries[i].valueSize = binary.LittleEndian.Uint32(data[10:14])
		b.entries[i].expiresAt = binary.LittleEndian.Uint32(data[14:18])
		b.entries[i].msgOffset = int64(binary.LittleEndian.Uint64(data[18:26]))
		data = data[entrySize:]
	}
	b.next = binary.LittleEndian.Uint32(data[:4])
	b.entryIdx = binary.LittleEndian.Uint16(data[4:6])
	return nil
}

func (bh *blockHandle) read() error {
	buf, err := bh.file.Slice(bh.offset, bh.offset+int64(blockSize))
	if err != nil {
		return err
	}
	return bh.UnmarshalBinary(buf)
}

func (bw *blockWriter) del(seq uint64) (entry, error) {
	var delEntry entry
	blockIdx := startBlockIndex(seq)
	off := blockOffset(blockIdx)
	b := blockHandle{file: bw.file, offset: off}
	if err := b.read(); err != nil {
		return delEntry, err
	}
	entryIdx := -1
	for i := 0; i < int(b.entryIdx); i++ {
		e := b.entries[i]
		if e.seq == seq { //record exist in db
			entryIdx = i
			break
		}
	}
	if entryIdx == -1 {
		return delEntry, nil // no entry in db to delete
	}
	delEntry = b.entries[entryIdx]
	b.entryIdx--

	i := entryIdx
	for ; i < entriesPerIndexBlock-1; i++ {
		b.entries[i] = b.entries[i+1]
	}
	b.entries[i] = entry{}

	return delEntry, nil
}

func (bw *blockWriter) append(e entry, blockIdx int32) (exists bool, err error) {
	var b block
	var ok bool
	startBlockIdx := startBlockIndex(e.seq)
	b, ok = bw.blocks[startBlockIdx]
	if !ok {
		if startBlockIdx <= blockIdx {
			off := blockOffset(int32(startBlockIdx))
			bh := blockHandle{file: bw.file, offset: off}
			if err := bh.read(); err != nil {
				return false, err
			}
			b = bh.block
			b.leased = true
		} else {
			bw.nBlocks++
		}
	}
	entryIdx := 0
	for i := 0; i < int(b.entryIdx); i++ {
		if b.entries[i].seq == e.seq { //record exist in db
			entryIdx = -1
			break
		}
	}
	if entryIdx == -1 {
		return true, nil
	}
	b.entries[b.entryIdx] = e
	b.entryIdx++

	bw.blocks[startBlockIdx] = b

	return false, nil
}

func (bw *blockWriter) write() error {
	defer func() {
		bw.nBlocks = 0
		bw.blocks = make(map[int32]block)
		bw.buffer.Reset()
	}()

	for blockIdx, b := range bw.blocks {
		if !b.leased {
			continue
		}
		if err := b.validation(blockIdx); err != nil {
			return err
		}
		off := blockOffset(blockIdx)
		buf := b.MarshalBinary()
		if _, err := bw.WriteAt(buf, off); err != nil {
			return err
		}
		delete(bw.blocks, blockIdx)
	}

	// sort blocks by blockIdx
	var idx []int
	for i := range bw.blocks {
		idx = append(idx, int(i))
	}
	sort.Ints(idx)
	blocks, err := blockRange(idx)
	if err != nil {
		return err
	}
	// fmt.Println("block.write: blocks ", blocks)
	for _, bIdx := range blocks {
		if len(bIdx) == 1 {
			blockIdx := int32(bIdx[0])
			off := blockOffset(blockIdx)
			b := bw.blocks[blockIdx]
			if err := b.validation(blockIdx); err != nil {
				return err
			}
			buf := b.MarshalBinary()
			if _, err := bw.WriteAt(buf, off); err != nil {
				return err
			}
			continue
		}
		blockOff := blockOffset(int32(bIdx[0]))
		bw.buffer.Reset()
		for i := bIdx[0]; i <= bIdx[1]; i++ {
			b := bw.blocks[int32(i)]
			if err := b.validation(int32(i)); err != nil {
				return err
			}
			bw.buffer.Write(b.MarshalBinary())
		}
		if _, err := bw.WriteAt(bw.buffer.Bytes(), blockOff); err != nil {
			return err
		}
	}

	return nil
}

func (bw *blockWriter) Count() int {
	return bw.nBlocks
}

func blockRange(idx []int) ([][]int, error) {
	if len(idx) == 0 {
		return nil, nil
	}
	var parts [][]int
	for n1 := 0; ; {
		n2 := n1 + 1
		for n2 < len(idx) && idx[n2] == idx[n2-1]+1 {
			n2++
		}
		s := []int{(idx[n1])}
		if n2 == n1+2 {
			parts = append(parts, []int{idx[n2-1]})
		} else if n2 > n1+2 {
			s = append(s, idx[n2-1])
		}
		parts = append(parts, s)
		if n2 == len(idx) {
			break
		}
		if idx[n2] == idx[n2-1] {
			return nil, errors.New(fmt.Sprintf(
				"sequence repeats value %d", idx[n2]))
		}
		if idx[n2] < idx[n2-1] {
			return nil, errors.New(fmt.Sprintf(
				"sequence not ordered: %d < %d", idx[n2], idx[n2-1]))
		}
		n1 = n2
	}
	return parts, nil
}
