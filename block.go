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
	}

	leasedBlock struct {
		entries  []entry
		entryIdx uint16
	}

	blockHandle struct {
		block
		file   fs.FileManager
		offset int64
	}

	blockWriter struct {
		blocks map[int32]block       // map[blockIdx]block
		leased map[int32]leasedBlock // map[blockIdx]leasedBlock

		*file
		buffer *bpool.Buffer
		count  int // non-leased block count
	}
)

func newBlockWriter(f *file, buf *bpool.Buffer) *blockWriter {
	return &blockWriter{blocks: make(map[int32]block), leased: make(map[int32]leasedBlock), file: f, buffer: buf}
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
	// binary.LittleEndian.PutUint64(buf[18:26], uint64(e.topicOffset))
	return data, nil
}

// MarshalBinary de-serialized entry from binary data
func (e *entry) UnmarshalBinary(data []byte) error {
	e.seq = binary.LittleEndian.Uint64(data[:8])
	e.topicSize = binary.LittleEndian.Uint16(data[8:10])
	e.valueSize = binary.LittleEndian.Uint32(data[10:14])
	e.expiresAt = binary.LittleEndian.Uint32(data[14:18])
	// e.topicOffset = int64(binary.LittleEndian.Uint64(data[18:26]))
	return nil
}

// func align(n uint32) uint32 {
// 	return (n + 511) &^ 511
// }

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

func (bw *blockWriter) append(e entry, leased bool) (exists bool, err error) {
	blockIdx := startBlockIndex(e.seq)
	if leased {
		b, ok := bw.leased[blockIdx]
		if !ok {
			off := blockOffset(int32(blockIdx))
			bh := blockHandle{file: bw.file, offset: off}
			if err := bh.read(); err != nil {
				return false, err
			}
			lBlock := leasedBlock{}
			b = leasedBlock{entries: append(lBlock.entries, bh.block.entries[:bh.entryIdx]...), entryIdx: bh.entryIdx}
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
		// fmt.Println("block.append: leased off, entryId, seq", blockOffset(int32(blockIdx)), b.entryIdx, e.seq)
		b.entries = append(b.entries, e)
		b.entryIdx++
		bw.leased[blockIdx] = b
		return false, nil
	}

	b, ok := bw.blocks[blockIdx]
	if !ok {
		bw.count++
	}

	// fmt.Println("block.append: off, entryId, seq", blockOffset(int32(blockIdx)), b.entryIdx, e.seq)
	b.entries[b.entryIdx] = e
	b.entryIdx++

	bw.blocks[blockIdx] = b

	return false, nil
}

func (bw *blockWriter) write() error {
	defer func() {
		bw.count = 0
		bw.blocks = make(map[int32]block)
		bw.leased = make(map[int32]leasedBlock)
		bw.buffer.Reset()
	}()

	// sort blocks by blockIdx
	var idx []int
	// var blocks [][]int
	for i := range bw.blocks {
		idx = append(idx, int(i))
	}
	sort.Ints(idx)
	blocks, err := blockRange(idx)
	if err != nil {
		return err
	}
	fmt.Println("block.write: blocks ", blocks)
	for _, idx := range blocks {
		if len(idx) == 1 {
			off := blockOffset(int32(idx[0]))
			b := bw.blocks[int32(idx[0])]
			buf := b.MarshalBinary()
			if _, err := bw.WriteAt(buf, off); err != nil {
				return err
			}
			continue
		}
		blockOff := blockOffset(int32(idx[0]))
		for i := idx[0]; i <= idx[1]; i++ {
			b := bw.blocks[int32(i)]
			bw.buffer.Write(b.MarshalBinary())
		}
		if _, err := bw.WriteAt(bw.buffer.Bytes(), blockOff); err != nil {
			return err
		}
	}

	for blockIdx, b := range bw.leased {
		off := blockOffset(int32(blockIdx))
		bh := blockHandle{file: bw.file, offset: off}
		if err := bh.read(); err != nil {
			return err
		}
		// fmt.Println("block.write: blockIdx, entryIdx, count ", blockIdx, bh.entryIdx, len(b.entries))
		for _, e := range b.entries {
			entryIdx := 0
			for i := 0; i < int(bh.entryIdx); i++ {
				if bh.entries[i].seq == e.seq { //record exist in db
					entryIdx = -1
					break
				}
			}
			if entryIdx == -1 {
				continue
			}
			if bh.entryIdx == entriesPerIndexBlock {
				seqs := []uint64{}
				for _, e := range b.entries {
					seqs = append(seqs, e.seq)
				}
				fmt.Println("block.write: err ", seqs)
			}
			bh.entries[bh.entryIdx] = e
			bh.entryIdx++
		}

		buf := bh.MarshalBinary()
		if _, err := bw.WriteAt(buf, off); err != nil {
			return err
		}
	}

	return nil
}

func (bw *blockWriter) Count() int {
	return bw.count
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
