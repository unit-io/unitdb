package tracedb

import (
	"github.com/unit-io/bpool"
)

type (
	dataTable struct {
		file
		lease lease

		offset int64
	}

	dataWriter struct {
		*dataTable
		buffer *bpool.Buffer

		writeComplete bool
	}
)

func newDataWriter(dt *dataTable, buf *bpool.Buffer) *dataWriter {
	return &dataWriter{dataTable: dt, buffer: buf}
}

func (dt *dataTable) readMessage(e entry) ([]byte, []byte, error) {
	if e.cacheBlock != nil {
		return e.cacheBlock[:idSize], e.cacheBlock[e.topicSize+idSize:], nil
	}
	message, err := dt.Slice(e.msgOffset, e.msgOffset+int64(e.mSize()))
	if err != nil {
		return nil, nil, err
	}
	return message[:idSize], message[e.topicSize+idSize:], nil
}

func (dt *dataTable) readId(e entry) ([]byte, error) {
	if e.cacheBlock != nil {
		return e.cacheBlock[:idSize], nil
	}
	return dt.Slice(e.msgOffset, e.msgOffset+int64(idSize))
}

func (dt *dataTable) readTopic(e entry) ([]byte, error) {
	if e.cacheBlock != nil {
		return e.cacheBlock[idSize : e.topicSize+idSize], nil
	}
	return dt.Slice(e.msgOffset+int64(idSize), e.msgOffset+int64(e.topicSize)+int64(idSize))
}

func (dt *dataTable) free(e entry) {
	// size := align(e.mSize())
	size := e.mSize()
	dt.lease.free(e.seq, e.msgOffset, size)
}

func (dt *dataTable) extend(size uint32) (int64, error) {
	off := dt.offset
	if _, err := dt.file.extend(size); err != nil {
		return 0, err
	}
	dt.offset += int64(size)

	return off, nil
}

func (dw *dataWriter) writeMessage(data []byte) (off int64, err error) {
	// dataLen := align(uint32(len(data)))
	dataLen := uint32(len(data))
	buf := make([]byte, dataLen)
	copy(buf, data)
	off = dw.lease.allocate(dataLen)
	if off != -1 {
		if _, err = dw.file.WriteAt(buf, off); err != nil {
			return 0, err
		}
	} else {
		off = dw.offset
		if _, err := dw.append(data); err != nil {
			return 0, err
		}
	}
	return off, err
}

func (dw *dataWriter) append(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// dataLen := align(uint32(len(data)))
	dataLen := len(data)
	off, err := dw.buffer.Extend(int64(dataLen))
	if err != nil {
		return 0, err
	}
	dw.offset += int64(dataLen)
	return dw.buffer.WriteAt(data, off)
}

func (dw *dataWriter) write() (int, error) {
	n, err := dw.file.write(dw.buffer.Bytes())
	if err != nil {
		return 0, err
	}
	dw.writeComplete = true
	return n, err
}
