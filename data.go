package tracedb

type dataTable struct {
	file
	fb freeblocks
}

func (t *dataTable) readMessage(e entry) ([]byte, []byte, error) {
	if e.cacheBlock != nil {
		return e.cacheBlock[:idSize], e.cacheBlock[e.topicSize+idSize:], nil
	}
	message, err := t.Slice(e.msgOffset, e.msgOffset+int64(e.mSize()))
	if err != nil {
		return nil, nil, err
	}
	return message[:idSize], message[e.topicSize+idSize:], nil
}

func (t *dataTable) readId(e entry) ([]byte, error) {
	if e.cacheBlock != nil {
		return e.cacheBlock[:idSize], nil
	}
	return t.Slice(e.msgOffset, e.msgOffset+int64(idSize))
}

func (t *dataTable) readTopic(e entry) ([]byte, error) {
	if e.cacheBlock != nil {
		return e.cacheBlock[idSize : e.topicSize+idSize], nil
	}
	return t.Slice(e.msgOffset+int64(idSize), e.msgOffset+int64(e.topicSize)+int64(idSize))
}

func (t *dataTable) free(size uint32, off int64) {
	size = align(size)
	t.fb.free(off, size)
}

func (t *dataTable) write(data []byte) (off int64, err error) {
	dataLen := align(uint32(len(data)))
	buf := make([]byte, dataLen)
	copy(buf, data)
	off = t.fb.allocate(dataLen)
	if off != -1 {
		if _, err = t.WriteAt(buf, off); err != nil {
			return 0, err
		}
		return off, errLeasedBlock
	} else {
		off, err = t.append(buf)
	}
	return off, err
}
