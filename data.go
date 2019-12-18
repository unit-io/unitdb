package tracedb

type dataTable struct {
	table
	fb freeblocks
}

func (t *dataTable) readMessage(e entry) ([]byte, []byte, error) {
	// var cacheKey uint64
	if t.cache != nil {
		// cacheKey = t.cacheID ^ e.seq
		if data, err := t.cache.GetData(e.seq); data != nil && uint32(len(data)) == e.mSize() {
			return data[:idSize], data[e.topicSize+idSize:], err
		}
	}
	message, err := t.Slice(e.mOffset, e.mOffset+int64(e.mSize()))
	if err != nil {
		return nil, nil, err
	}
	return message[:idSize], message[e.topicSize+idSize:], nil
}

func (t *dataTable) readId(e entry) ([]byte, error) {
	if t.cache != nil {
		// cacheKey := t.cacheID ^ e.seq
		if data, err := t.cache.GetData(e.seq); data != nil && uint32(len(data)) == e.mSize() {
			return data[:idSize], err
		}
	}
	return t.Slice(e.mOffset, e.mOffset+int64(idSize))
}

func (t *dataTable) readTopic(e entry) ([]byte, error) {
	if t.cache != nil {
		// cacheKey := t.cacheID ^ e.seq
		if data, err := t.cache.GetData(e.seq); data != nil && uint32(len(data)) == e.mSize() {
			return data[idSize : e.topicSize+idSize], err
		}
	}
	return t.Slice(e.mOffset+int64(idSize), e.mOffset+int64(e.topicSize)+int64(idSize))
}

func (t *dataTable) allocate(size uint32) (int64, error) {
	size = align512(size)
	if off := t.fb.allocate(size); off > 0 {
		return off, nil
	}
	return t.extend(size)
}

func (t *dataTable) free(size uint32, off int64) {
	size = align512(size)
	t.fb.free(off, size)
}

func (t *dataTable) writeMessage(id, topic, value []byte) (off int64, err error) {
	dataLen := align512(uint32(idSize + len(topic) + len(value)))
	data := make([]byte, dataLen)
	copy(data, id)
	copy(data[idSize:], topic)
	copy(data[len(topic)+idSize:], value)
	off = t.fb.allocate(dataLen)
	if off != -1 {
		if _, err = t.WriteAt(data, off); err != nil {
			return 0, err
		}
	} else {
		off, err = t.append(data)
	}
	return off, err
}

func (t *dataTable) writeRaw(data []byte, off int64) error {
	if _, err := t.WriteAt(data, off); err != nil {
		return err
	}
	return nil
}
