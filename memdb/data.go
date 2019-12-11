package memdb

type dataTable struct {
	tableManager
	fb   freeblocks
	size int64
}

func (t *dataTable) extend(size uint32) (int64, error) {
	off := t.size
	if err := t.truncate(off + int64(size)); err != nil {
		return 0, err
	}
	t.size += int64(size)
	return off, nil
}

func (t *dataTable) append(data []byte) (int64, error) {
	off := t.size
	if _, err := t.writeAt(data, off); err != nil {
		return 0, err
	}
	t.size += int64(len(data))
	return off, nil
}

func (t *dataTable) readRaw(off, kvSize int64) ([]byte, error) {
	return t.slice(off, off+kvSize)
}

func (t *dataTable) readMessage(e entry) ([]byte, []byte, error) {
	message, err := t.slice(e.mOffset, e.mOffset+int64(e.mSize()))
	if err != nil {
		return nil, nil, err
	}
	return message[:idSize], message[e.topicSize+idSize:], nil
}

func (t *dataTable) readId(e entry) ([]byte, error) {
	return t.slice(e.mOffset, e.mOffset+idSize)
}

func (t *dataTable) allocate(size uint32) (int64, error) {
	if off := t.fb.allocate(size); off > 0 {
		return off, nil
	}
	return t.extend(size)
}

func (t *dataTable) readTopic(e entry) ([]byte, error) {
	return t.slice(e.mOffset+idSize, e.mOffset+int64(e.topicSize)+idSize)
}

func (t *dataTable) free(size uint32, off int64) {
	size = align512(size)
	t.fb.free(off, size)
}

func (t *dataTable) writeMessage(id, topic, value []byte) (off int64, err error) {
	dataLen := align512(idSize + uint32(len(topic)+len(value)))
	data := make([]byte, dataLen)
	copy(data, id)
	copy(data[idSize:], topic)
	copy(data[len(topic)+idSize:], value)
	off = t.fb.allocate(dataLen)
	if off != -1 {
		if _, err = t.writeAt(data, off); err != nil {
			return 0, err
		}
	} else {
		off, err = t.append(data)
	}
	return off, err
}

func (t *dataTable) writeRaw(raw []byte) (int64, error) {
	return t.append(raw)
}
