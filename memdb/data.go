package memdb

type dataTable struct {
	tableManager
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

func (t *dataTable) readKeyValue(e entry) ([]byte, []byte, error) {
	keyValue, err := t.slice(e.kvOffset, e.kvOffset+int64(e.kvSize()))
	if err != nil {
		return nil, nil, err
	}
	return keyValue[:keySize], keyValue[e.topicSize+keySize:], nil
}

func (t *dataTable) readKey(e entry) ([]byte, error) {
	return t.slice(e.kvOffset, e.kvOffset+int64(keySize))
}

func (t *dataTable) readTopic(e entry) ([]byte, error) {
	return t.slice(e.kvOffset+int64(keySize), e.kvOffset+int64(e.topicSize)+int64(keySize))
}

func (t *dataTable) writeKeyValue(topic, key, value []byte) (int64, error) {
	dataLen := align512(uint32(len(topic) + keySize + len(value)))
	data := make([]byte, dataLen)
	copy(data, key)
	copy(data[keySize:], topic)
	copy(data[len(topic)+keySize:], value)
	return t.append(data)
}

func (t *dataTable) writeRaw(raw []byte) (int64, error) {
	return t.append(raw)
}
