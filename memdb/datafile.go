package memdb

type dataFile struct {
	file
	fl freelist
}

func (f *dataFile) readKeyValue(e entry) ([]byte, []byte, error) {
	keyValue, err := f.Slice(e.kvOffset, e.kvOffset+int64(e.kvSize()))
	if err != nil {
		return nil, nil, err
	}
	return keyValue[:keySize], keyValue[e.topicSize+keySize:], nil
}

func (f *dataFile) readKey(e entry) ([]byte, error) {
	return f.Slice(e.kvOffset, e.kvOffset+int64(keySize))
}

func (f *dataFile) readTopic(e entry) ([]byte, error) {
	return f.Slice(e.kvOffset+int64(keySize), e.kvOffset+int64(e.topicSize)+int64(keySize))
}

func (f *dataFile) allocate(size uint32) (int64, error) {
	size = align512(size)
	if off := f.fl.allocate(size); off > 0 {
		return off, nil
	}
	return f.extend(size)
}

func (f *dataFile) free(size uint32, off int64) {
	size = align512(size)
	f.fl.free(off, size)
}

func (f *dataFile) writeKeyValue(topic, key, value []byte) (int64, error) {
	dataLen := align512(uint32(len(topic) + keySize + len(value)))
	data := make([]byte, dataLen)
	copy(data, key)
	copy(data[keySize:], topic)
	copy(data[len(topic)+keySize:], value)
	off := f.fl.allocate(dataLen)
	if off != -1 {
		if _, err := f.WriteAt(data, off); err != nil {
			return 0, err
		}
	} else {
		return f.append(data)
	}
	return off, nil
}
