package tracedb

import (
	"encoding/binary"
)

type dataTable struct {
	table
	fb freeblocks
}

func (t *dataTable) readKeyValue(e entry, fillCache bool) ([]byte, []byte, error) {

	var cacheKey string
	if t.cache != nil {
		var kb [8]byte
		binary.LittleEndian.PutUint64(kb[:8], t.cacheID^uint64(e.kvOffset))
		cacheKey = string(kb[:])

		if data, err := t.cache.Get(cacheKey); data != nil && len(data) == int(e.kvSize()) {
			return data[:keySize], data[e.topicSize+keySize:], err
		}
	}
	keyValue, err := t.Slice(e.kvOffset, e.kvOffset+int64(e.kvSize()))
	if err != nil {
		return nil, nil, err
	}
	if t.cache != nil && fillCache {
		t.cache.Set(cacheKey, keyValue)
	}
	return keyValue[:keySize], keyValue[e.topicSize+keySize:], nil
}

func (t *dataTable) readKey(e entry) ([]byte, error) {
	var cacheKey string
	if t.cache != nil {
		var kb [8]byte
		binary.LittleEndian.PutUint64(kb[:8], t.cacheID^uint64(e.kvOffset))
		cacheKey = string(kb[:])

		if data, err := t.cache.Get(cacheKey); data != nil {
			return data[:keySize], err
		}
	}
	return t.Slice(e.kvOffset, e.kvOffset+int64(keySize))
}

func (t *dataTable) readTopic(e entry) ([]byte, error) {
	var cacheKey string
	if t.cache != nil {
		var kb [8]byte
		binary.LittleEndian.PutUint64(kb[:8], t.cacheID^uint64(e.kvOffset))
		cacheKey = string(kb[:])

		if data, err := t.cache.Get(cacheKey); data != nil {
			return data[keySize : e.topicSize+keySize], err
		}
	}
	return t.Slice(e.kvOffset+int64(keySize), e.kvOffset+int64(e.topicSize)+int64(keySize))
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

func (t *dataTable) writeKeyValue(topic, key, value []byte) (off int64, err error) {
	dataLen := align512(uint32(len(topic) + keySize + len(value)))
	data := make([]byte, dataLen)
	copy(data, key)
	copy(data[keySize:], topic)
	copy(data[len(topic)+keySize:], value)
	off = t.fb.allocate(dataLen)
	if off != -1 {
		if _, err = t.WriteAt(data, off); err != nil {
			return 0, err
		}
	} else {
		off, err = t.append(data)
	}
	if t.cache != nil {
		var kb [8]byte
		binary.LittleEndian.PutUint64(kb[:8], t.cacheID^uint64(off))
		cacheKey := string(kb[:])
		t.cache.Delete(cacheKey)
	}
	return off, err
}
