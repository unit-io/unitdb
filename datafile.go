package tracedb

import (
	"encoding/binary"
)

type dataFile struct {
	file
	fl freelist
}

func (f *dataFile) readKeyValue(e entry, fillCache bool) ([]byte, []byte, error) {

	var cacheKey string
	if f.cache != nil {
		var kb [8]byte
		binary.LittleEndian.PutUint64(kb[:8], f.cacheID^uint64(e.kvOffset))
		cacheKey = string(kb[:])

		if data, err := f.cache.Get(cacheKey); data != nil && len(data) == int(e.kvSize()) {
			return data[:keySize], data[e.topicSize+keySize:], err
		}
	}
	keyValue, err := f.Slice(e.kvOffset, e.kvOffset+int64(e.kvSize()))
	if err != nil {
		return nil, nil, err
	}
	if f.cache != nil && fillCache {
		f.cache.Set(cacheKey, keyValue)
	}
	return keyValue[:keySize], keyValue[e.topicSize+keySize:], nil
}

func (f *dataFile) readKey(e entry) ([]byte, error) {
	var cacheKey string
	if f.cache != nil {
		var kb [8]byte
		binary.LittleEndian.PutUint64(kb[:8], f.cacheID^uint64(e.kvOffset))
		cacheKey = string(kb[:])

		if data, err := f.cache.Get(cacheKey); data != nil {
			return data[:keySize], err
		}
	}
	return f.Slice(e.kvOffset, e.kvOffset+int64(keySize))
}

func (f *dataFile) readTopic(e entry) ([]byte, error) {
	var cacheKey string
	if f.cache != nil {
		var kb [8]byte
		binary.LittleEndian.PutUint64(kb[:8], f.cacheID^uint64(e.kvOffset))
		cacheKey = string(kb[:])

		if data, err := f.cache.Get(cacheKey); data != nil {
			return data[keySize : e.topicSize+keySize], err
		}
	}
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

func (f *dataFile) writeKeyValue(topic, key, value []byte) (off int64, err error) {
	dataLen := align512(uint32(len(topic) + keySize + len(value)))
	data := make([]byte, dataLen)
	copy(data, key)
	copy(data[keySize:], topic)
	copy(data[len(topic)+keySize:], value)
	off = f.fl.allocate(dataLen)
	if off != -1 {
		if _, err = f.WriteAt(data, off); err != nil {
			return 0, err
		}
	} else {
		off, err = f.append(data)
	}
	if f.cache != nil {
		var kb [8]byte
		binary.LittleEndian.PutUint64(kb[:8], f.cacheID^uint64(off))
		cacheKey := string(kb[:])
		f.cache.Delete(cacheKey)
	}
	return off, err
}
