package tracedb

import "encoding/binary"

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

		if data, err := f.cache.Get(cacheKey); data != nil {
			return data[:e.keySize], data[e.keySize:], err
		}
	}
	keyValue, err := f.Slice(e.kvOffset, e.kvOffset+int64(e.kvSize()))
	if err != nil {
		return nil, nil, err
	}
	if f.cache != nil && fillCache {
		f.cache.Set(cacheKey, keyValue)
	}
	return keyValue[:e.keySize], keyValue[e.keySize:], nil
}

func (f *dataFile) readKey(e entry) ([]byte, error) {
	return f.Slice(e.kvOffset, e.kvOffset+int64(e.keySize))
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

func (f *dataFile) writeKeyValue(key []byte, value []byte) (int64, error) {
	dataLen := align512(uint32(len(key) + len(value)))
	data := make([]byte, dataLen)
	copy(data, key)
	copy(data[len(key):], value)
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

func (f *dataFile) writeValue(value []byte) (int64, error) {
	dataLen := align512(uint32(len(value)))
	data := make([]byte, dataLen)
	copy(data, value)
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
