package memdb

// type dataTable struct {
// 	tableManager
// 	size int64
// }

// func (t *dataTable) append(data []byte) (int64, error) {
// 	off := t.size
// 	if _, err := t.writeAt(data, off); err != nil {
// 		return 0, err
// 	}
// 	t.size += int64(len(data))
// 	return off, nil
// }

// func (t *dataTable) allocate(size uint32) (int64, error) {
// 	return t.extend(size)
// }

// func (t *dataTable) readRaw(off, mSize int64) ([]byte, error) {
// 	return t.slice(off, off+mSize)
// }

// func (t *dataTable) writeMessage(id, topic, value []byte) (off int64, err error) {
// 	dataLen := align512(idSize + uint32(len(topic)+len(value)))
// 	data := make([]byte, dataLen)
// 	copy(data, id)
// 	copy(data[idSize:], topic)
// 	copy(data[len(topic)+idSize:], value)
// 	t.allocate(dataLen)
// 	off, err = t.append(data)
// 	return off, err
// }
