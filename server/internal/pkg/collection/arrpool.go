package collection

import (
	"encoding/binary"
	"sync"
)

var arrayPool = &sync.Pool{
	New: func() interface{} {
		return &Array{
			buf: make([]byte, 0, 500),
		}
	},
}

// Array is used to prepopulate an array of items
// which can be re-used to add to log messages.
type Array struct {
	buf []byte
}

func putArray(arr *Array) {
	// Proper usage of a sync.Pool requires each entry to have approximately
	// the same memory cost. To obtain this property when the stored type
	// contains a variably-sized buffer, we add a hard limit on the maximum buffer
	// to place back in the pool.
	//
	// See https://golang.org/issue/23199
	const maxSize = 1 << 16 // 64KiB
	if cap(arr.buf) > maxSize {
		return
	}
	arrayPool.Put(arr)
}

// Arr creates an array to be added to an Event or Context.
func Arr() *Array {
	arr := arrayPool.Get().(*Array)
	arr.buf = arr.buf[:0]
	return arr
}

// MarshalZerologArray method here is no-op - since data is
// already in the needed format.
func (*Array) MarshalZerologArray(*Array) {
}

func (arr *Array) write(dst []byte) []byte {
	sizebuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizebuf[:4], uint32(len(arr.buf)))
	if len(arr.buf) > 0 {
		dst = append(append(dst, sizebuf...))
		dst = append(append(dst, arr.buf...))
	}
	putArray(arr)
	return dst
}
