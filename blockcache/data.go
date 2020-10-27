/*
 * Copyright 2020 Saffat Technologies, Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cache

import "errors"

type _DataTable struct {
	buf    []byte
	size   int64
	closed bool
}

func (t *_DataTable) allocate(size uint32) (int64, error) {
	if size == 0 {
		panic("unable to allocate zero bytes")
	}
	off := t.size
	return off, t.truncate(t.size + int64(size))
}

func (t *_DataTable) shrink(off int64) error {
	if t.size == 0 {
		panic("unable to shrink table of size zero bytes")
	}
	if err := t.truncateFront(off); err != nil {
		return err
	}
	return nil
}

func (t *_DataTable) readRaw(off int64, size uint32) ([]byte, error) {
	return t.slice(off, off+int64(size))
}

func (t *_DataTable) close() error {
	if t.closed {
		return errors.New("table closed")
	}
	t.closed = true
	return nil
}

func (t *_DataTable) writeAt(p []byte, off int64) (int, error) {
	if t.closed {
		return 0, errors.New("table closed")
	}
	n := len(p)
	if off == t.size {
		t.buf = append(t.buf, p...)
		t.size += int64(n)
	} else if off+int64(n) > t.size {
		panic("trying to write past EOF - undefined behavior")
	} else {
		copy(t.buf[off:off+int64(n)], p)
	}
	return n, nil
}

func (t *_DataTable) truncate(size int64) error {
	if t.closed {
		return errors.New("table closed")
	}
	if size > t.size {
		diff := int(size - t.size)
		t.buf = append(t.buf, make([]byte, diff)...)
	} else {
		t.buf = t.buf[:t.size]
	}
	t.size = size
	return nil
}

func (t *_DataTable) truncateFront(off int64) error {
	if t.closed {
		return errors.New("table closed")
	}
	if off > t.size {
		t.buf = nil
		t.size = 0
		return nil
	}
	t.buf = t.buf[off:t.size]
	t.size = t.size - off
	return nil
}

func (t *_DataTable) slice(start int64, end int64) ([]byte, error) {
	if t.closed {
		return nil, errors.New("table closed")
	}
	return t.buf[start:end], nil
}
