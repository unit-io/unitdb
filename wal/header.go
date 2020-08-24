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

package wal

import (
	"encoding/binary"
)

var (
	signature     = [8]byte{'t', 'r', 'a', 'c', 'e', 'd', 'b', '\xfd'}
	logHeaderSize = 20
	headerSize    = uint32(36)
)

type logInfo struct {
	version    uint16
	status     LogStatus
	entryCount uint32
	size       uint32
	offset     int64

	_ [20]byte
}

// MarshalBinary serialized logInfo into binary data.
func (l logInfo) MarshalBinary() ([]byte, error) {
	buf := make([]byte, logHeaderSize)
	binary.LittleEndian.PutUint16(buf[:2], l.version)
	binary.LittleEndian.PutUint16(buf[2:4], uint16(l.status))
	binary.LittleEndian.PutUint32(buf[4:8], l.entryCount)
	binary.LittleEndian.PutUint32(buf[8:12], l.size)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(l.offset))
	return buf, nil
}

// UnmarshalBinary deserialized logInfo from binary data.
func (l *logInfo) UnmarshalBinary(data []byte) error {
	l.version = binary.LittleEndian.Uint16(data[:2])
	l.status = LogStatus(binary.LittleEndian.Uint16(data[2:4]))
	l.entryCount = binary.LittleEndian.Uint32(data[4:8])
	l.size = binary.LittleEndian.Uint32(data[8:12])
	l.offset = int64(binary.LittleEndian.Uint64(data[12:20]))
	return nil
}

type header struct {
	signature [8]byte
	version   uint32
	segments
	_ [2]byte
}

// MarshalBinary serialized header into binary data.
func (h header) MarshalBinary() ([]byte, error) {
	buf := make([]byte, headerSize)
	copy(buf[:8], h.signature[:])
	binary.LittleEndian.PutUint32(buf[8:12], h.version)
	binary.LittleEndian.PutUint32(buf[12:16], h.segments[0].size)
	binary.LittleEndian.PutUint64(buf[16:24], uint64(h.segments[0].offset))
	binary.LittleEndian.PutUint32(buf[24:28], h.segments[1].size)
	binary.LittleEndian.PutUint64(buf[28:36], uint64(h.segments[1].offset))
	return buf, nil
}

// UnmarshalBinary deserialized header from binary data.
func (h *header) UnmarshalBinary(data []byte) error {
	copy(h.signature[:], data[:8])
	h.version = binary.LittleEndian.Uint32(data[8:12])
	h.segments[0].size = binary.LittleEndian.Uint32(data[12:16])
	h.segments[0].offset = int64(binary.LittleEndian.Uint64(data[16:24]))
	h.segments[1].size = binary.LittleEndian.Uint32(data[24:28])
	h.segments[1].offset = int64(binary.LittleEndian.Uint64(data[28:36]))
	return nil
}
