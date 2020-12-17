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
	logHeaderSize = 18
)

type _LogInfo struct {
	version uint16
	timeID  int64
	count   uint32
	size    uint32

	_ [28]byte
}

// MarshalBinary serialized logInfo into binary data.
func (l _LogInfo) MarshalBinary() ([]byte, error) {
	buf := make([]byte, logHeaderSize)
	binary.LittleEndian.PutUint16(buf[:2], l.version)
	binary.LittleEndian.PutUint64(buf[2:10], uint64(l.timeID))
	binary.LittleEndian.PutUint32(buf[10:14], l.count)
	binary.LittleEndian.PutUint32(buf[14:18], l.size)

	return buf, nil
}

// UnmarshalBinary deserialized logInfo from binary data.
func (l *_LogInfo) UnmarshalBinary(data []byte) error {
	l.version = binary.LittleEndian.Uint16(data[:2])
	l.timeID = int64(binary.LittleEndian.Uint64(data[2:10]))
	l.count = binary.LittleEndian.Uint32(data[10:14])
	l.size = binary.LittleEndian.Uint32(data[14:18])

	return nil
}
