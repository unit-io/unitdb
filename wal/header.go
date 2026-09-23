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
	"hash/crc32"
)

var (
	// logHeaderSize is the size of a version 2 header; version 1 headers have
	// no checksum.
	logHeaderSize   = 22
	logHeaderSizeV1 = 18

	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

type _LogInfo struct {
	version  uint16
	timeID   int64
	count    uint32
	size     uint32
	checksum uint32 // CRC32C of the log data, since version 2.

	_ [24]byte
}

func (l _LogInfo) MarshalBinary() ([]byte, error) {
	buf := make([]byte, logHeaderSize)
	binary.LittleEndian.PutUint16(buf[:2], l.version)
	binary.LittleEndian.PutUint64(buf[2:10], uint64(l.timeID))
	binary.LittleEndian.PutUint32(buf[10:14], l.count)
	binary.LittleEndian.PutUint32(buf[14:18], l.size)
	binary.LittleEndian.PutUint32(buf[18:22], l.checksum)

	return buf, nil
}

func (l *_LogInfo) UnmarshalBinary(data []byte) error {
	l.version = binary.LittleEndian.Uint16(data[:2])
	l.timeID = int64(binary.LittleEndian.Uint64(data[2:10]))
	l.count = binary.LittleEndian.Uint32(data[10:14])
	l.size = binary.LittleEndian.Uint32(data[14:18])
	if l.version >= 2 && len(data) >= logHeaderSize {
		l.checksum = binary.LittleEndian.Uint32(data[18:22])
	}

	return nil
}

// headerSize returns the header size for a log of the given version.
func headerSize(version uint16) int {
	if version < 2 {
		return logHeaderSizeV1
	}
	return logHeaderSize
}
