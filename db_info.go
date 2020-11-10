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

package unitdb

import (
	"encoding/binary"
)

var (
	signature = [7]byte{'u', 'n', 'i', 't', 'd', 'b', '\x0e'}
	fixed     = uint32(64)
)

type (
	_Header struct {
		signature [7]byte
		version   uint32
	}
	_DBInfo struct {
		header     _Header
		encryption int8
		sequence   uint64
		count      uint64
		blockIdx   int32
		windowIdx  int32
	}
)

// MarshalBinary serializes db info into binary data.
func (inf _DBInfo) MarshalBinary() ([]byte, error) {
	buf := make([]byte, fixed)
	copy(buf[:7], inf.header.signature[:])
	binary.LittleEndian.PutUint32(buf[7:11], inf.header.version)
	buf[12] = uint8(inf.encryption)
	binary.LittleEndian.PutUint64(buf[12:20], inf.sequence)
	binary.LittleEndian.PutUint64(buf[20:28], inf.count)
	binary.LittleEndian.PutUint32(buf[28:32], uint32(inf.windowIdx))
	binary.LittleEndian.PutUint32(buf[32:36], uint32(inf.blockIdx))

	return buf, nil
}

// UnmarshalBinary de-serializes db info from binary data.
func (inf *_DBInfo) UnmarshalBinary(data []byte) error {
	copy(inf.header.signature[:], data[:7])
	inf.header.version = binary.LittleEndian.Uint32(data[7:11])
	inf.encryption = int8(data[7])
	inf.sequence = binary.LittleEndian.Uint64(data[12:20])
	inf.count = binary.LittleEndian.Uint64(data[20:28])
	inf.windowIdx = int32(binary.LittleEndian.Uint32(data[28:32]))
	inf.blockIdx = int32(binary.LittleEndian.Uint32(data[32:36]))

	return nil
}
