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
	signature  = [7]byte{'u', 'n', 'i', 't', 'd', 'b', '\x0e'}
	headerSize = uint32(64)
)

type _Header struct {
	signature [7]byte
	version   uint32
	dbInfo    _DBInfo
	_         [12]byte
}

// MarshalBinary serializes header into binary data.
func (h _Header) MarshalBinary() ([]byte, error) {
	buf := make([]byte, headerSize)
	copy(buf[:7], h.signature[:])
	buf[7] = uint8(h.dbInfo.encryption)
	binary.LittleEndian.PutUint32(buf[8:12], h.version)
	binary.LittleEndian.PutUint64(buf[12:20], h.dbInfo.sequence)
	binary.LittleEndian.PutUint64(buf[20:28], h.dbInfo.count)
	binary.LittleEndian.PutUint32(buf[28:32], uint32(h.dbInfo.windowIdx))
	binary.LittleEndian.PutUint32(buf[32:36], uint32(h.dbInfo.blockIdx))
	binary.LittleEndian.PutUint64(buf[36:44], h.dbInfo.cacheID)
	return buf, nil
}

// UnmarshalBinary de-serializes header from binary data.
func (h *_Header) UnmarshalBinary(data []byte) error {
	copy(h.signature[:], data[:7])
	h.dbInfo.encryption = int8(data[7])
	h.dbInfo.sequence = binary.LittleEndian.Uint64(data[12:20])
	h.dbInfo.count = binary.LittleEndian.Uint64(data[20:28])
	h.dbInfo.windowIdx = int32(binary.LittleEndian.Uint32(data[28:32]))
	h.dbInfo.blockIdx = int32(binary.LittleEndian.Uint32(data[32:36]))
	h.dbInfo.cacheID = binary.LittleEndian.Uint64(data[36:44])

	return nil
}
