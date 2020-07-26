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

type dataTable struct {
	file
	lease *lease

	offset int64
}

func (dt *dataTable) readMessage(s slot) ([]byte, []byte, error) {
	if s.cacheBlock != nil {
		return s.cacheBlock[:idSize], s.cacheBlock[s.topicSize+idSize:], nil
	}
	message, err := dt.Slice(s.msgOffset, s.msgOffset+int64(s.mSize()))
	if err != nil {
		return nil, nil, err
	}
	return message[:idSize], message[s.topicSize+idSize:], nil
}

func (dt *dataTable) readTopic(s slot) ([]byte, error) {
	if s.cacheBlock != nil {
		return s.cacheBlock[idSize : s.topicSize+idSize], nil
	}
	return dt.Slice(s.msgOffset+int64(idSize), s.msgOffset+int64(s.topicSize)+int64(idSize))
}

func (dt *dataTable) extend(size uint32) (int64, error) {
	off := dt.offset
	if _, err := dt.file.extend(size); err != nil {
		return 0, err
	}
	dt.offset += int64(size)

	return off, nil
}
