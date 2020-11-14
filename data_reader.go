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

type _DataReader struct {
	dataTable _DataTable

	file *_File
}

func newDataReader(dt _DataTable, f *_File) *_DataReader {
	return &_DataReader{dataTable: dt, file: f}
}

func (r *_DataReader) readMessage(e _IndexEntry) ([]byte, []byte, error) {
	if e.cache != nil {
		return e.cache[:idSize], e.cache[e.topicSize+idSize:], nil
	}
	message, err := r.file.slice(e.msgOffset, e.msgOffset+int64(e.mSize()))
	if err != nil {
		return nil, nil, err
	}
	return message[:idSize], message[e.topicSize+idSize:], nil
}

func (r *_DataReader) readTopic(e _IndexEntry) ([]byte, error) {
	if e.cache != nil {
		return e.cache[idSize : e.topicSize+idSize], nil
	}
	return r.file.slice(e.msgOffset+int64(idSize), e.msgOffset+int64(e.topicSize)+int64(idSize))
}

func (r *_DataReader) size() (int64, error) {
	s, err := r.file.Stat()
	if err != nil {
		return 0, err
	}

	return s.Size(), nil
}
