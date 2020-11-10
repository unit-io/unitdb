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

func (r *_DataReader) readMessage(s _Slot) ([]byte, []byte, error) {
	if s.cache != nil {
		return s.cache[:idSize], s.cache[s.topicSize+idSize:], nil
	}
	message, err := r.file.Slice(s.msgOffset, s.msgOffset+int64(s.mSize()))
	if err != nil {
		return nil, nil, err
	}
	return message[:idSize], message[s.topicSize+idSize:], nil
}

func (r *_DataReader) readTopic(s _Slot) ([]byte, error) {
	if s.cache != nil {
		return s.cache[idSize : s.topicSize+idSize], nil
	}
	return r.file.Slice(s.msgOffset+int64(idSize), s.msgOffset+int64(s.topicSize)+int64(idSize))
}

func (r *_DataReader) size() (int64, error) {
	ds, err := r.file.Stat()
	if err != nil {
		return 0, err
	}

	return ds.Size(), nil
}
