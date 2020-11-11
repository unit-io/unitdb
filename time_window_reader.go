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

type _WindowReader struct {
	winBlock _WinBlock
	file     *_File
}

func newWindowReader(f *_File) *_WindowReader {
	return &_WindowReader{file: f}
}

func (r *_WindowReader) readBlock(off int64) (_WinBlock, error) {
	buf, err := r.file.slice(off, off+int64(blockSize))
	if err != nil {
		return _WinBlock{}, err
	}
	r.winBlock.UnmarshalBinary(buf)

	return r.winBlock, nil
}
