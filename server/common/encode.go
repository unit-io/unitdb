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

package common

import (
	"github.com/golang/protobuf/proto"
)

const (
	MaxMessageSize = 1 << 19
)

// Encoder encodes a byte slice to write into the destination proto.Message.
// You do not need to copy the slice; you may use it directly.
//
// You do not have to encode the full byte slice in one packet. You can
// choose to chunk your packets by returning 0 < n < len(p) and the
// Conn will repeatedly send subsequent messages by slicing into the
// byte slice.
type Encoder func(proto.Message, []byte) (int, error)

// Decode is given a Response value and expects you to decode the
// response value into the byte slice given. You MUST decode up to
// len(p) if available.
//
// This should return the data slice directly from m. The length of this
// is used to determine if there is more data and the offset for the next
// read.
type Decoder func(m proto.Message, offset int, p []byte) ([]byte, error)

// Encode is the easiest way to generate an Encoder for a proto.Message.
// You just give it a callback that gets the pointer to the byte slice field
// and a valid encoder will be generated.
//
// Example: given a structure that has a field "Data []byte", you could:
//
//     Encode(func(msg proto.Message) *[]byte {
//         return &msg.(*pbx.Packet).Data
//     })
//
func Encode(f func(proto.Message) *[]byte) Encoder {
	return func(msg proto.Message, p []byte) (int, error) {
		bytePtr := f(msg)
		*bytePtr = p
		return len(p), nil
	}
}

// SimpleDecoder is the easiest way to generate a Decoder for a proto.Message.
// Provide a callback that gets the pointer to the byte slice field and a
// valid decoder will be generated.
func Decode(f func(proto.Message) *[]byte) Decoder {
	return func(msg proto.Message, offset int, p []byte) ([]byte, error) {
		bytePtr := f(msg)
		copy(p, (*bytePtr)[offset:])
		return *bytePtr, nil
	}
}

// ChunkedEncoder ensures that data to encode is chunked at the proper size.
func ChunkedEncoder(enc Encoder, size int) Encoder {
	return func(msg proto.Message, p []byte) (int, error) {
		if len(p) > size {
			p = p[:size]
		}

		return enc(msg, p)
	}
}
