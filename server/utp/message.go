/*
 * Copyright 2021 Saffat Technologies, Ltd.
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

package utp

import (
	"bytes"
	"io"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
)

// MessageType represents a Message type
type MessageType uint8

const (
	// Message
	CONNECT MessageType = iota + 1
	PUBLISH
	RELAY
	SUBSCRIBE
	UNSUBSCRIBE
	PINGREQ
	DISCONNECT
	FLOWCONTROL
)

// Below are the const definitions for error codes returned by
// Connect()
const (
	Accepted                     = 0x00
	ErrRefusedBadProtocolVersion = 0x01
	ErrRefusedIDRejected         = 0x02
	ErrRefusedbADID              = 0x03
	ErrRefusedServerUnavailable  = 0x04
	ErrNotAuthorised             = 0x05
	ErrBadRequest                = 0x06
)

// FixedHeader
type FixedHeader struct {
	MessageType   MessageType
	FlowControl   FlowControl
	MessageLength int
}

// Info returns Qos and MessageID by the Info() function called on the Packet
type Info struct {
	DeliveryMode uint8
	MessageID    uint16
}

func (fh *FixedHeader) pack() bytes.Buffer {
	var head bytes.Buffer
	ph := pbx.FixedHeader{
		MessageType:   pbx.MessageType(fh.MessageType),
		FlowControl:   pbx.FlowControl(fh.FlowControl),
		MessageLength: int32(fh.MessageLength),
	}
	h, err := proto.Marshal(&ph)
	if err != nil {
		return head
	}
	size := encodeLength(len(h))
	head.Write(size)
	head.Write(h)
	return head
}

func (fh *FixedHeader) FromBinary(r io.Reader) error {
	fhSize, err := decodeLength(r)
	if err != nil {
		return err
	}

	// read FixedHeader
	head := make([]byte, fhSize)
	if _, err := io.ReadFull(r, head); err != nil {
		return err
	}

	var h pbx.FixedHeader
	proto.Unmarshal(head, &h)

	*fh = FixedHeader{
		MessageType:   MessageType(h.MessageType),
		FlowControl:   FlowControl(h.FlowControl),
		MessageLength: int(h.MessageLength),
	}

	return nil
}

func encodeLength(length int) []byte {
	var encLength []byte
	for {
		digit := byte(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		encLength = append(encLength, digit)
		if length == 0 {
			break
		}
	}
	return encLength
}

func decodeLength(r io.Reader) (int, error) {
	var rLength uint32
	var multiplier uint32
	b := make([]byte, 1)
	for multiplier < 27 { //fix: Infinite '(digit & 128) == 1' will cause the dead loop
		_, err := io.ReadFull(r, b)
		if err != nil {
			return 0, err
		}

		digit := b[0]
		rLength |= uint32(digit&127) << multiplier
		if (digit & 128) == 0 {
			break
		}
		multiplier += 7
	}
	return int(rLength), nil
}
