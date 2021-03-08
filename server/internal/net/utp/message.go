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

package utp

import (
	"bytes"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	lp "github.com/unit-io/unitdb/server/internal/net"
	pbx "github.com/unit-io/unitdb/server/proto"
)

type FixedHeader pbx.FixedHeader

type Message struct {
}

// Read unpacks the packet from the provided reader.
func (m *Message) Read(r io.Reader) (lp.LineProtocol, error) {
	var fh FixedHeader
	fh.unpack(r)

	// Check for empty Messages
	switch uint8(fh.MessageType) {
	case 0:
		// TODO fixe zero length message issue.
		return &lp.Pingreq{}, nil
	case lp.PINGREQ.Value():
		return &lp.Pingreq{}, nil
	case lp.DISCONNECT.Value():
		return &lp.Disconnect{}, nil
	}

	rawMsg := make([]byte, fh.MessageLength)
	_, err := io.ReadFull(r, rawMsg)
	if err != nil {
		return nil, err
	}

	// unpack the body
	var msg lp.LineProtocol
	if uint8(fh.FlowControl) !=lp.NONE.Value() {
		return unpackControlMessage(fh, rawMsg), nil
	}

	switch uint8(fh.MessageType) {
	case lp.CONNECT.Value():
		msg = unpackConnect(rawMsg)
	case lp.PUBLISH.Value():
		msg = unpackPublish(rawMsg)
	case lp.SUBSCRIBE.Value():
		msg = unpackSubscribe(rawMsg)
	case lp.UNSUBSCRIBE.Value():
		msg = unpackUnsubscribe(rawMsg)
	default:
		return nil, fmt.Errorf("message::Read: Invalid zero-length Message type %d", fh.MessageType)
	}

	return msg, nil
}

// Encode encodes the message into binary data
func (m *Message) Encode(msg lp.LineProtocol) (bytes.Buffer, error) {
	switch msg.Type() {
	case lp.DISCONNECT:
		return encodeDisconnect(*msg.(*lp.Disconnect))
	case lp.PUBLISH:
		return encodePublish(*msg.(*lp.Publish))
	case lp.FLOWCONTROL:
		return encodeControlMessage(*msg.(*lp.ControlMessage))
	default:
		return bytes.Buffer{}, fmt.Errorf("message::Encode: Invalid zero-length Message type %d", msg.Type())
	}
	return bytes.Buffer{}, nil
}

func (fh *FixedHeader) pack() bytes.Buffer {
	var head bytes.Buffer
	ph := pbx.FixedHeader(*fh)
	h, err := proto.Marshal(&ph)
	if err != nil {
		return head
	}
	size := encodeLength(len(h))
	head.Write(size)
	head.Write(h)
	return head
}

func (fh *FixedHeader) unpack(r io.Reader) error {
	fhSize, err := decodeLength(r)
	if err != nil {
		return err
	}

	// read FixedHeader
	head := make([]byte, fhSize)
	_, err = io.ReadFull(r, head)
	if err != nil {
		return err
	}

	var h pbx.FixedHeader
	proto.Unmarshal(head, &h)

	*fh = FixedHeader(h)
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
