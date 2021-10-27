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

package net

import (
	"bytes"
	"fmt"
	"io"

	"github.com/unit-io/unitdb/server/utp"
)

// MessagePack is the interface for all Messages
type MessagePack interface {
	ToBinary() (bytes.Buffer, error)
	FromBinary(fh utp.FixedHeader, data []byte)
	Type() utp.MessageType
	Info() utp.Info
}

// Read unpacks the packet from the provided reader.
func Read(r io.Reader) (MessagePack, error) {
	var fh utp.FixedHeader
	if err := fh.FromBinary(r); err != nil {
		return nil, err
	}

	// Check for empty Messages
	switch fh.MessageType {
	case 0:
		// TODO fixe zero length message issue.
		return &utp.Pingreq{}, nil
	case utp.PINGREQ:
		return &utp.Pingreq{}, nil
	case utp.DISCONNECT:
		return &utp.Disconnect{}, nil
	}

	rawMsg := make([]byte, fh.MessageLength)
	_, err := io.ReadFull(r, rawMsg)
	if err != nil {
		return nil, err
	}

	// unpack the body
	var pack MessagePack
	if fh.FlowControl != utp.NONE {
		pack = &utp.ControlMessage{}
		pack.FromBinary(fh, rawMsg)
		return pack, nil
	}
	switch fh.MessageType {
	case utp.CONNECT:
		pack = &utp.Connect{}
	case utp.PUBLISH:
		pack = &utp.Publish{}
	case utp.RELAY:
		pack = &utp.Relay{}
	case utp.SUBSCRIBE:
		pack = &utp.Subscribe{}
	case utp.UNSUBSCRIBE:
		pack = &utp.Unsubscribe{}
	default:
		return nil, fmt.Errorf("message::Read: Invalid zero-length packet type %d", fh.MessageType)
	}

	pack.FromBinary(fh, rawMsg)
	return pack, nil
}

// Encode encodes the message into binary data
func Encode(pack MessagePack) (bytes.Buffer, error) {
	switch pack.Type() {
	case utp.DISCONNECT:
		return pack.(*utp.Disconnect).ToBinary()
	case utp.PUBLISH:
		return pack.(*utp.Publish).ToBinary()
	case utp.FLOWCONTROL:
		return pack.(*utp.ControlMessage).ToBinary()
	default:
		return bytes.Buffer{}, fmt.Errorf("message::Encode: Invalid zero-length packet type %d", pack.Type())
	}
}
