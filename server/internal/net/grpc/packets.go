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

package grpc

import (
	"bytes"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	lp "github.com/unit-io/unitdb/server/internal/net"
	pbx "github.com/unit-io/unitdb/server/proto"
)

type FixedHeader pbx.FixedHeader

type LineProto struct {
}

// ReadPacket unpacks the packet from the provided reader.
func (p *LineProto) ReadPacket(r io.Reader) (lp.Packet, error) {
	var fh FixedHeader
	fh.unpack(r)

	// Check for empty packets
	switch uint8(fh.MessageType) {
	case lp.PINGREQ:
		return &lp.Pingreq{}, nil
	case lp.PINGRESP:
		return &lp.Pingresp{}, nil
	case lp.DISCONNECT:
		return &lp.Disconnect{}, nil
	}

	msg := make([]byte, fh.RemainingLength)
	_, err := io.ReadFull(r, msg)
	if err != nil {
		return nil, err
	}

	// unpack the body
	var pkt lp.Packet
	switch uint8(fh.MessageType) {
	case lp.CONNECT:
		pkt = unpackConnect(msg)
	case lp.CONNACK:
		pkt = unpackConnack(msg)
	case lp.PUBLISH:
		pkt = unpackPublish(msg)
	case lp.PUBACK:
		pkt = unpackPuback(msg)
	case lp.PUBREC:
		pkt = unpackPubrec(msg)
	case lp.PUBREL:
		pkt = unpackPubrel(msg)
	case lp.PUBCOMP:
		pkt = unpackPubcomp(msg)
	case lp.SUBSCRIBE:
		pkt = unpackSubscribe(msg)
	case lp.SUBACK:
		pkt = unpackSuback(msg)
	case lp.UNSUBSCRIBE:
		pkt = unpackUnsubscribe(msg)
	case lp.UNSUBACK:
		pkt = unpackUnsuback(msg)
	default:
		return nil, fmt.Errorf("Invalid zero-length packet with type %d", fh.MessageType)
	}

	return pkt, nil
}

// Encode encodes the message into binary data
func (p *LineProto) Encode(pkt lp.Packet) (bytes.Buffer, error) {
	switch pkt.Type() {
	case lp.PINGREQ:
		return encodePingreq(*pkt.(*lp.Pingreq))
	case lp.PINGRESP:
		return encodePingresp(*pkt.(*lp.Pingresp))
	case lp.CONNECT:
		return encodeConnect(*pkt.(*lp.Connect))
	case lp.CONNACK:
		return encodeConnack(*pkt.(*lp.Connack))
	case lp.DISCONNECT:
		return encodeDisconnect(*pkt.(*lp.Disconnect))
	case lp.SUBSCRIBE:
		return encodeSubscribe(*pkt.(*lp.Subscribe))
	case lp.SUBACK:
		return encodeSuback(*pkt.(*lp.Suback))
	case lp.UNSUBSCRIBE:
		return encodeUnsubscribe(*pkt.(*lp.Unsubscribe))
	case lp.UNSUBACK:
		return encodeUnsuback(*pkt.(*lp.Unsuback))
	case lp.PUBLISH:
		return encodePublish(*pkt.(*lp.Publish))
	case lp.PUBACK:
		return encodePuback(*pkt.(*lp.Puback))
	case lp.PUBREC:
		return encodePubrec(*pkt.(*lp.Pubrec))
	case lp.PUBREL:
		return encodePubrel(*pkt.(*lp.Pubrel))
	case lp.PUBCOMP:
		return encodePubcomp(*pkt.(*lp.Pubcomp))
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
