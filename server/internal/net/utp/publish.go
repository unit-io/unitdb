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

	"github.com/golang/protobuf/proto"
	lp "github.com/unit-io/unitdb/server/internal/net"
	pbx "github.com/unit-io/unitdb/server/proto"
)

func encodePublish(p lp.Publish) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pub := pbx.Publish{
		MessageID:    int32(p.MessageID),
		DeliveryMode: int32(p.DeliveryMode),
		Topic:        p.Topic,
		Payload:      p.Payload,
		Ttl:          p.Ttl,
	}
	pkt, err := proto.Marshal(&pub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBLISH, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodePubnew(p lp.Pubnew) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubnew := pbx.Pubnew{
		MessageID: int32(p.MessageID),
	}
	pkt, err := proto.Marshal(&pubnew)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBNEW, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodePubreceipt(p lp.Pubreceipt) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubrec := pbx.Pubreceipt{
		MessageID: int32(p.MessageID),
	}
	pkt, err := proto.Marshal(&pubrec)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBRECEIPT, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodePubcomplete(p lp.Pubcomplete) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubcomp := pbx.Pubcomplete{
		MessageID: int32(p.MessageID),
	}
	pkt, err := proto.Marshal(&pubcomp)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBCOMPLETE, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func unpackPublish(data []byte) lp.LineProtocol {
	var pkt pbx.Publish
	proto.Unmarshal(data, &pkt)

	return &lp.Publish{
		MessageID:    uint16(pkt.MessageID),
		DeliveryMode: uint8(pkt.DeliveryMode),
		Topic:        pkt.Topic,
		Payload:      pkt.Payload,
		Ttl:          pkt.Ttl,
	}
}

func unpackPubreceive(data []byte) lp.LineProtocol {
	var pkt pbx.Pubreceive
	proto.Unmarshal(data, &pkt)

	return &lp.Pubreceive{
		MessageID: uint16(pkt.MessageID),
	}
}

func unpackPubreceipt(data []byte) lp.LineProtocol {
	var pkt pbx.Pubreceipt
	proto.Unmarshal(data, &pkt)

	return &lp.Pubreceipt{
		MessageID: uint16(pkt.MessageID),
	}
}

func unpackPubcomplete(data []byte) lp.LineProtocol {
	var pkt pbx.Pubcomplete
	proto.Unmarshal(data, &pkt)
	return &lp.Pubcomplete{
		MessageID: uint16(pkt.MessageID),
	}
}
