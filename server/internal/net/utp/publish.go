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
	var msgs []*pbx.PublishMessage
	for _, m := range p.Messages {
		pubMsg := &pbx.PublishMessage{
			Topic:   string(m.Topic),
			Payload: m.Payload,
			Ttl:     m.Ttl,
		}
		msgs = append(msgs, pubMsg)
	}
	pub := pbx.Publish{
		MessageID:    int32(p.MessageID),
		DeliveryMode: int32(p.DeliveryMode),
		Messages:     msgs,
	}
	rawMsg, err := proto.Marshal(&pub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBLISH, MessageLength: int32(len(rawMsg))}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func unpackPublish(data []byte) lp.LineProtocol {
	var pub pbx.Publish
	proto.Unmarshal(data, &pub)
	var msgs []*lp.PublishMessage
	for _, m := range pub.Messages {
		pubMsg := &lp.PublishMessage{
			Topic:   []byte(m.Topic),
			Payload: m.Payload,
			Ttl:     m.Ttl,
		}
		msgs = append(msgs, pubMsg)
	}
	return &lp.Publish{
		MessageID:    uint16(pub.MessageID),
		DeliveryMode: uint8(pub.DeliveryMode),
		Messages:     msgs,
	}
}
