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
	pbx "github.com/unit-io/unitdb/server/proto"
)

// PublishMessage reprensents a publish Message
type PublishMessage struct {
	Topic   string
	Payload []byte
	Ttl     string
}

// Publish represents a publish Messages.
type Publish struct {
	IsForwarded  bool
	MessageID    uint16
	DeliveryMode uint8
	Messages     []*PublishMessage
}

func (p *Publish) ToBinary() (bytes.Buffer, error) {
	var msg bytes.Buffer

	var pubMessages []*pbx.PublishMessage
	for _, m := range p.Messages {
		var pubMsg pbx.PublishMessage
		pubMsg.Topic = string(m.Topic)
		pubMsg.Payload = m.Payload
		pubMsg.Ttl = m.Ttl
		pubMessages = append(pubMessages, &pubMsg)
	}
	pub := pbx.Publish{
		MessageID:    int32(p.MessageID),
		DeliveryMode: int32(p.DeliveryMode),
		Messages:     pubMessages,
	}
	rawMsg, err := proto.Marshal(&pub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: PUBLISH, MessageLength: len(rawMsg)}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func (p *Publish) FromBinary(fh FixedHeader, data []byte) {
	var pub pbx.Publish
	proto.Unmarshal(data, &pub)
	var pubMessages []*PublishMessage
	for _, m := range pub.Messages {
		pubMsg := &PublishMessage{
			Topic:   m.Topic,
			Payload: m.Payload,
			Ttl:     m.Ttl,
		}
		pubMessages = append(pubMessages, pubMsg)
	}
	p.MessageID = uint16(pub.MessageID)
	p.DeliveryMode = uint8(pub.DeliveryMode)
	p.Messages = pubMessages
}

// Type returns the Message type.
func (p *Publish) Type() MessageType {
	return PUBLISH
}

// Info returns DeliveryMode and MessageID of this Message.
func (p *Publish) Info() Info {
	return Info{DeliveryMode: p.DeliveryMode, MessageID: p.MessageID}
}
