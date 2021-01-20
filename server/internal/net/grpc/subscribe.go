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

	"github.com/golang/protobuf/proto"
	lp "github.com/unit-io/unitdb/server/internal/net"
	pbx "github.com/unit-io/unitdb/server/proto"
)

func encodeSubscribe(s lp.Subscribe) (bytes.Buffer, error) {
	var msg bytes.Buffer
	var subs []*pbx.Subscriber
	for _, t := range s.Subscriptions {
		sub := &pbx.Subscriber{}
		sub.Topic = string(t.Topic)
		sub.Qos = int32(t.Qos)
		subs = append(subs, sub)
	}
	sub := pbx.Subscribe{
		MessageID:   int32(s.MessageID),
		Subscribers: subs,
	}
	pkt, err := proto.Marshal(&sub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBSCRIBE, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodeSuback(s lp.Suback) (bytes.Buffer, error) {
	var msg bytes.Buffer
	var qoss []int32
	for _, q := range s.Qos {
		qoss = append(qoss, int32(q))
	}
	suback := pbx.Suback{
		MessageID: int32(s.MessageID),
		Qos:       qoss,
	}
	pkt, err := proto.Marshal(&suback)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBACK, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	fmt.Println("suback: messageID ", s.MessageID)
	return msg, err
}

func encodeUnsubscribe(u lp.Unsubscribe) (bytes.Buffer, error) {
	var msg bytes.Buffer
	var subs []*pbx.Subscriber
	for _, t := range u.Subscriptions {
		sub := &pbx.Subscriber{}
		sub.Topic = string(t.Topic)
		sub.Qos = int32(t.Qos)
		subs = append(subs, sub)
	}
	unsub := pbx.Unsubscribe{
		MessageID:   int32(u.MessageID),
		Subscribers: subs,
	}
	pkt, err := proto.Marshal(&unsub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBSCRIBE, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodeUnsuback(u lp.Unsuback) (bytes.Buffer, error) {
	var msg bytes.Buffer
	unusuback := pbx.Unsuback{
		MessageID: int32(u.MessageID),
	}
	pkt, err := proto.Marshal(&unusuback)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBACK, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func unpackSubscribe(data []byte) lp.Packet {
	var pkt pbx.Subscribe
	proto.Unmarshal(data, &pkt)
	var topics []lp.TopicQOSTuple
	for _, sub := range pkt.Subscribers {
		var t lp.TopicQOSTuple
		t.Topic = []byte(sub.Topic)
		t.Qos = uint8(sub.Qos)
		topics = append(topics, t)
	}
	return &lp.Subscribe{
		MessageID:     uint16(pkt.MessageID),
		Subscriptions: topics,
	}
}

func unpackSuback(data []byte) lp.Packet {
	var pkt pbx.Suback
	proto.Unmarshal(data, &pkt)
	var qoses []uint8
	//is this efficient
	for _, qos := range pkt.Qos {
		qoses = append(qoses, uint8(qos))
	}
	return &lp.Suback{
		MessageID: uint16(pkt.MessageID),
		Qos:       qoses,
	}
}

func unpackUnsubscribe(data []byte) lp.Packet {
	var pkt pbx.Unsubscribe
	proto.Unmarshal(data, &pkt)
	var topics []lp.TopicQOSTuple
	for _, sub := range pkt.Subscribers {
		var t lp.TopicQOSTuple
		t.Topic = []byte(sub.Topic)
		topics = append(topics, t)
	}
	return &lp.Unsubscribe{
		MessageID:     uint16(pkt.MessageID),
		Subscriptions: topics,
	}
}

func unpackUnsuback(data []byte) lp.Packet {
	var pkt pbx.Unsuback
	proto.Unmarshal(data, &pkt)
	return &lp.Unsuback{
		MessageID: uint16(pkt.MessageID),
	}
}
