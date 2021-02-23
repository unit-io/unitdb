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

func encodeSuback(s lp.Suback) (bytes.Buffer, error) {
	var msg bytes.Buffer
	suback := pbx.Suback{
		MessageID: int32(s.MessageID),
	}
	pkt, err := proto.Marshal(&suback)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_SUBACK, MessageLength: int32(len(pkt))}
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
	fh := FixedHeader{MessageType: pbx.MessageType_UNSUBACK, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func unpackSubscribe(data []byte) lp.LineProtocol {
	var pkt pbx.Subscribe
	proto.Unmarshal(data, &pkt)
	proto.Unmarshal(data, &pkt)
	var subs []lp.Subscription
	for _, t := range pkt.Subscriptions {
		sub := lp.Subscription{}
		sub.Topic = t.Topic
		sub.Last = t.Last
		sub.DeliveryMode = uint8(t.DeliveryMode)
		subs = append(subs, sub)
	}

	return &lp.Subscribe{
		MessageID:     uint16(pkt.MessageID),
		Subscriptions: subs,
	}
}

func unpackUnsubscribe(data []byte) lp.LineProtocol {
	var pkt pbx.Unsubscribe
	proto.Unmarshal(data, &pkt)
	var subs []lp.Subscription
	for _, t := range pkt.Subscriptions {
		sub := lp.Subscription{}
		sub.Topic = t.Topic
		sub.Last = t.Last
		sub.DeliveryMode = uint8(t.DeliveryMode)
		subs = append(subs, sub)
	}

	return &lp.Unsubscribe{
		MessageID:     uint16(pkt.MessageID),
		Subscriptions: subs,
	}
}
