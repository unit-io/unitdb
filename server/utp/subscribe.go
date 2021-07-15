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
	// "bytes"

	"bytes"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
)

// Subscription is a struct for pairing the delivery mode and topic together
// for the delivery mode's pairs in unsubscribe and subscribe
type Subscription struct {
	DeliveryMode uint8
	Delay        int32
	Topic        string
}

// Subscribe tells the server which topics the client would like to subscribe to
type Subscribe struct {
	IsForwarded   bool
	MessageID     uint16
	Subscriptions []*Subscription
}

// Unsubscribe is the Message to send if you don't want to subscribe to a topic anymore
type Unsubscribe struct {
	IsForwarded   bool
	MessageID     uint16
	Subscriptions []*Subscription
}

func (s *Subscribe) ToBinary() (bytes.Buffer, error) {
	var msg bytes.Buffer
	var subs []*pbx.Subscription
	for _, subscription := range s.Subscriptions {
		var sub pbx.Subscription
		sub.DeliveryMode = int32(subscription.DeliveryMode)
		sub.Delay = subscription.Delay
		sub.Topic = string(subscription.Topic)
		subs = append(subs, &sub)
	}
	sub := pbx.Subscribe{
		MessageID:     int32(s.MessageID),
		Subscriptions: subs,
	}
	rawMsg, err := proto.Marshal(&sub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: SUBSCRIBE, MessageLength: len(rawMsg)}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func (s *Subscribe) FromBinary(fh FixedHeader, data []byte) {
	var sub pbx.Subscribe
	proto.Unmarshal(data, &sub)
	var subs []*Subscription
	for _, subscription := range sub.Subscriptions {
		sub := &Subscription{}
		sub.DeliveryMode = uint8(subscription.DeliveryMode)
		sub.Delay = subscription.Delay
		sub.Topic = subscription.Topic
		subs = append(subs, sub)
	}

	s.MessageID = uint16(sub.MessageID)
	s.Subscriptions = subs
}

// Type returns the Message type.
func (s *Subscribe) Type() MessageType {
	return SUBSCRIBE
}

// Info returns DeliveryMode and MessageID of this Message.
func (s *Subscribe) Info() Info {
	return Info{DeliveryMode: 1, MessageID: s.MessageID}
}

func (u *Unsubscribe) ToBinary() (bytes.Buffer, error) {
	var msg bytes.Buffer
	var subs []*pbx.Subscription
	for _, subscription := range u.Subscriptions {
		var sub pbx.Subscription
		sub.DeliveryMode = int32(subscription.DeliveryMode)
		sub.Delay = subscription.Delay
		sub.Topic = string(subscription.Topic)
		subs = append(subs, &sub)
	}
	unsub := pbx.Unsubscribe{
		MessageID:     int32(u.MessageID),
		Subscriptions: subs,
	}
	rawMsg, err := proto.Marshal(&unsub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: UNSUBSCRIBE, MessageLength: len(rawMsg)}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func (u *Unsubscribe) FromBinary(fh FixedHeader, data []byte) {
	var unsub pbx.Unsubscribe
	proto.Unmarshal(data, &unsub)
	var subs []*Subscription
	for _, subscription := range unsub.Subscriptions {
		sub := &Subscription{}
		sub.DeliveryMode = uint8(subscription.DeliveryMode)
		sub.Delay = subscription.Delay
		sub.Topic = subscription.Topic
		subs = append(subs, sub)
	}

	u.MessageID = uint16(unsub.MessageID)
	u.Subscriptions = subs
}

// Type returns the Message type.
func (u *Unsubscribe) Type() MessageType {
	return UNSUBSCRIBE
}

// Info returns DeliveryMode and MessageID of this Message.
func (u *Unsubscribe) Info() Info {
	return Info{DeliveryMode: 1, MessageID: u.MessageID}
}
