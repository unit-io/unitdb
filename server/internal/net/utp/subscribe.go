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

	"github.com/golang/protobuf/proto"
	lp "github.com/unit-io/unitdb/server/internal/net"
	pbx "github.com/unit-io/unitdb/server/proto"
)

func unpackSubscribe(data []byte) lp.LineProtocol {
	var sub pbx.Subscribe
	proto.Unmarshal(data, &sub)
	var subs []*lp.Subscription
	for _, t := range sub.Subscriptions {
		s := &lp.Subscription{}
		s.DeliveryMode = uint8(t.DeliveryMode)
		s.Delay = t.Delay
		s.Topic = []byte(t.Topic)
		subs = append(subs, s)
	}

	return &lp.Subscribe{
		MessageID:     uint16(sub.MessageID),
		Subscriptions: subs,
	}
}

func unpackUnsubscribe(data []byte) lp.LineProtocol {
	var unsub pbx.Unsubscribe
	proto.Unmarshal(data, &unsub)
	var subs []*lp.Subscription
	for _, t := range unsub.Subscriptions {
		s := &lp.Subscription{}
		s.DeliveryMode = uint8(t.DeliveryMode)
		s.Delay = t.Delay
		s.Topic = []byte(t.Topic)
		subs = append(subs, s)
	}

	return &lp.Unsubscribe{
		MessageID:     uint16(unsub.MessageID),
		Subscriptions: subs,
	}
}
