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

// RelayRequest is pairing the Topic and Last parameter together
type RelayRequest struct {
	Topic string
	Last  string
}

// Relay tells the server which topics and last durations the client would like get data. The Delivery Mode for relay is EXPRESS.
type Relay struct {
	IsForwarded   bool
	MessageID     uint16
	RelayRequests []*RelayRequest
}

func (r *Relay) ToBinary() (bytes.Buffer, error) {
	var buf bytes.Buffer
	var reqs []*pbx.RelayRequest
	for _, req := range r.RelayRequests {
		r := pbx.RelayRequest{
			Topic: string(req.Topic),
			Last:  req.Last,
		}
		reqs = append(reqs, &r)
	}
	rel := pbx.Relay{
		MessageID:     int32(r.MessageID),
		RelayRequests: reqs,
	}
	rawMsg, err := proto.Marshal(&rel)
	if err != nil {
		return buf, err
	}
	fh := FixedHeader{MessageType: RELAY, MessageLength: len(rawMsg)}
	buf = fh.pack()
	_, err = buf.Write(rawMsg)
	return buf, err
}

func (r *Relay) FromBinary(fh FixedHeader, data []byte) {
	var rel pbx.Relay
	proto.Unmarshal(data, &rel)
	var reqs []*RelayRequest
	for _, req := range rel.RelayRequests {
		r := &RelayRequest{
			Topic: req.Topic,
			Last:  req.Last,
		}
		reqs = append(reqs, r)
	}

	r.MessageID = uint16(rel.MessageID)
	r.RelayRequests = reqs
}

// Type returns the Message type.
func (r *Relay) Type() MessageType {
	return RELAY
}

// Info returns DeliveryMode and MessageID of this Message.
func (r *Relay) Info() Info {
	return Info{DeliveryMode: 1, MessageID: r.MessageID}
}
