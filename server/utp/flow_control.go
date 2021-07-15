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
	"bytes"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
)

// FlowControl represents FlowControl Message type
type FlowControl uint8

const (
	// Flow Control
	NONE FlowControl = iota
	ACKNOWLEDGE
	NOTIFY
	RECEIVE
	RECEIPT
	COMPLETE
)

type ControlMessage struct {
	MessageID   uint16
	MessageType MessageType
	FlowControl FlowControl
	Message     []byte
}

// ToBinary encodes the Control Message into binary data
func (c *ControlMessage) ToBinary() (bytes.Buffer, error) {
	var msg bytes.Buffer
	var err error
	var fh FixedHeader
	ctrl := pbx.ControlMessage{
		MessageID: int32(c.MessageID),
		Message:   c.Message,
	}
	rawMsg, err := proto.Marshal(&ctrl)
	if err != nil {
		return msg, err
	}
	switch c.FlowControl {
	case ACKNOWLEDGE:
		switch c.MessageType {
		case CONNECT:
			fh = FixedHeader{MessageType: CONNECT, FlowControl: ACKNOWLEDGE, MessageLength: len(rawMsg)}
		case PUBLISH:
			fh = FixedHeader{MessageType: PUBLISH, FlowControl: ACKNOWLEDGE, MessageLength: len(rawMsg)}
		case RELAY:
			fh = FixedHeader{MessageType: RELAY, FlowControl: ACKNOWLEDGE, MessageLength: len(rawMsg)}
		case SUBSCRIBE:
			fh = FixedHeader{MessageType: SUBSCRIBE, FlowControl: ACKNOWLEDGE, MessageLength: len(rawMsg)}
		case UNSUBSCRIBE:
			fh = FixedHeader{MessageType: UNSUBSCRIBE, FlowControl: ACKNOWLEDGE, MessageLength: len(rawMsg)}
		case PINGREQ:
			fh = FixedHeader{MessageType: PINGREQ, FlowControl: ACKNOWLEDGE, MessageLength: len(rawMsg)}
		}
	case NOTIFY:
		fh = FixedHeader{MessageType: PUBLISH, FlowControl: NOTIFY, MessageLength: len(rawMsg)}
	case RECEIVE:
		fh = FixedHeader{MessageType: PUBLISH, FlowControl: RECEIVE, MessageLength: len(rawMsg)}
	case RECEIPT:
		fh = FixedHeader{MessageType: PUBLISH, FlowControl: RECEIPT, MessageLength: len(rawMsg)}
	case COMPLETE:
		fh = FixedHeader{MessageType: PUBLISH, FlowControl: COMPLETE, MessageLength: len(rawMsg)}
	}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func (c *ControlMessage) FromBinary(fh FixedHeader, data []byte) {
	var ctrl pbx.ControlMessage
	proto.Unmarshal(data, &ctrl)

	c.MessageID = uint16(ctrl.MessageID)
	c.MessageType = fh.MessageType
	c.FlowControl = fh.FlowControl
	c.Message = ctrl.Message
}

// Type returns the Message type.
func (c *ControlMessage) Type() MessageType {
	return FLOWCONTROL
}

// Info returns DeliveryMode and MessageID of this Message.
func (c *ControlMessage) Info() Info {
	return Info{DeliveryMode: 1, MessageID: c.MessageID}
}
