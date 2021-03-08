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

// encodeControlMessage encodes the Control Message into binary data
func encodeControlMessage(c lp.ControlMessage) (bytes.Buffer, error) {
	var msg bytes.Buffer
	var err error
	var fh FixedHeader
	ctrl := pbx.ControlMessage{
		MessageID: int32(c.MessageID),
	}
	rawMsg, err := proto.Marshal(&ctrl)
	if err != nil {
		return msg, err
	}
	switch c.FlowControl {
	case lp.ACKNOWLEDGE:
		switch c.MessageType {
		case lp.CONNECT:
			connack := *c.Message.(*lp.ConnectAcknowledge)
				ack := pbx.ConnectAcknowledge{
		ReturnCode: int32(connack.ReturnCode),
		Epoch:      int32(connack.Epoch),
		ConnID:     int32(connack.ConnID),
	}
	rawAck, err := proto.Marshal(&ack)
	if err != nil {
		return msg, err
	}
	ctrl.Message = rawAck
	rawMsg, err = proto.Marshal(&ctrl)
	if err != nil {
		return msg, err
	}

			fh = FixedHeader{MessageType: pbx.MessageType_CONNECT, FlowControl: pbx.FlowControl_ACKNOWLEDGE, MessageLength: int32(len(rawMsg))}
		case lp.PUBLISH:
			fh = FixedHeader{MessageType: pbx.MessageType_PUBLISH, FlowControl: pbx.FlowControl_ACKNOWLEDGE, MessageLength: int32(len(rawMsg))}
		case lp.SUBSCRIBE:
			fh = FixedHeader{MessageType: pbx.MessageType_SUBSCRIBE, FlowControl: pbx.FlowControl_ACKNOWLEDGE, MessageLength: int32(len(rawMsg))}
		case lp.UNSUBSCRIBE:
			fh = FixedHeader{MessageType: pbx.MessageType_UNSUBSCRIBE, FlowControl: pbx.FlowControl_ACKNOWLEDGE, MessageLength: int32(len(rawMsg))}
		case lp.PINGREQ:
			fh = FixedHeader{MessageType: pbx.MessageType_PINGREQ, FlowControl: pbx.FlowControl_ACKNOWLEDGE, MessageLength: int32(len(rawMsg))}
		}
	case lp.NOTIFY:
		fh = FixedHeader{MessageType: pbx.MessageType_PUBLISH, FlowControl: pbx.FlowControl_NOTIFY, MessageLength: int32(len(rawMsg))}
	case lp.RECEIVE:
		fh = FixedHeader{MessageType: pbx.MessageType_PUBLISH, FlowControl: pbx.FlowControl_RECEIVE, MessageLength: int32(len(rawMsg))}
	case lp.RECEIPT:
		fh = FixedHeader{MessageType: pbx.MessageType_PUBLISH, FlowControl: pbx.FlowControl_RECEIPT, MessageLength: int32(len(rawMsg))}
	case lp.COMPLETE:
		fh = FixedHeader{MessageType: pbx.MessageType_PUBLISH, FlowControl: pbx.FlowControl_COMPLETE, MessageLength: int32(len(rawMsg))}
	}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func unpackControlMessage(fh FixedHeader, data []byte) lp.LineProtocol {
	var ctrl pbx.ControlMessage
	proto.Unmarshal(data, &ctrl)

		return &lp.ControlMessage{MessageID: uint16(ctrl.MessageID), MessageType: lp.MessageType(uint8(fh.MessageType)), FlowControl: lp.FlowControl(uint8(fh.FlowControl))}
}
