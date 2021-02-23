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

func encodeConnack(c lp.Connack) (bytes.Buffer, error) {
	var msg bytes.Buffer
	connack := pbx.Connack{
		ReturnCode: int32(c.ReturnCode),
		ConnID:     int32(c.ConnID),
	}
	pkt, err := proto.Marshal(&connack)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_CONNACK, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodePingresp(p lp.Pingresp) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pingresp := pbx.Pingresp{}
	pkt, err := proto.Marshal(&pingresp)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PINGRESP, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodeDisconnect(d lp.Disconnect) (bytes.Buffer, error) {
	var msg bytes.Buffer
	disc := pbx.Disconnect{}
	pkt, err := proto.Marshal(&disc)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_DISCONNECT, MessageLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func unpackConnect(data []byte) lp.LineProtocol {
	var pkt pbx.Conn
	proto.Unmarshal(data, &pkt)

	connect := &lp.Connect{
		ProtoName:     pkt.ProtoName,
		Version:       uint8(pkt.Version),
		KeepAlive:     uint16(pkt.KeepAlive),
		ClientID:      pkt.ClientID,
		InsecureFlag:  pkt.InsecureFlag,
		CleanSessFlag: pkt.CleanSessFlag,
		Username:      pkt.Username,
		Password:      pkt.Password,
	}

	return connect
}

func unpackConnack(data []byte) lp.LineProtocol {
	var pkt pbx.Connack
	proto.Unmarshal(data, &pkt)

	return &lp.Connack{
		ReturnCode: uint8(pkt.ReturnCode),
	}
}
