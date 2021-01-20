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

	"github.com/golang/protobuf/proto"
	lp "github.com/unit-io/unitdb/server/internal/net"
	pbx "github.com/unit-io/unitdb/server/proto"
)

func encodeConnect(c lp.Connect) (bytes.Buffer, error) {
	var msg bytes.Buffer
	conn := pbx.Conn{
		ProtoName:     string(c.ProtoName),
		Version:       int32(c.Version),
		UsernameFlag:  c.UsernameFlag,
		PasswordFlag:  c.PasswordFlag,
		CleanSessFlag: c.CleanSessFlag,
		KeepAlive:     int32(c.KeepAlive),
		ClientID:      string(c.ClientID),
		Username:      string(c.Username),
		Password:      string(c.Password),
	}

	pkt, err := proto.Marshal(&conn)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_CONNECT, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

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
	fh := FixedHeader{MessageType: pbx.MessageType_CONNACK, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodePingreq(p lp.Pingreq) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pingreq := pbx.Pingreq{}
	pkt, err := proto.Marshal(&pingreq)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PINGREQ, RemainingLength: int32(len(pkt))}
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
	fh := FixedHeader{MessageType: pbx.MessageType_PINGRESP, RemainingLength: int32(len(pkt))}
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
	fh := FixedHeader{MessageType: pbx.MessageType_DISCONNECT, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func unpackConnect(data []byte) lp.Packet {
	var pkt pbx.Conn
	proto.Unmarshal(data, &pkt)

	connect := &lp.Connect{
		ProtoName:     []byte(pkt.ProtoName),
		Version:       uint8(pkt.Version),
		KeepAlive:     uint16(pkt.KeepAlive),
		ClientID:      []byte(pkt.ClientID),
		InsecureFlag:  pkt.InsecureFlag,
		UsernameFlag:  pkt.UsernameFlag,
		PasswordFlag:  pkt.PasswordFlag,
		CleanSessFlag: pkt.CleanSessFlag,
	}

	if connect.UsernameFlag {
		connect.Username = []byte(pkt.Username)
	}

	if connect.PasswordFlag {
		connect.Password = []byte(pkt.Password)
	}
	return connect
}

func unpackConnack(data []byte) lp.Packet {
	var pkt pbx.Connack
	proto.Unmarshal(data, &pkt)

	return &lp.Connack{
		ReturnCode: uint8(pkt.ReturnCode),
	}
}

func unpackPingreq(data []byte) lp.Packet {
	return &lp.Pingreq{}
}

func unpackPingresp(data []byte) lp.Packet {
	return &lp.Pingresp{}
}

func unpackDisconnect(data []byte) lp.Packet {
	return &lp.Disconnect{}
}
