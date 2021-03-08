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

func encodeDisconnect(d lp.Disconnect) (bytes.Buffer, error) {
	var msg bytes.Buffer
	disc := pbx.Disconnect{}
	rawMsg, err := proto.Marshal(&disc)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_DISCONNECT, MessageLength: int32(len(rawMsg))}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func unpackConnect(data []byte) lp.LineProtocol {
	var conn pbx.Connect
	proto.Unmarshal(data, &conn)

	connect := &lp.Connect{
		Version:             uint8(conn.Version),
		InsecureFlag:        conn.InsecureFlag,
		ClientID:            []byte(conn.ClientID),
		KeepAlive:           uint16(conn.KeepAlive),
		CleanSessFlag:       conn.CleanSessFlag,
		SessKey:             uint32(conn.SessKey),
		Username:            conn.Username,
		Password:            conn.Password,
		BatchDuration:       conn.BatchDuration,
		BatchByteThreshold:  conn.BatchByteThreshold,
		BatchCountThreshold: conn.BatchCountThreshold,
	}

	return connect
}

func unpackConnectAcknowledge(data []byte) lp.LineProtocol {
	var connack pbx.ConnectAcknowledge
	proto.Unmarshal(data, &connack)

	return &lp.ConnectAcknowledge{
		ReturnCode: uint8(connack.ReturnCode),
	}
}
