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
	pbx "github.com/unit-io/unitdb/server/proto"
)

// Connect represents a connect Message.
type Connect struct {
	Version             int32
	InsecureFlag        bool
	ClientID            string
	KeepAlive           int32
	CleanSessFlag       bool
	SessKey             int32
	Username            string
	Password            []byte
	BatchDuration       int32
	BatchByteThreshold  int32
	BatchCountThreshold int32
}

// ConnectAcknowledge represents a CONNECT Acknowledge Message.
// 0x00 connection accepted
// 0x01 refused: unacceptable proto version
// 0x02 refused: identifier rejected
// 0x03 refused: unacceptable identifier, access not allowed
// 0x04 refused server unavailiable
// 0x05 not authorized
// 0x06 bad request
type ConnectAcknowledge struct {
	ReturnCode uint8
	Epoch      int32
	ConnID     int32
}

// Pingreq is a keepalive
type Pingreq struct {
}

//Disconnect is to signal you want to cease communications with the server
type Disconnect struct {
	MessageID uint16
}

func (c *Connect) ToBinary() (bytes.Buffer, error) {
	var msg bytes.Buffer
	conn := pbx.Connect{
		Version:             c.Version,
		InsecureFlag:        c.InsecureFlag,
		ClientID:            c.ClientID,
		KeepAlive:           c.KeepAlive,
		CleanSessFlag:       c.CleanSessFlag,
		SessKey:             c.SessKey,
		Username:            c.Username,
		Password:            c.Password,
		BatchDuration:       c.BatchDuration,
		BatchByteThreshold:  c.BatchByteThreshold,
		BatchCountThreshold: c.BatchCountThreshold,
	}
	rawMsg, err := proto.Marshal(&conn)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: CONNECT, MessageLength: len(rawMsg)}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func (c *Connect) FromBinary(fh FixedHeader, data []byte) {
	var conn pbx.Connect
	proto.Unmarshal(data, &conn)

	c.Version = conn.Version
	c.InsecureFlag = conn.InsecureFlag
	c.ClientID = conn.ClientID
	c.KeepAlive = conn.KeepAlive
	c.CleanSessFlag = conn.CleanSessFlag
	c.SessKey = conn.SessKey
	c.Username = conn.Username
	c.Password = conn.Password
	c.BatchDuration = conn.BatchDuration
	c.BatchByteThreshold = conn.BatchByteThreshold
	c.BatchCountThreshold = conn.BatchCountThreshold
}

// Type returns the Message type.
func (c *Connect) Type() MessageType {
	return CONNECT
}

// Info returns DeliveryMode and MessageID of this Message.
func (c *Connect) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

func (c *ConnectAcknowledge) ToBinary() (bytes.Buffer, error) {
	var msg bytes.Buffer
	connack := pbx.ConnectAcknowledge{
		ReturnCode: int32(c.ReturnCode),
		Epoch:      c.Epoch,
		ConnID:     c.ConnID,
	}
	rawMsg, err := proto.Marshal(&connack)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: CONNECT, FlowControl: ACKNOWLEDGE, MessageLength: len(rawMsg)}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func (c *ConnectAcknowledge) FromBinary(fh FixedHeader, data []byte) {
	var connack pbx.ConnectAcknowledge
	proto.Unmarshal(data, &connack)

	c.ReturnCode = uint8(connack.ReturnCode)
	c.Epoch = connack.Epoch
	c.ConnID = connack.ConnID
}

// Type returns the Message type.
func (c *ConnectAcknowledge) Type() MessageType {
	return FLOWCONTROL
}

// Info returns DeliveryMode and MessageID of this Message.
func (c *ConnectAcknowledge) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

func (p *Pingreq) ToBinary() (bytes.Buffer, error) {
	var msg bytes.Buffer
	var pingreq pbx.PingRequest
	rawMsg, err := proto.Marshal(&pingreq)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: PINGREQ, MessageLength: len(rawMsg)}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func (p *Pingreq) FromBinary(fh FixedHeader, data []byte) {
	var pingreq pbx.PingRequest
	proto.Unmarshal(data, &pingreq)
}

// Type returns the Message type.
func (p *Pingreq) Type() MessageType {
	return PINGREQ
}

// Info returns DeliveryMode and MessageID of this Message.
func (p *Pingreq) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

func (d *Disconnect) ToBinary() (bytes.Buffer, error) {
	var msg bytes.Buffer
	var disc pbx.Disconnect
	rawMsg, err := proto.Marshal(&disc)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: DISCONNECT, MessageLength: len(rawMsg)}
	msg = fh.pack()
	_, err = msg.Write(rawMsg)
	return msg, err
}

func (d *Disconnect) FromBinary(fh FixedHeader, data []byte) {
	var disc pbx.Disconnect
	proto.Unmarshal(data, &disc)

	d.MessageID = uint16(disc.MessageID)
}

// Type returns the Message type.
func (d *Disconnect) Type() MessageType {
	return DISCONNECT
}

// Info returns DeliveryMode and MessageID of this Message.
func (d *Disconnect) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}
