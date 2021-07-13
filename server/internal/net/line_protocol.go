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

package net

import (
	"bytes"
	"io"
)

// DeliverMode represents a delivery mode of a Message.
type DeliveryMode uint8

// MessageType represents a Message type
type MessageType uint8

// FlowControl represents FlowControl Message type
type FlowControl uint8

const (
	// Messages
	CONNECT MessageType = iota + 1
	PUBLISH
	RELAY
	SUBSCRIBE
	UNSUBSCRIBE
	PINGREQ
	DISCONNECT
	FLOWCONTROL
)
const (
	// Flow Control
	NONE FlowControl = iota
	ACKNOWLEDGE
	NOTIFY
	RECEIVE
	RECEIPT
	COMPLETE
)
const (
	EXPRESS DeliveryMode = iota
	RELIABLE
	BATCH
)

func (t MessageType) Value() uint8 {
	return uint8(t)
}

func (t FlowControl) Value() uint8 {
	return uint8(t)
}

// LineProtocol is the interface all our Messages in the line protocol will be implementing
type LineProtocol interface {
	Type() MessageType
	Info() Info
}

// Info returns DeliveryMode and MessageID by the Info() function called on the Message
type Info struct {
	DeliveryMode uint8
	MessageID    uint16
}

// FixedHeader
type FixedHeader struct {
	MessageType    uint8
	FlowControl    uint8
	MesssageLength int
}

// Connect represents a connect Message.
type Connect struct {
	Version             uint8
	InsecureFlag        bool
	ClientID            []byte
	KeepAlive           uint16
	CleanSessFlag       bool
	SessKey             uint32
	Username            string
	Password            []byte
	BatchDuration       int32
	BatchByteThreshold  int32
	BatchCountThreshold int32

	LineProtocol
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
	Epoch      uint32
	ConnID     uint32

	LineProtocol
}

// Pingreq is a keepalive
type Pingreq struct {
	LineProtocol
}

//Disconnect is to signal you want to cease communications with the server
type Disconnect struct {
	LineProtocol
}

// PublishMessage reprensents a publish Message
type PublishMessage struct {
	Topic   []byte
	Payload []byte
	Ttl     string
}

// Publish represents a publish Messages.
type Publish struct {
	IsForwarded  bool
	MessageID    uint16
	DeliveryMode uint8
	Messages     []*PublishMessage

	LineProtocol
}

// RelayRequest is pairing the Topic and Last parameter together
type RelayRequest struct {
	Topic []byte
	Last  string
}

// Relay tells the server which topics and last durations the client would like get data. The Delivery Mode for relay is EXPRESS.
type Relay struct {
	IsForwarded   bool
	MessageID     uint16
	RelayRequests []*RelayRequest
}

// Subscription is a struct for pairing the delivery mode and topic together
// for the delivery mode's pairs in unsubscribe and subscribe
type Subscription struct {
	DeliveryMode uint8
	Delay        int32
	Topic        []byte
	Last         string
}

// Subscribe tells the server which topics the client would like to subscribe to
type Subscribe struct {
	IsForwarded   bool
	MessageID     uint16
	Subscriptions []*Subscription

	LineProtocol
}

// Unsubscribe is the Message to send if you don't want to subscribe to a topic anymore
type Unsubscribe struct {
	IsForwarded   bool
	MessageID     uint16
	Subscriptions []*Subscription

	LineProtocol
}

// ControlMessage is to send a Control Message in reponse to another Message Flow.
type ControlMessage struct {
	MessageID   uint16
	MessageType MessageType
	FlowControl FlowControl
	Message     LineProtocol
}

type ProtoAdapter interface {
	Read(r io.Reader) (LineProtocol, error)
	Encode(msg LineProtocol) (bytes.Buffer, error)
}

func Read(adp ProtoAdapter, r io.Reader) (LineProtocol, error) {
	return adp.Read(r)
}

func Encode(adp ProtoAdapter, msg LineProtocol) (bytes.Buffer, error) {
	return adp.Encode(msg)
}

// Type returns the Connect Message type.
func (c *Connect) Type() MessageType {
	return CONNECT
}

// Info returns DeliveryMode and MessageID of this Message.
func (c *Connect) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

// Type returns the Pingreq Message type.
func (p *Pingreq) Type() MessageType {
	return PINGREQ
}

// Info returns DeliveryMode and MessageID of this Message.
func (p *Pingreq) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

// Type returns the Disconnect Message type.
func (d *Disconnect) Type() MessageType {
	return DISCONNECT
}

// Info returns DeliveryMode and MessageID of this Message.
func (d *Disconnect) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

// Type returns the Publish Message type.
func (p *Publish) Type() MessageType {
	return PUBLISH
}

// Info returns DeliveryMode and MessageID of this Message.
func (p *Publish) Info() Info {
	return Info{DeliveryMode: p.DeliveryMode, MessageID: p.MessageID}
}

// Type returns the Relay Message type.
func (r *Relay) Type() MessageType {
	return RELAY
}

// Info returns DeliveryMode and MessageID of this Message.
func (r *Relay) Info() Info {
	return Info{DeliveryMode: 1, MessageID: r.MessageID}
}

// Type returns the Subscribe Message type.
func (s *Subscribe) Type() MessageType {
	return SUBSCRIBE
}

// Info returns DeliveryMode and MessageID of this Message.
func (s *Subscribe) Info() Info {
	return Info{DeliveryMode: 1, MessageID: s.MessageID}
}

// Type returns the Unsubscribe Message type.
func (u *Unsubscribe) Type() MessageType {
	return UNSUBSCRIBE
}

// Info returns DeliveryMode and MessageID of this Message.
func (u *Unsubscribe) Info() Info {
	return Info{DeliveryMode: 1, MessageID: u.MessageID}
}

// Type returns the FlowControl Message type.
func (c *ControlMessage) Type() MessageType {
	return FLOWCONTROL
}

// Info returns DeliveryMode and MessageID of this Message.
func (c *ControlMessage) Info() Info {
	return Info{DeliveryMode: 1, MessageID: c.MessageID}
}
