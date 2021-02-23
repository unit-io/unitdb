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

const (
	CONNECT = uint8(iota + 1)
	CONNACK
	PUBLISH
	PUBNEW
	PUBRECEIVE
	PUBRECEIPT
	PUBCOMPLETE
	SUBSCRIBE
	SUBACK
	UNSUBSCRIBE
	UNSUBACK
	PINGREQ
	PINGRESP
	DISCONNECT

	DELIVERYMODE DeliveryMode = iota
	EXPRESS
	RELIABLE
	BATCH
)

// LineProtocol is the interface all our Messages in the line protocol will be implementing
type LineProtocol interface {
	Type() uint8
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
	MesssageLength int
}

// Connect represents a connect Message.
type Connect struct {
	ProtoName     []byte
	Version       uint8
	InsecureFlag  bool
	CleanSessFlag bool
	KeepAlive     uint16
	ClientID      []byte
	Username      []byte
	Password      []byte

	LineProtocol
}

// Connack represents an connack Message.
// 0x00 connection accepted
// 0x01 refused: unacceptable proto version
// 0x02 refused: client identifier rejected
// 0x03 refused server unavailiable
// 0x04 bad user or password
// 0x05 unauthorized
type Connack struct {
	ReturnCode uint8
	ConnID     uint32
	LineProtocol
}

// Pingreq is a keepalive
type Pingreq struct {
	LineProtocol
}

// Pingresp is for saying "hey, the server is alive"
type Pingresp struct {
	LineProtocol
}

//Disconnect is to signal you want to cease communications with the server
type Disconnect struct {
	LineProtocol
}

// Publish represents a publish Message.
type Publish struct {
	MessageID    uint16
	DeliveryMode uint8
	IsForwarded  bool
	Topic        []byte
	Payload      []byte
	Ttl          string

	LineProtocol
}

type Pubnew struct {
	MessageID uint16

	LineProtocol
}

type Pubreceive struct {
	MessageID uint16

	LineProtocol
}

// Pubreceipt is for verifying the receipt of a publish
// DeliveryMode the spec:"It is the second Message of the DeliveryMode Reliable protocol flow. A PUBRECEIPT Mesage is sent by the server in response to a PUBLISH Message from a publishing client, or by a subscriber in response to a PUBLISH Message from the server."
type Pubreceipt struct {
	MessageID uint16

	LineProtocol
}

// Pubcomplete is a final Message in the delivery of publish flow.
type Pubcomplete struct {
	MessageID uint16

	LineProtocol
}

// Subscription is a struct for pairing the delivery mode and topic together
// for the delivery mode's pairs in unsubscribe and subscribe
type Subscription struct {
	Topic        []byte
	Last         string
	DeliveryMode uint8
}

// Subscribe tells the server which topics the client would like to subscribe to
type Subscribe struct {
	MessageID     uint16
	IsForwarded   bool
	Subscriptions []Subscription

	LineProtocol
}

// Suback is to say "hey, you got it buddy. I will send you messages that fit this pattern"
type Suback struct {
	MessageID uint16

	LineProtocol
}

// Unsubscribe is the Message to send if you don't want to subscribe to a topic anymore
type Unsubscribe struct {
	MessageID     uint16
	IsForwarded   bool
	Subscriptions []Subscription

	LineProtocol
}

// Unsuback is to unsubscribe as suback is to subscribe
type Unsuback struct {
	MessageID uint16

	LineProtocol
}

type ProtoAdapter interface {
	Read(r io.Reader) (LineProtocol, error)
	Encode(pkt LineProtocol) (bytes.Buffer, error)
}

func Read(adp ProtoAdapter, r io.Reader) (LineProtocol, error) {
	return adp.Read(r)
}

func Encode(adp ProtoAdapter, pkt LineProtocol) (bytes.Buffer, error) {
	return adp.Encode(pkt)
}

// Type returns the Connect Message type.
func (c *Connect) Type() uint8 {
	return CONNECT
}

// Info returns DeliveryMode and MessageID of this Message.
func (c *Connect) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

// Type returns the Connack Message type.
func (c *Connack) Type() uint8 {
	return CONNACK
}

// Info returns DeliveryMode and MessageID of this Message.
func (c *Connack) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

// Type returns the Pingreq Message type.
func (p *Pingreq) Type() uint8 {
	return PINGREQ
}

// Info returns DeliveryMode and MessageID of this Message.
func (p *Pingreq) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

// Type returns the Pingresp Message type.
func (p *Pingresp) Type() uint8 {
	return PINGRESP
}

// Info returns DeliveryMode and MessageID of this Message.
func (p *Pingresp) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

// Type returns the Disconnect Message type.
func (d *Disconnect) Type() uint8 {
	return DISCONNECT
}

// Info returns DeliveryMode and MessageID of this Message.
func (d *Disconnect) Info() Info {
	return Info{DeliveryMode: 0, MessageID: 0}
}

// Type returns the Publish Message type.
func (p *Publish) Type() uint8 {
	return PUBLISH
}

// Info returns DeliveryMode and MessageID of this Message.
func (p *Publish) Info() Info {
	return Info{DeliveryMode: p.DeliveryMode, MessageID: p.MessageID}
}

// Type returns the Pubnew Message type.
func (p *Pubnew) Type() uint8 {
	return PUBNEW
}

// Info returns MessageID of this Message.
func (p *Pubnew) Info() Info {
	return Info{DeliveryMode: 1, MessageID: p.MessageID}
}

// Type returns the Pubreceive Message type.
func (p *Pubreceive) Type() uint8 {
	return PUBRECEIVE
}

// Info returns MessageID of this Message.
func (p *Pubreceive) Info() Info {
	return Info{DeliveryMode: 1, MessageID: p.MessageID}
}

// Type returns the Pubreceipt Message type.
func (p *Pubreceipt) Type() uint8 {
	return PUBRECEIPT
}

// Info returns DeliveryMode and MessageID of this Message.
func (p *Pubreceipt) Info() Info {
	return Info{DeliveryMode: 1, MessageID: p.MessageID}
}

// Type returns the Pubcomplete Message type.
func (p *Pubcomplete) Type() uint8 {
	return PUBCOMPLETE
}

// Info returns DeliveryMode and MessageID of this Message.
func (p *Pubcomplete) Info() Info {
	return Info{DeliveryMode: 0, MessageID: p.MessageID}
}

// Type returns the Subscribe Message type.
func (s *Subscribe) Type() uint8 {
	return SUBSCRIBE
}

// Info returns DeliveryMode and MessageID of this Message.
func (s *Subscribe) Info() Info {
	return Info{DeliveryMode: 1, MessageID: s.MessageID}
}

// Type returns the Suback Message type.
func (s *Suback) Type() uint8 {
	return SUBACK
}

// Info returns DeliveryMode and MessageID of this Message.
func (s *Suback) Info() Info {
	return Info{DeliveryMode: 0, MessageID: s.MessageID}
}

// Type returns the Unsubscribe Message type.
func (u *Unsubscribe) Type() uint8 {
	return UNSUBSCRIBE
}

// Info returns DeliveryMode and MessageID of this Message.
func (u *Unsubscribe) Info() Info {
	return Info{DeliveryMode: 1, MessageID: u.MessageID}
}

// Type returns the Unsuback Message type.
func (u *Unsuback) Type() uint8 {
	return UNSUBACK
}

// Info returns DeliveryMode and MessageID of this Message.
func (u *Unsuback) Info() Info {
	return Info{DeliveryMode: 0, MessageID: u.MessageID}
}
