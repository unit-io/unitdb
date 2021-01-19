package net

import (
	"bytes"
	"io"
)

//Packet is the interface all our packets in the line protocol will be implementing
type Packet interface {
	Type() uint8
	Info() Info
}

const (
	CONNECT = uint8(iota + 1)
	CONNACK
	PUBLISH
	PUBACK
	PUBREC
	PUBREL
	PUBCOMP
	SUBSCRIBE
	SUBACK
	UNSUBSCRIBE
	UNSUBACK
	PINGREQ
	PINGRESP
	DISCONNECT
)

// Info returns Qos and MessageID by the Info() function called on the Packet
type Info struct {
	Qos       uint8
	MessageID uint16
}

// FixedHeader
type FixedHeader struct {
	MessageType     byte
	Dup             bool
	Retain          bool
	Qos             uint8
	RemainingLength int
}

// Connect represents a connect packet.
type Connect struct {
	ProtoName      []byte
	Version        uint8
	InsecureFlag   bool
	UsernameFlag   bool
	PasswordFlag   bool
	WillRetainFlag bool
	WillQOS        uint8
	WillFlag       bool
	CleanSessFlag  bool
	KeepAlive      uint16
	ClientID       []byte
	WillTopic      []byte
	WillMessage    []byte
	Username       []byte
	Password       []byte

	Packet
}

// Connack represents an connack packet.
// 0x00 connection accepted
// 0x01 refused: unacceptable proto version
// 0x02 refused: identifier rejected
// 0x03 refused server unavailiable
// 0x04 bad user or password
// 0x05 not authorized
type Connack struct {
	ReturnCode uint8
	ConnID     uint32
	Packet
}

//Pingreq is a keepalive
type Pingreq struct {
	Packet
}

//Pingresp is for saying "hey, the server is alive"
type Pingresp struct {
	Packet
}

//Disconnect is to signal you want to cease communications with the server
type Disconnect struct {
	Packet
}

// Publish represents a publish packet.
type Publish struct {
	FixedHeader
	Topic       []byte
	MessageID   uint16
	IsForwarded bool
	Payload     []byte

	Packet
}

//Puback is sent for QOS level one to verify the receipt of a publish
//Qoth the spec: "A PUBACK Packet is sent by a server in response to a PUBLISH Packet from a publishing client, and by a subscriber in response to a PUBLISH Packet from the server."
type Puback struct {
	MessageID uint16

	Packet
}

//Pubrec is for verifying the receipt of a publish
//Qoth the spec:"It is the second Packet of the QoS level 2 protocol flow. A PUBREC Packet is sent by the server in response to a PUBLISH Packet from a publishing client, or by a subscriber in response to a PUBLISH Packet from the server."
type Pubrec struct {
	FixedHeader
	MessageID uint16

	Packet
}

//Pubrel is a response to pubrec from either the client or server.
type Pubrel struct {
	FixedHeader
	MessageID uint16

	Packet
}

//Pubcomp is for saying is in response to a pubrel sent by the publisher
//the final member of the QOS2 flow. both sides have said "hey, we did it!"
type Pubcomp struct {
	MessageID uint16

	Packet
}

//TopicQOSTuple is a struct for pairing the Qos and topic together
//for the QOS' pairs in unsubscribe and subscribe
type TopicQOSTuple struct {
	Qos   uint8
	Topic []byte
}

//Subscribe tells the server which topics the client would like to subscribe to
type Subscribe struct {
	FixedHeader
	MessageID     uint16
	IsForwarded   bool
	Subscriptions []TopicQOSTuple

	Packet
}

//Suback is to say "hey, you got it buddy. I will send you messages that fit this pattern"
type Suback struct {
	MessageID uint16
	Qos       []uint8

	Packet
}

//Unsubscribe is the Packet to send if you don't want to subscribe to a topic anymore
type Unsubscribe struct {
	FixedHeader
	MessageID     uint16
	IsForwarded   bool
	Subscriptions []TopicQOSTuple

	Packet
}

//Unsuback is to unsubscribe as suback is to subscribe
type Unsuback struct {
	MessageID uint16

	Packet
}

type ProtoAdapter interface {
	ReadPacket(r io.Reader) (Packet, error)
	Encode(pkt Packet) (bytes.Buffer, error)
}

func ReadPacket(adp ProtoAdapter, r io.Reader) (Packet, error) {
	return adp.ReadPacket(r)
}

func Encode(adp ProtoAdapter, pkt Packet) (bytes.Buffer, error) {
	return adp.Encode(pkt)
}

// Type returns the Connect packet type.
func (c *Connect) Type() uint8 {
	return CONNECT
}

// Info returns Qos and MessageID of this packet.
func (c *Connect) Info() Info {
	return Info{Qos: 0, MessageID: 0}
}

// Type returns the Connack packet type.
func (c *Connack) Type() uint8 {
	return CONNACK
}

// Info returns Qos and MessageID of this packet.
func (c *Connack) Info() Info {
	return Info{Qos: 0, MessageID: 0}
}

// Type returns the Pingreq packet type.
func (p *Pingreq) Type() uint8 {
	return PINGREQ
}

// Info returns Qos and MessageID of this packet.
func (p *Pingreq) Info() Info {
	return Info{Qos: 0, MessageID: 0}
}

// Type returns the Pingresp packet type.
func (p *Pingresp) Type() uint8 {
	return PINGRESP
}

// Info returns Qos and MessageID of this packet.
func (p *Pingresp) Info() Info {
	return Info{Qos: 0, MessageID: 0}
}

// Type returns the Disconnect packet type.
func (d *Disconnect) Type() uint8 {
	return DISCONNECT
}

// Info returns Qos and MessageID of this packet.
func (d *Disconnect) Info() Info {
	return Info{Qos: 0, MessageID: 0}
}

// Type returns the Publish Packet type.
func (p *Publish) Type() uint8 {
	return PUBLISH
}

// Info returns Qos and MessageID of this packet.
func (p *Publish) Info() Info {
	return Info{Qos: p.Qos, MessageID: p.MessageID}
}

// Type returns the Puback Packet type.
func (p *Puback) Type() uint8 {
	return PUBACK
}

// Info returns Qos and MessageID of this packet.
func (p *Puback) Info() Info {
	return Info{Qos: 0, MessageID: p.MessageID}
}

// Type returns the Pubrec Packet type.
func (p *Pubrec) Type() uint8 {
	return PUBREC
}

// Info returns Qos and MessageID of this packet.
func (p *Pubrec) Info() Info {
	return Info{Qos: p.Qos, MessageID: p.MessageID}
}

// Type returns the Pubrel Packet type.
func (p *Pubrel) Type() uint8 {
	return PUBREL
}

// Info returns Qos and MessageID of this packet.
func (p *Pubrel) Info() Info {
	return Info{Qos: p.Qos, MessageID: p.MessageID}
}

// Type returns the Pubcomp Packet type.
func (p *Pubcomp) Type() uint8 {
	return PUBCOMP
}

// Info returns Qos and MessageID of this packet.
func (p *Pubcomp) Info() Info {
	return Info{Qos: 0, MessageID: p.MessageID}
}

// Type returns the Subscribe Packet type.
func (s *Subscribe) Type() uint8 {
	return SUBSCRIBE
}

// Info returns Qos and MessageID of this packet.
func (s *Subscribe) Info() Info {
	return Info{Qos: 1, MessageID: s.MessageID}
}

// Type returns the Suback Packet type.
func (s *Suback) Type() uint8 {
	return SUBACK
}

// Info returns Qos and MessageID of this packet.
func (s *Suback) Info() Info {
	return Info{Qos: 0, MessageID: s.MessageID}
}

// Type returns the Unsubscribe Packet type.
func (u *Unsubscribe) Type() uint8 {
	return UNSUBSCRIBE
}

// Info returns Qos and MessageID of this packet.
func (u *Unsubscribe) Info() Info {
	return Info{Qos: 1, MessageID: u.MessageID}
}

// Type returns the Unsuback Packet type.
func (u *Unsuback) Type() uint8 {
	return UNSUBACK
}

// Info returns Qos and MessageID of this packet.
func (u *Unsuback) Info() Info {
	return Info{Qos: 0, MessageID: u.MessageID}
}
