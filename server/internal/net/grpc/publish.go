package grpc

import (
	"bytes"

	"github.com/golang/protobuf/proto"
	lp "github.com/unit-io/unitdb/server/internal/net"
	pbx "github.com/unit-io/unitdb/server/proto"
)

func encodePublish(p lp.Publish) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pub := pbx.Publish{
		MessageID: int32(p.MessageID),
		Topic:     string(p.Topic),
		Payload:   string(p.Payload),
		Qos:       int32(p.FixedHeader.Qos),
	}
	pkt, err := proto.Marshal(&pub)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBLISH, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodePuback(p lp.Puback) (bytes.Buffer, error) {
	var msg bytes.Buffer
	puback := pbx.Puback{
		MessageID: int32(p.MessageID),
	}
	pkt, err := proto.Marshal(&puback)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBACK, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodePubrec(p lp.Pubrec) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubrec := pbx.Pubrec{
		MessageID: int32(p.MessageID),
		Qos:       int32(p.FixedHeader.Qos),
	}
	pkt, err := proto.Marshal(&pubrec)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBREC, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodePubrel(p lp.Pubrel) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubrel := pbx.Pubrel{
		MessageID: int32(p.MessageID),
		Qos:       int32(p.FixedHeader.Qos),
	}
	pkt, err := proto.Marshal(&pubrel)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBREL, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func encodePubcomp(p lp.Pubcomp) (bytes.Buffer, error) {
	var msg bytes.Buffer
	pubcomp := pbx.Pubcomp{
		MessageID: int32(p.MessageID),
	}
	pkt, err := proto.Marshal(&pubcomp)
	if err != nil {
		return msg, err
	}
	fh := FixedHeader{MessageType: pbx.MessageType_PUBCOMP, RemainingLength: int32(len(pkt))}
	msg = fh.pack()
	_, err = msg.Write(pkt)
	return msg, err
}

func unpackPublish(data []byte) lp.Packet {
	var pkt pbx.Publish
	proto.Unmarshal(data, &pkt)

	fh := lp.FixedHeader{
		Qos: uint8(pkt.Qos),
	}

	return &lp.Publish{
		FixedHeader: fh,
		MessageID:   uint16(pkt.MessageID),
		Topic:       []byte(pkt.Topic),
		Payload:     []byte(pkt.Payload),
	}
}

func unpackPuback(data []byte) lp.Packet {
	var pkt pbx.Puback
	proto.Unmarshal(data, &pkt)
	return &lp.Puback{
		MessageID: uint16(pkt.MessageID),
	}
}

func unpackPubrec(data []byte) lp.Packet {
	var pkt pbx.Pubrec
	proto.Unmarshal(data, &pkt)

	fh := lp.FixedHeader{
		Qos: uint8(pkt.Qos),
	}
	return &lp.Pubrec{
		FixedHeader: fh,
		MessageID:   uint16(pkt.MessageID),
	}
}

func unpackPubrel(data []byte) lp.Packet {
	var pkt pbx.Pubrel
	proto.Unmarshal(data, &pkt)

	fh := lp.FixedHeader{
		Qos: uint8(pkt.Qos),
	}

	return &lp.Pubrel{
		FixedHeader: fh,
		MessageID:   uint16(pkt.MessageID),
	}
}

func unpackPubcomp(data []byte) lp.Packet {
	var pkt pbx.Pubcomp
	proto.Unmarshal(data, &pkt)
	return &lp.Pubcomp{
		MessageID: uint16(pkt.MessageID),
	}
}
