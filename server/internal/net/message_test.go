package net

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"
	pbx "github.com/unit-io/unitdb/server/proto"
	"github.com/unit-io/unitdb/server/utp"
)

func TestReadDecodesAllInboundMessages(t *testing.T) {
	msgs := []MessagePack{
		&utp.Connect{ClientID: "id", InsecureFlag: true, KeepAlive: 30},
		&utp.Publish{MessageID: 1, DeliveryMode: 1, Messages: []*utp.PublishMessage{{Topic: "a", Payload: []byte("p")}}},
		&utp.Relay{MessageID: 2, RelayRequests: []*utp.RelayRequest{{Topic: "a", Last: "1m"}}},
		&utp.Subscribe{MessageID: 3, Subscriptions: []*utp.Subscription{{Topic: "a"}}},
		&utp.Unsubscribe{MessageID: 4, Subscriptions: []*utp.Subscription{{Topic: "a"}}},
		&utp.ControlMessage{MessageID: 5, MessageType: utp.PUBLISH, FlowControl: utp.RECEIPT},
	}

	// Write all the messages to one stream to check message framing.
	var stream bytes.Buffer
	for _, m := range msgs {
		buf, err := m.ToBinary()
		if err != nil {
			t.Fatal(err)
		}
		stream.Write(buf.Bytes())
	}

	for _, want := range msgs {
		got, err := Read(&stream)
		if err != nil {
			t.Fatalf("Read %T: %v", want, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
	}
	if stream.Len() != 0 {
		t.Fatalf("%d unread bytes", stream.Len())
	}
}

func TestReadEmptyMessages(t *testing.T) {
	var stream bytes.Buffer
	ping, _ := (&utp.Pingreq{}).ToBinary()
	disc, _ := (&utp.Disconnect{}).ToBinary()
	stream.Write(ping.Bytes())
	stream.Write(disc.Bytes())

	if m, err := Read(&stream); err != nil || m.Type() != utp.PINGREQ {
		t.Fatalf("got %v, %v; want PINGREQ", m, err)
	}
	if m, err := Read(&stream); err != nil || m.Type() != utp.DISCONNECT {
		t.Fatalf("got %v, %v; want DISCONNECT", m, err)
	}
}

func TestReadErrors(t *testing.T) {
	if _, err := Read(bytes.NewReader(nil)); err == nil {
		t.Fatal("expected error on empty stream")
	}

	// A publish with a body shorter than its declared length.
	buf, _ := (&utp.Publish{MessageID: 1, Messages: []*utp.PublishMessage{{Topic: "a", Payload: []byte("payload")}}}).ToBinary()
	raw := buf.Bytes()
	if _, err := Read(bytes.NewReader(raw[:len(raw)-2])); err == nil {
		t.Fatal("expected error on truncated body")
	}
}

func TestEncode(t *testing.T) {
	for _, m := range []MessagePack{
		&utp.Publish{MessageID: 1},
		&utp.ControlMessage{MessageID: 1, MessageType: utp.PUBLISH, FlowControl: utp.ACKNOWLEDGE},
		&utp.Disconnect{},
	} {
		buf, err := Encode(m)
		if err != nil {
			t.Fatalf("Encode %T: %v", m, err)
		}
		got, err := Read(&buf)
		if err != nil {
			t.Fatalf("Read %T: %v", m, err)
		}
		if got.Type() != m.Type() {
			t.Fatalf("type %d, want %d", got.Type(), m.Type())
		}
	}

	// The server never sends connect or subscribe messages.
	if _, err := Encode(&utp.Subscribe{}); err == nil {
		t.Fatal("expected error encoding an inbound-only message")
	}
}

// frame builds a message whose fixed header declares length, followed by body.
func frame(t *testing.T, msgType utp.MessageType, length int32, body []byte) []byte {
	t.Helper()
	h, err := proto.Marshal(&pbx.FixedHeader{MessageType: pbx.MessageType(msgType), MessageLength: length})
	if err != nil {
		t.Fatal(err)
	}
	if len(h) > 127 {
		t.Fatal("header too long for a one byte length")
	}
	return append(append([]byte{byte(len(h))}, h...), body...)
}

func TestReadRejectsInvalidLength(t *testing.T) {
	for _, length := range []int32{-1, -1 << 30, MaxFrameSize + 1, 1<<31 - 1} {
		if _, err := Read(bytes.NewReader(frame(t, utp.PUBLISH, length, nil))); err == nil {
			t.Errorf("length %d: expected an error", length)
		}
	}
	// A frame with a valid length is read normally.
	body, _ := proto.Marshal(&pbx.Publish{MessageID: 1})
	if _, err := Read(bytes.NewReader(frame(t, utp.PUBLISH, int32(len(body)), body))); err != nil {
		t.Fatalf("valid frame: %v", err)
	}
}
