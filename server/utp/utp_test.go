package utp

import (
	"bytes"
	"reflect"
	"testing"
)

type binaryMessage interface {
	ToBinary() (bytes.Buffer, error)
	FromBinary(fh FixedHeader, data []byte)
	Type() MessageType
	Info() Info
}

// roundTrip encodes msg, decodes the fixed header and the body into out.
func roundTrip(t *testing.T, msg, out binaryMessage) FixedHeader {
	t.Helper()
	buf, err := msg.ToBinary()
	if err != nil {
		t.Fatalf("ToBinary: %v", err)
	}
	r := bytes.NewReader(buf.Bytes())
	var fh FixedHeader
	if err := fh.FromBinary(r); err != nil {
		t.Fatalf("FixedHeader.FromBinary: %v", err)
	}
	if fh.MessageLength != r.Len() {
		t.Fatalf("message length %d, remaining bytes %d", fh.MessageLength, r.Len())
	}
	body := make([]byte, fh.MessageLength)
	r.Read(body)
	out.FromBinary(fh, body)
	return fh
}

func TestConnectRoundTrip(t *testing.T) {
	in := &Connect{
		Version:             2,
		InsecureFlag:        true,
		ClientID:            "client-id",
		KeepAlive:           30,
		CleanSessFlag:       true,
		SessKey:             42,
		Username:            "user",
		Password:            []byte("secret"),
		BatchDuration:       100,
		BatchByteThreshold:  1024,
		BatchCountThreshold: 10,
	}
	out := &Connect{}
	fh := roundTrip(t, in, out)
	if fh.MessageType != CONNECT || fh.FlowControl != NONE {
		t.Fatalf("unexpected fixed header %+v", fh)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %+v, want %+v", out, in)
	}
}

func TestConnectAcknowledgeRoundTrip(t *testing.T) {
	in := &ConnectAcknowledge{ReturnCode: ErrRefusedIDRejected, Epoch: 12345, ConnID: 678}
	buf, err := in.ToBinary()
	if err != nil {
		t.Fatal(err)
	}
	out := &ConnectAcknowledge{}
	out.FromBinary(FixedHeader{MessageType: CONNECT, FlowControl: ACKNOWLEDGE}, buf.Bytes())
	if *in != *out {
		t.Fatalf("got %+v, want %+v", out, in)
	}
}

func TestPublishRoundTrip(t *testing.T) {
	in := &Publish{
		MessageID:    7,
		DeliveryMode: 1,
		Messages: []*PublishMessage{
			{Topic: "teams.alpha", Payload: []byte("one"), Ttl: "1m"},
			{Topic: "teams.beta", Payload: []byte("two")},
		},
	}
	out := &Publish{}
	fh := roundTrip(t, in, out)
	if fh.MessageType != PUBLISH {
		t.Fatalf("message type %d, want %d", fh.MessageType, PUBLISH)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %+v, want %+v", out, in)
	}
	if info := out.Info(); info.DeliveryMode != 1 || info.MessageID != 7 {
		t.Fatalf("unexpected info %+v", info)
	}
}

func TestPublishIsForwardedIsNotEncoded(t *testing.T) {
	in := &Publish{IsForwarded: true, MessageID: 1}
	out := &Publish{}
	roundTrip(t, in, out)
	if out.IsForwarded {
		t.Fatal("IsForwarded is a local flag and must not travel on the wire")
	}
}

func TestRelayRoundTrip(t *testing.T) {
	in := &Relay{
		MessageID: 9,
		RelayRequests: []*RelayRequest{
			{Topic: "teams.alpha", Last: "10m"},
			{Topic: "teams.beta"},
		},
	}
	out := &Relay{}
	fh := roundTrip(t, in, out)
	if fh.MessageType != RELAY {
		t.Fatalf("message type %d, want %d", fh.MessageType, RELAY)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("got %+v, want %+v", out, in)
	}
}

func TestSubscribeUnsubscribeRoundTrip(t *testing.T) {
	subs := []*Subscription{
		{DeliveryMode: 1, Delay: 100, Topic: "teams.alpha"},
		{DeliveryMode: 0, Topic: "teams.beta..."},
	}

	sub := &Subscribe{MessageID: 3, Subscriptions: subs}
	subOut := &Subscribe{}
	if fh := roundTrip(t, sub, subOut); fh.MessageType != SUBSCRIBE {
		t.Fatalf("message type %d, want %d", fh.MessageType, SUBSCRIBE)
	}
	if !reflect.DeepEqual(sub, subOut) {
		t.Fatalf("got %+v, want %+v", subOut, sub)
	}

	unsub := &Unsubscribe{MessageID: 4, Subscriptions: subs}
	unsubOut := &Unsubscribe{}
	if fh := roundTrip(t, unsub, unsubOut); fh.MessageType != UNSUBSCRIBE {
		t.Fatalf("message type %d, want %d", fh.MessageType, UNSUBSCRIBE)
	}
	if !reflect.DeepEqual(unsub, unsubOut) {
		t.Fatalf("got %+v, want %+v", unsubOut, unsub)
	}
}

func TestPingreqAndDisconnectHaveEmptyBody(t *testing.T) {
	for _, msg := range []binaryMessage{&Pingreq{}, &Disconnect{}} {
		buf, err := msg.ToBinary()
		if err != nil {
			t.Fatal(err)
		}
		var fh FixedHeader
		if err := fh.FromBinary(bytes.NewReader(buf.Bytes())); err != nil {
			t.Fatal(err)
		}
		if fh.MessageType != msg.Type() || fh.MessageLength != 0 {
			t.Fatalf("%T: unexpected fixed header %+v", msg, fh)
		}
	}
}

func TestControlMessageRoundTrip(t *testing.T) {
	tests := []struct {
		msgType MessageType
		flow    FlowControl
		// wantType is the message type written in the fixed header.
		wantType MessageType
	}{
		{CONNECT, ACKNOWLEDGE, CONNECT},
		{PUBLISH, ACKNOWLEDGE, PUBLISH},
		{RELAY, ACKNOWLEDGE, RELAY},
		{SUBSCRIBE, ACKNOWLEDGE, SUBSCRIBE},
		{UNSUBSCRIBE, ACKNOWLEDGE, UNSUBSCRIBE},
		{PINGREQ, ACKNOWLEDGE, PINGREQ},
		// The remaining flow controls are always about publish messages.
		{SUBSCRIBE, NOTIFY, PUBLISH},
		{SUBSCRIBE, RECEIVE, PUBLISH},
		{SUBSCRIBE, RECEIPT, PUBLISH},
		{SUBSCRIBE, COMPLETE, PUBLISH},
	}
	for _, tt := range tests {
		in := &ControlMessage{MessageID: 11, MessageType: tt.msgType, FlowControl: tt.flow, Message: []byte("m")}
		out := &ControlMessage{}
		fh := roundTrip(t, in, out)
		if fh.MessageType != tt.wantType || fh.FlowControl != tt.flow {
			t.Errorf("%d/%d: unexpected fixed header %+v", tt.msgType, tt.flow, fh)
			continue
		}
		if out.MessageID != 11 || out.MessageType != tt.wantType || out.FlowControl != tt.flow || string(out.Message) != "m" {
			t.Errorf("%d/%d: got %+v", tt.msgType, tt.flow, out)
		}
		if out.Type() != FLOWCONTROL {
			t.Errorf("control message type %d, want %d", out.Type(), FLOWCONTROL)
		}
	}
}

func TestLengthEncoding(t *testing.T) {
	for _, n := range []int{0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455} {
		enc := encodeLength(n)
		got, err := decodeLength(bytes.NewReader(enc))
		if err != nil {
			t.Fatalf("decodeLength(%d): %v", n, err)
		}
		if got != n {
			t.Fatalf("decodeLength(encodeLength(%d)) = %d", n, got)
		}
	}
}

func TestDecodeLengthShortRead(t *testing.T) {
	// A continuation bit with no following byte must fail rather than block.
	if _, err := decodeLength(bytes.NewReader([]byte{0x80})); err == nil {
		t.Fatal("expected error on truncated length")
	}
}

func TestFixedHeaderTruncated(t *testing.T) {
	buf, _ := (&Publish{MessageID: 1}).ToBinary()
	raw := buf.Bytes()
	var fh FixedHeader
	if err := fh.FromBinary(bytes.NewReader(raw[:2])); err == nil {
		t.Fatal("expected error on truncated fixed header")
	}
}
