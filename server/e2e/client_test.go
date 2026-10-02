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

package e2e

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/unitdb/server/internal/message/security"
	lpnet "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/pkg/crypto"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	pbx "github.com/unit-io/unitdb/server/proto"
	"github.com/unit-io/unitdb/server/utp"
	"google.golang.org/protobuf/proto"
)

// mac builds the server's MAC from the shared test key, so the helpers below
// mint client IDs and topic keys the server accepts. This uses only exported
// APIs; it forges nothing the server itself would not issue via keygen.
func mac() *crypto.MAC {
	m, err := crypto.New([]byte(testKey))
	if err != nil {
		panic(err)
	}
	return m
}

// newClientID returns an encoded secondary client ID bound to contract, so two
// clients on the same contract share a namespace and different contracts are
// isolated.
func newClientID(contract uint32) string {
	id, err := uid.NewClientID(1)
	if err != nil {
		panic(err)
	}
	id.SetContract(contract)
	// A non-primary id is cached by the server and usable for pub/sub.
	id.SetPermissions(0)
	return id.Encode(mac())
}

// serviceClientID returns a primary client ID for contract marked as a
// trusted service's, as `mintid -service` issues: its connections need no
// topic keys.
func serviceClientID(contract uint32) string {
	id, err := uid.MintClientID(contract, true)
	if err != nil {
		panic(err)
	}
	return id.Encode(mac())
}

// primaryClientID returns a primary client ID for contract, as `mintid`
// issues.
func primaryClientID(contract uint32) string {
	id, err := uid.MintClientID(contract, false)
	if err != nil {
		panic(err)
	}
	return id.Encode(mac())
}

// contractOf returns the contract of a client ID minted with the test key.
func contractOf(clientID string) (uint32, error) {
	id, err := uid.Decode([]byte(clientID), mac())
	if err != nil {
		return 0, err
	}
	return id.Contract(), nil
}

// signer issues signed topic keys as the test servers' keygen does.
var signer = security.NewSigner([]byte(testKey))

// signedTopicKey mints a signed topic key for contract with the given
// permissions, as the server's keygen would.
func signedTopicKey(contract uint32, topic string, permissions uint32) string {
	k, err := signer.GenerateKey(contract, topic, permissions)
	if err != nil {
		panic(err)
	}
	return k
}

// topicKey mints a topic key for contract with the given permissions, as the
// server's keygen would.
func topicKey(contract uint32, topic string, permissions uint32) string {
	k, err := security.GenerateKey(contract, topic, permissions)
	if err != nil {
		panic(err)
	}
	return k
}

// keyed prefixes a topic with a key: "<key>/<topic>", the secure form.
func keyed(key, topic string) string {
	return key + "/" + topic
}

// client is a raw uTP client over TCP. It is not safe for concurrent writes;
// callers serialize sends. Incoming packets are delivered on channels by a
// background read loop.
type client struct {
	conn    net.Conn
	r       *bufio.Reader
	writeMu sync.Mutex

	nextID uint16
	idMu   sync.Mutex

	// incoming publishes (delivered messages) and control messages.
	pub     chan *utp.Publish
	ctrl    chan *utp.ControlMessage
	acks    chan *utp.ControlMessage // ACKNOWLEDGE only (not connect ack)
	connack chan *utp.ControlMessage

	closeOnce sync.Once
	closed    chan struct{}
	readErr   error

	// holdNotify, when set, leaves NOTIFYs unanswered, so the server keeps
	// the messages logged for the session.
	holdNotify atomic.Bool

	// autoKey and contract are set by connect (see connectOpts.autoKey).
	autoKey  bool
	contract uint32
}

// keyed returns topic as the client sends it: with autoKey, a topic without a
// key gets a signed read/write key minted for it.
func (c *client) keyed(topic string) string {
	if !c.autoKey || strings.Contains(topic, "/") {
		return topic
	}
	name := topic
	if i := strings.IndexByte(topic, '?'); i >= 0 {
		name = topic[:i]
	}
	return keyed(signedTopicKey(c.contract, name, security.AllowReadWrite), topic)
}

func dial(ctx context.Context, addr string) (*client, error) {
	conn, err := dialContext(ctx, addr)
	if err != nil {
		return nil, err
	}
	c := &client{
		conn:    conn,
		r:       bufio.NewReader(conn),
		pub:     make(chan *utp.Publish, 1024),
		ctrl:    make(chan *utp.ControlMessage, 1024),
		acks:    make(chan *utp.ControlMessage, 64),
		connack: make(chan *utp.ControlMessage, 1),
		closed:  make(chan struct{}),
	}
	go c.readLoop()
	return c, nil
}

func (c *client) id() uint16 {
	c.idMu.Lock()
	defer c.idMu.Unlock()
	c.nextID++
	return c.nextID
}

// writeRaw writes bytes to the socket under the write lock. Each utp ToBinary
// already produces a fully framed packet (header + body), so normal sends pass
// its output straight through.
func (c *client) writeRaw(b []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := c.conn.Write(b)
	return err
}

// writeFrame builds a frame by hand: varint(len(header)) + header + body,
// matching utp.FixedHeader.pack. Used only by the security tests to send
// malformed or hand-crafted packets.
func (c *client) writeFrame(mt utp.MessageType, fc utp.FlowControl, body []byte) error {
	h := headerBytes(mt, fc, int32(len(body)))
	buf := append(encodeVarint(len(h)), h...)
	buf = append(buf, body...)
	return c.writeRaw(buf)
}

// headerBytes marshals a FixedHeader declaring length bytes of body.
func headerBytes(mt utp.MessageType, fc utp.FlowControl, length int32) []byte {
	h, err := proto.Marshal(&pbx.FixedHeader{
		MessageType:   pbx.MessageType(mt),
		FlowControl:   pbx.FlowControl(fc),
		MessageLength: length,
	})
	if err != nil {
		panic(err)
	}
	return h
}

func encodeVarint(length int) []byte {
	var out []byte
	for {
		digit := byte(length % 128)
		length /= 128
		if length > 0 {
			digit |= 0x80
		}
		out = append(out, digit)
		if length == 0 {
			break
		}
	}
	return out
}

func (c *client) readLoop() {
	defer close(c.closed)
	for {
		pack, err := lpnet.Read(c.r)
		if err != nil {
			c.readErr = err
			return
		}
		switch p := pack.(type) {
		case *utp.Publish:
			select {
			case c.pub <- p:
			default:
			}
			// Reliable/batch deliveries expect a RECEIPT; mode 0 an ACKNOWLEDGE.
			if p.DeliveryMode == 0 {
				c.sendControl(p.MessageID, utp.PUBLISH, utp.ACKNOWLEDGE, nil)
			} else {
				c.sendControl(p.MessageID, utp.PUBLISH, utp.RECEIPT, nil)
			}
		case *utp.ControlMessage:
			switch {
			case p.MessageType == utp.CONNECT && p.FlowControl == utp.ACKNOWLEDGE:
				select {
				case c.connack <- p:
				default:
				}
			case p.FlowControl == utp.ACKNOWLEDGE:
				select {
				case c.acks <- p:
				default:
				}
			case p.FlowControl == utp.NOTIFY:
				// NOTIFY(id): reply RECEIVE so the server delivers the stored publish.
				if !c.holdNotify.Load() {
					c.sendControl(p.MessageID, utp.PUBLISH, utp.RECEIVE, nil)
				}
			}
			select {
			case c.ctrl <- p:
			default:
			}
		}
	}
}

func (c *client) sendControl(id uint16, mt utp.MessageType, fc utp.FlowControl, msg []byte) error {
	cm := &utp.ControlMessage{MessageID: id, MessageType: mt, FlowControl: fc, Message: msg}
	buf, err := cm.ToBinary()
	if err != nil {
		return err
	}
	return c.writeRaw(buf.Bytes())
}

func (c *client) close() {
	c.closeOnce.Do(func() { c.conn.Close() })
}

// connectOpts are the CONNECT fields the tests vary.
type connectOpts struct {
	clientID string
	// insecure sets the CONNECT insecure flag, which a server refuses
	// unless it allows insecure clients (serverOpts.allowInsecure), and a
	// cluster always refuses.
	insecure bool
	// autoKey keys every topic the client uses without a key with a
	// read/write key minted for the client id's contract, for tests that
	// are not about keys.
	autoKey  bool
	sessKey  int32
	username string
	resume   bool // resume the session instead of starting it clean
}

// connect sends CONNECT and waits for the acknowledgement. autoKey keys the
// topics the client uses (see connectOpts).
func (c *client) connect(clientID string, autoKey bool, sessKey int32) (*utp.ConnectAcknowledge, error) {
	return c.connectWith(connectOpts{clientID: clientID, autoKey: autoKey, sessKey: sessKey})
}

// connectWith sends CONNECT with o and waits for the acknowledgement.
func (c *client) connectWith(o connectOpts) (*utp.ConnectAcknowledge, error) {
	c.autoKey = o.autoKey
	if o.autoKey {
		contract, err := contractOf(o.clientID)
		if err != nil {
			return nil, fmt.Errorf("autoKey: %v", err)
		}
		c.contract = contract
	}
	m := &utp.Connect{
		Version:       1,
		InsecureFlag:  o.insecure,
		ClientID:      o.clientID,
		KeepAlive:     30,
		CleanSessFlag: !o.resume,
		SessKey:       o.sessKey,
		Username:      o.username,
	}
	buf, err := m.ToBinary()
	if err != nil {
		return nil, err
	}
	if err := c.writeRaw(buf.Bytes()); err != nil {
		return nil, err
	}
	select {
	case <-c.closed:
		return nil, fmt.Errorf("connection closed during connect: %v", c.readErr)
	case p := <-c.connack:
		ack := &utp.ConnectAcknowledge{}
		ack.FromBinary(utp.FixedHeader{}, p.Message)
		if ack.ReturnCode != utp.Accepted {
			return ack, fmt.Errorf("connect refused, return code %d", ack.ReturnCode)
		}
		return ack, nil
	case <-time.After(3 * time.Second):
		return nil, fmt.Errorf("connect timeout")
	}
}

func (c *client) publish(mode uint8, topic string, payload []byte, ttl string) (uint16, error) {
	id := c.id()
	m := &utp.Publish{
		MessageID:    id,
		DeliveryMode: mode,
		Messages:     []*utp.PublishMessage{{Topic: c.keyed(topic), Payload: payload, Ttl: ttl}},
	}
	buf, err := m.ToBinary()
	if err != nil {
		return 0, err
	}
	return id, c.writeRaw(buf.Bytes())
}

func (c *client) subscribe(mode uint8, topic string) (uint16, error) {
	id := c.id()
	m := &utp.Subscribe{
		MessageID:     id,
		Subscriptions: []*utp.Subscription{{DeliveryMode: mode, Topic: c.keyed(topic)}},
	}
	buf, err := m.ToBinary()
	if err != nil {
		return 0, err
	}
	return id, c.writeRaw(buf.Bytes())
}

func (c *client) unsubscribe(topic string) (uint16, error) {
	id := c.id()
	m := &utp.Unsubscribe{
		MessageID:     id,
		Subscriptions: []*utp.Subscription{{Topic: c.keyed(topic)}},
	}
	buf, err := m.ToBinary()
	if err != nil {
		return 0, err
	}
	return id, c.writeRaw(buf.Bytes())
}

func (c *client) relay(topic, last string) (uint16, error) {
	id := c.id()
	m := &utp.Relay{
		MessageID:     id,
		RelayRequests: []*utp.RelayRequest{{Topic: c.keyed(topic), Last: last}},
	}
	buf, err := m.ToBinary()
	if err != nil {
		return 0, err
	}
	return id, c.writeRaw(buf.Bytes())
}

func (c *client) ping() error {
	m := &utp.Pingreq{}
	buf, err := m.ToBinary()
	if err != nil {
		return err
	}
	return c.writeRaw(buf.Bytes())
}

// waitPub waits for a delivered publish or times out.
func (c *client) waitPub(d time.Duration) (*utp.Publish, bool) {
	select {
	case p := <-c.pub:
		return p, true
	case <-time.After(d):
		return nil, false
	}
}

// waitAck waits for an ACKNOWLEDGE with the given id.
func (c *client) waitAck(id uint16, d time.Duration) bool {
	deadline := time.After(d)
	for {
		select {
		case a := <-c.acks:
			if a.MessageID == id {
				return true
			}
		case <-deadline:
			return false
		}
	}
}
