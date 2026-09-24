package internal

import (
	"bufio"
	"context"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/rs/zerolog"
	"github.com/unit-io/unitdb/server/internal/config"
	"github.com/unit-io/unitdb/server/internal/message/security"
	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
	pbx "github.com/unit-io/unitdb/server/proto"
	"github.com/unit-io/unitdb/server/utp"
	"google.golang.org/grpc"
)

const waitTimeout = 5 * time.Second

var (
	tcpAddr  string
	grpcAddr string

	// lastSessKey hands out a distinct session key per connection. Without it
	// the session key is the client id epoch, which only has second
	// resolution, so connections of unrelated tests would share a session.
	lastSessKey int32 = 1 << 20
)

func nextSessKey() int32 {
	return atomic.AddInt32(&lastSessKey, 1)
}

// TestMain starts one service for the package: the store and the connection
// cache are process wide, so the service cannot be started per test.
func TestMain(m *testing.M) {
	if os.Getenv(shutdownHelperEnv) != "" {
		// The shutdown helper starts and closes its own service.
		zerolog.SetGlobalLevel(zerolog.Disabled)
		os.Exit(m.Run())
	}
	os.Exit(runWithService(m))
}

func runWithService(m *testing.M) int {
	zerolog.SetGlobalLevel(zerolog.Disabled)

	dir, err := ioutil.TempDir("", "unitdb-server-test")
	if err != nil {
		fmt.Println(err)
		return 1
	}
	defer os.RemoveAll(dir)

	tcpAddr = freeAddr()
	grpcAddr = freeAddr()
	cfg := &config.Config{
		Listen:           tcpAddr,
		GrpcListen:       grpcAddr,
		EncryptionConfig: json.RawMessage(`{"key":"4BWm1vZletvrCDGWsF6mex8oBSd59m6I","identifier":"local"}`),
		DBPath:           dir,
		StoreConfig:      json.RawMessage(`{"reset":true,"adapters":{"unitdb":{"mem_size":16777216}}}`),
	}

	svc, err := NewService(cfg)
	if err != nil {
		fmt.Println(err)
		return 1
	}
	Globals.Service = svc
	defer svc.Close()

	svc.listen(cfg.Listen)

	return m.Run()
}

func freeAddr() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// testClient speaks raw uTP to the service so the tests only depend on the
// server's wire protocol.
type testClient struct {
	t       *testing.T
	conn    net.Conn
	in      chan lp.MessagePack
	readErr chan error
	pending []lp.MessagePack
}

func newTestClient(t *testing.T, conn net.Conn) *testClient {
	c := &testClient{
		t:       t,
		conn:    conn,
		in:      make(chan lp.MessagePack, 64),
		readErr: make(chan error, 1),
	}
	go func() {
		r := bufio.NewReader(conn)
		for {
			m, err := readMessage(r)
			if err != nil {
				c.readErr <- err
				close(c.in)
				return
			}
			c.in <- m
		}
	}()
	t.Cleanup(func() { conn.Close() })
	return c
}

// readMessage reads one outbound server message. The server's lp.Read is
// meant for inbound messages: it decodes a PINGREQ acknowledge as a PINGREQ.
func readMessage(r io.Reader) (lp.MessagePack, error) {
	var fh utp.FixedHeader
	if err := fh.FromBinary(r); err != nil {
		return nil, err
	}
	body := make([]byte, fh.MessageLength)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	var m lp.MessagePack
	switch {
	case fh.FlowControl != utp.NONE:
		m = &utp.ControlMessage{}
	case fh.MessageType == utp.PUBLISH:
		m = &utp.Publish{}
	case fh.MessageType == utp.DISCONNECT:
		m = &utp.Disconnect{}
	default:
		return nil, fmt.Errorf("unexpected message type %d", fh.MessageType)
	}
	m.FromBinary(fh, body)
	return m, nil
}

func dialTCP(t *testing.T) *testClient {
	t.Helper()
	conn, err := net.DialTimeout("tcp", tcpAddr, waitTimeout)
	if err != nil {
		t.Fatal(err)
	}
	return newTestClient(t, conn)
}

func dialGRPC(t *testing.T) *testClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()
	cc, err := grpc.DialContext(ctx, grpcAddr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cc.Close() })

	streamCtx, streamCancel := context.WithCancel(context.Background())
	t.Cleanup(streamCancel)
	stream, err := pbx.NewUnitdbClient(cc).Stream(streamCtx)
	if err != nil {
		t.Fatal(err)
	}
	return newTestClient(t, lp.StreamConn(stream))
}

func (c *testClient) send(m lp.MessagePack) {
	c.t.Helper()
	buf, err := m.ToBinary()
	if err != nil {
		c.t.Fatal(err)
	}
	if _, err := c.conn.Write(buf.Bytes()); err != nil {
		c.t.Fatal(err)
	}
}

// next returns the first message that matches, keeping the other messages
// for later calls so that out of order delivery does not lose messages.
func (c *testClient) next(match func(lp.MessagePack) bool, timeout time.Duration) (lp.MessagePack, bool) {
	for i, m := range c.pending {
		if match(m) {
			c.pending = append(c.pending[:i], c.pending[i+1:]...)
			return m, true
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	for {
		select {
		case m, ok := <-c.in:
			if !ok {
				return nil, false
			}
			if match(m) {
				return m, true
			}
			c.pending = append(c.pending, m)
		case <-timer.C:
			return nil, false
		}
	}
}

func (c *testClient) waitFor(desc string, match func(lp.MessagePack) bool) lp.MessagePack {
	c.t.Helper()
	m, ok := c.next(match, waitTimeout)
	if !ok {
		c.t.Fatalf("timed out waiting for %s (unmatched: %s)", desc, describe(c.pending))
	}
	return m
}

func (c *testClient) expectNone(desc string, match func(lp.MessagePack) bool, d time.Duration) {
	c.t.Helper()
	if m, ok := c.next(match, d); ok {
		c.t.Fatalf("unexpected %s: %s", desc, describe([]lp.MessagePack{m}))
	}
}

func (c *testClient) waitClosed() {
	c.t.Helper()
	select {
	case <-c.readErr:
	case <-time.After(waitTimeout):
		c.t.Fatal("server did not close the connection")
	}
}

func describe(msgs []lp.MessagePack) string {
	s := ""
	for _, m := range msgs {
		switch m := m.(type) {
		case *utp.Publish:
			for _, pm := range m.Messages {
				s += fmt.Sprintf("[publish id=%d topic=%q payload=%q] ", m.MessageID, pm.Topic, pm.Payload)
			}
		case *utp.ControlMessage:
			s += fmt.Sprintf("[control type=%d flow=%d id=%d] ", m.MessageType, m.FlowControl, m.MessageID)
		default:
			s += fmt.Sprintf("[%T] ", m)
		}
	}
	return s
}

func isControl(msgType utp.MessageType, flow utp.FlowControl, id uint16) func(lp.MessagePack) bool {
	return func(m lp.MessagePack) bool {
		ctrl, ok := m.(*utp.ControlMessage)
		return ok && ctrl.MessageType == msgType && ctrl.FlowControl == flow && ctrl.MessageID == id
	}
}

func isAck(msgType utp.MessageType, id uint16) func(lp.MessagePack) bool {
	return isControl(msgType, utp.ACKNOWLEDGE, id)
}

func isConnack(m lp.MessagePack) bool {
	ctrl, ok := m.(*utp.ControlMessage)
	return ok && ctrl.MessageType == utp.CONNECT && ctrl.FlowControl == utp.ACKNOWLEDGE
}

func isPublishOn(topic string) func(lp.MessagePack) bool {
	return func(m lp.MessagePack) bool {
		pub, ok := m.(*utp.Publish)
		if !ok {
			return false
		}
		for _, pm := range pub.Messages {
			if pm.Topic == topic {
				return true
			}
		}
		return false
	}
}

func payloadOf(m lp.MessagePack) []byte {
	return m.(*utp.Publish).Messages[0].Payload
}

// connect sends a CONNECT and returns the decoded CONNECT acknowledgement.
// A zero SessKey is replaced with a distinct one; use rawConnect to connect
// with the client id epoch as the session key.
func (c *testClient) connect(cm *utp.Connect) *utp.ConnectAcknowledge {
	c.t.Helper()
	if cm.SessKey == 0 {
		cm.SessKey = nextSessKey()
	}
	return c.rawConnect(cm)
}

func (c *testClient) rawConnect(cm *utp.Connect) *utp.ConnectAcknowledge {
	c.t.Helper()
	if cm.BatchDuration == 0 {
		cm.BatchDuration = 100
	}
	if cm.KeepAlive == 0 {
		cm.KeepAlive = 30
	}
	c.send(cm)
	ctrl := c.waitFor("connect acknowledge", isConnack).(*utp.ControlMessage)
	ack := &utp.ConnectAcknowledge{}
	ack.FromBinary(utp.FixedHeader{MessageType: utp.CONNECT, FlowControl: utp.ACKNOWLEDGE}, ctrl.Message)
	return ack
}

// newClientID connects without a client id and returns the primary client id
// the server assigns.
func newClientID(t *testing.T) string {
	t.Helper()
	c := dialTCP(t)
	c.send(&utp.Connect{})
	m := c.waitFor("assigned client id", isPublishOn("unitdb/clientid/"))
	c.waitClosed()
	return string(payloadOf(m))
}

// connectedClient dials, connects with clientID and checks the connection is accepted.
func connectedClient(t *testing.T, clientID string, insecure bool) *testClient {
	t.Helper()
	c := dialTCP(t)
	if ack := c.connect(&utp.Connect{ClientID: clientID, InsecureFlag: insecure}); ack.ReturnCode != utp.Accepted {
		t.Fatalf("connect return code %d, want %d", ack.ReturnCode, utp.Accepted)
	}
	return c
}

// secondaryClientID requests a secondary client id for the primary client c.
func (c *testClient) secondaryClientID() string {
	c.t.Helper()
	c.send(&utp.Publish{Messages: []*utp.PublishMessage{{Topic: "unitdb/clientid"}}})
	m := c.waitFor("client id response", isPublishOn("unitdb/clientid"))
	var resp types.ClientIdResponse
	if err := json.Unmarshal(payloadOf(m), &resp); err != nil {
		c.t.Fatal(err)
	}
	if resp.Status != 200 {
		c.t.Fatalf("client id response status %d: %s", resp.Status, payloadOf(m))
	}
	return resp.ClientId
}

// keygen requests a key for topic with the given access type ("r", "w", "rw", ...).
func (c *testClient) keygen(topic, access string) string {
	c.t.Helper()
	req, _ := json.Marshal([]types.KeyGenRequest{{Topic: topic, Type: access}})
	c.send(&utp.Publish{Messages: []*utp.PublishMessage{{Topic: "unitdb/keygen", Payload: req}}})
	m := c.waitFor("keygen response", isPublishOn("unitdb/keygen"))
	var resp []types.KeyGenResponse
	if err := json.Unmarshal(payloadOf(m), &resp); err != nil {
		c.t.Fatalf("keygen response %q: %v", payloadOf(m), err)
	}
	if len(resp) != 1 || resp[0].Status != 200 || resp[0].Topic != topic {
		c.t.Fatalf("unexpected keygen response %+v", resp)
	}
	return resp[0].Key
}

func (c *testClient) subscribe(id uint16, topic string, deliveryMode uint8) {
	c.t.Helper()
	c.send(&utp.Subscribe{MessageID: id, Subscriptions: []*utp.Subscription{{Topic: topic, DeliveryMode: deliveryMode}}})
	c.waitFor("subscribe acknowledge", isAck(utp.SUBSCRIBE, id))
}

func (c *testClient) unsubscribe(id uint16, topic string) {
	c.t.Helper()
	c.send(&utp.Unsubscribe{MessageID: id, Subscriptions: []*utp.Subscription{{Topic: topic}}})
	c.waitFor("unsubscribe acknowledge", isAck(utp.UNSUBSCRIBE, id))
}

func (c *testClient) publish(id uint16, topic string, payload string, deliveryMode uint8) {
	c.t.Helper()
	c.send(&utp.Publish{MessageID: id, DeliveryMode: deliveryMode, Messages: []*utp.PublishMessage{{Topic: topic, Payload: []byte(payload)}}})
	c.waitFor("publish acknowledge", isAck(utp.PUBLISH, id))
}

// serverError waits for an error notification for the request with messageID.
func (c *testClient) serverError(messageID uint16) types.Error {
	c.t.Helper()
	m := c.waitFor("error notification", isPublishOn("unitdb/error/"))
	var e types.Error
	if err := json.Unmarshal(payloadOf(m), &e); err != nil {
		c.t.Fatal(err)
	}
	if e.ID != int(messageID) {
		c.t.Fatalf("error for message %d, want %d", e.ID, messageID)
	}
	return e
}

func TestConnectAssignsClientID(t *testing.T) {
	c := dialTCP(t)
	c.send(&utp.Connect{KeepAlive: 30})

	m := c.waitFor("assigned client id", isPublishOn("unitdb/clientid/"))
	if id := payloadOf(m); len(id) != 52 {
		t.Fatalf("client id %q has length %d, want 52", id, len(id))
	}
	// The connection is refused after the client id is sent.
	c.waitClosed()

	if m, ok := c.next(isConnack, 0); ok {
		ack := &utp.ConnectAcknowledge{}
		ack.FromBinary(utp.FixedHeader{}, m.(*utp.ControlMessage).Message)
		if ack.ReturnCode != types.ErrInvalidClientID.ReturnCode {
			t.Fatalf("connect return code %d, want %d", ack.ReturnCode, types.ErrInvalidClientID.ReturnCode)
		}
	}
}

func TestShortFirstMessage(t *testing.T) {
	// An empty CONNECT is 3 bytes on the wire, shorter than "GET " which the
	// listener sniffs for.
	c := dialTCP(t)
	c.send(&utp.Connect{})
	c.waitFor("assigned client id", isPublishOn("unitdb/clientid/"))
}

func TestConnectRejectsForgedClientID(t *testing.T) {
	c := dialTCP(t)
	// Well formed, but not signed with the server key.
	forged := "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	c.send(&utp.Connect{ClientID: forged, KeepAlive: 30})

	m := c.waitFor("assigned client id", isPublishOn("unitdb/clientid/"))
	if string(payloadOf(m)) == forged {
		t.Fatal("server accepted a client id it did not sign")
	}
	c.waitClosed()
}

func TestConnectWithClientID(t *testing.T) {
	c := dialTCP(t)
	ack := c.connect(&utp.Connect{ClientID: newClientID(t)})
	if ack.ReturnCode != utp.Accepted {
		t.Fatalf("connect return code %d, want %d", ack.ReturnCode, utp.Accepted)
	}
	if ack.ConnID == 0 || ack.Epoch == 0 {
		t.Fatalf("connect acknowledge without connection id or epoch: %+v", ack)
	}
}

func TestPing(t *testing.T) {
	c := connectedClient(t, newClientID(t), false)
	c.send(&utp.Pingreq{})
	c.waitFor("ping acknowledge", isAck(utp.PINGREQ, 0))
}

func TestDisconnectRemovesConnection(t *testing.T) {
	c := dialTCP(t)
	ack := c.connect(&utp.Connect{ClientID: newClientID(t), InsecureFlag: true})
	c.subscribe(1, "presence", 0)
	connID := uid.LID(ack.ConnID)
	if Globals.connCache.get(connID) == nil {
		t.Fatal("connection missing from the connection cache")
	}

	c.send(&utp.Disconnect{})
	c.conn.Close()

	deadline := time.Now().Add(waitTimeout)
	for Globals.connCache.get(connID) != nil {
		if time.Now().After(deadline) {
			t.Fatal("connection still cached after disconnect")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestServerClosesOnDisconnect(t *testing.T) {
	c := connectedClient(t, newClientID(t), false)
	c.send(&utp.Disconnect{})
	c.waitClosed()
}

func TestSecurePubSub(t *testing.T) {
	c := connectedClient(t, newClientID(t), false)
	const topic = "teams.alpha.ch1"
	key := c.keygen(topic, "rw")

	c.subscribe(1, key+"/"+topic, 0)
	c.publish(2, key+"/"+topic, "hello", 0)

	m := c.waitFor("published message", isPublishOn(topic))
	if got := string(payloadOf(m)); got != "hello" {
		t.Fatalf("payload %q, want %q", got, "hello")
	}

	c.unsubscribe(3, key+"/"+topic)
	c.publish(4, key+"/"+topic, "after unsubscribe", 0)
	c.expectNone("message after unsubscribe", isPublishOn(topic), 500*time.Millisecond)
}

func TestSecureRequestErrors(t *testing.T) {
	c := connectedClient(t, newClientID(t), false)
	keyA := c.keygen("teams.a", "rw")
	wildcardKey := c.keygen("teams...", "rw")

	t.Run("malformed key", func(t *testing.T) {
		c.t = t
		c.send(&utp.Subscribe{MessageID: 10, Subscriptions: []*utp.Subscription{{Topic: "badkey/teams.a"}}})
		if e := c.serverError(10); e.Status != types.ErrBadRequest.Status {
			t.Fatalf("status %d, want %d", e.Status, types.ErrBadRequest.Status)
		}
		// Subscribe is acknowledged even when a subscription fails.
		c.waitFor("subscribe acknowledge", isAck(utp.SUBSCRIBE, 10))
	})

	t.Run("key for another topic", func(t *testing.T) {
		c.t = t
		c.send(&utp.Subscribe{MessageID: 11, Subscriptions: []*utp.Subscription{{Topic: keyA + "/teams.b"}}})
		if e := c.serverError(11); e.Status != types.ErrUnauthorized.Status {
			t.Fatalf("status %d, want %d", e.Status, types.ErrUnauthorized.Status)
		}
		c.waitFor("subscribe acknowledge", isAck(utp.SUBSCRIBE, 11))
	})

	t.Run("publish without key", func(t *testing.T) {
		c.t = t
		c.send(&utp.Publish{MessageID: 12, Messages: []*utp.PublishMessage{{Topic: "teams.a", Payload: []byte("x")}}})
		if e := c.serverError(12); e.Status != types.ErrBadRequest.Status {
			t.Fatalf("status %d, want %d", e.Status, types.ErrBadRequest.Status)
		}
		c.expectNone("publish acknowledge", isAck(utp.PUBLISH, 12), 300*time.Millisecond)
	})

	t.Run("publish to wildcard topic", func(t *testing.T) {
		c.t = t
		c.send(&utp.Publish{MessageID: 13, Messages: []*utp.PublishMessage{{Topic: wildcardKey + "/teams...", Payload: []byte("x")}}})
		if e := c.serverError(13); e.Status != types.ErrForbidden.Status {
			t.Fatalf("status %d, want %d", e.Status, types.ErrForbidden.Status)
		}
	})

	t.Run("key from another contract", func(t *testing.T) {
		other := connectedClient(t, newClientID(t), false)
		otherKey := other.keygen("teams.a", "rw")
		c.t = t
		c.send(&utp.Subscribe{MessageID: 14, Subscriptions: []*utp.Subscription{{Topic: otherKey + "/teams.a"}}})
		if e := c.serverError(14); e.Status != types.ErrUnauthorized.Status {
			t.Fatalf("status %d, want %d", e.Status, types.ErrUnauthorized.Status)
		}
	})
}

func TestClientIDRequest(t *testing.T) {
	primary := connectedClient(t, newClientID(t), false)
	secondaryID := primary.secondaryClientID()

	secondary := connectedClient(t, secondaryID, false)

	// Only a primary client may request new client ids.
	secondary.send(&utp.Publish{Messages: []*utp.PublishMessage{{Topic: "unitdb/clientid"}}})
	m := secondary.waitFor("client id response", isPublishOn("unitdb/clientid"))
	var e types.Error
	if err := json.Unmarshal(payloadOf(m), &e); err != nil {
		t.Fatal(err)
	}
	if e.Status != types.ErrClientIdForbidden.Status {
		t.Fatalf("status %d, want %d", e.Status, types.ErrClientIdForbidden.Status)
	}
}

func TestUnknownSpecialRequest(t *testing.T) {
	c := connectedClient(t, newClientID(t), false)
	c.send(&utp.Publish{Messages: []*utp.PublishMessage{{Topic: "unitdb/nosuchrequest"}}})
	m := c.waitFor("special request response", isPublishOn("unitdb/nosuchrequest"))
	var e types.Error
	if err := json.Unmarshal(payloadOf(m), &e); err != nil {
		t.Fatal(err)
	}
	if e.Status != types.ErrNotFound.Status {
		t.Fatalf("status %d, want %d", e.Status, types.ErrNotFound.Status)
	}
}

func TestPubSubAcrossConnections(t *testing.T) {
	publisher := connectedClient(t, newClientID(t), true)
	subscriber := connectedClient(t, publisher.secondaryClientID(), true)
	// A client of another contract must not see the messages.
	stranger := connectedClient(t, newClientID(t), true)

	const topic = "chat.room1"
	subscriber.subscribe(1, topic, 0)
	stranger.subscribe(1, topic, 0)

	for i := 0; i < 3; i++ {
		publisher.publish(uint16(10+i), topic, fmt.Sprintf("msg-%d", i), 0)
	}
	for i := 0; i < 3; i++ {
		want := fmt.Sprintf("msg-%d", i)
		subscriber.waitFor(want, func(m lp.MessagePack) bool {
			return isPublishOn(topic)(m) && string(payloadOf(m)) == want
		})
	}
	stranger.expectNone("message from another contract", isPublishOn(topic), 300*time.Millisecond)
	// The publisher did not subscribe.
	publisher.expectNone("message on the publisher", isPublishOn(topic), 100*time.Millisecond)
}

func TestWildcardSubscription(t *testing.T) {
	publisher := connectedClient(t, newClientID(t), true)
	subscriber := connectedClient(t, publisher.secondaryClientID(), true)

	subscriber.subscribe(1, "sensors...", 0)
	publisher.publish(2, "sensors.room1.temp", "21", 0)

	m := subscriber.waitFor("wildcard delivery", isPublishOn("sensors.room1.temp"))
	if string(payloadOf(m)) != "21" {
		t.Fatalf("payload %q, want %q", payloadOf(m), "21")
	}
}

func TestPublishAfterSubscriberLeft(t *testing.T) {
	publisher := connectedClient(t, newClientID(t), true)
	secondaryID := publisher.secondaryClientID()

	subscriber := connectedClient(t, secondaryID, true)
	subscriber.subscribe(1, "presence", 0)
	subscriber.send(&utp.Disconnect{})
	subscriber.conn.Close()

	// Publishing to a topic whose only subscriber is gone must still succeed.
	publisher.publish(2, "presence", "anyone?", 0)
}

func TestReliableDelivery(t *testing.T) {
	publisher := connectedClient(t, newClientID(t), true)
	subscriber := connectedClient(t, publisher.secondaryClientID(), true)

	const topic = "orders.new"
	subscriber.subscribe(1, topic, 1)
	publisher.publish(5, topic, "order-1", 1)

	// RELIABLE: the server notifies, the subscriber asks for the message,
	// then confirms receipt and the server completes the flow. The id is the
	// subscriber connection's, not the publisher's.
	id := subscriber.waitFor("notify", isNotify).(*utp.ControlMessage).MessageID
	pub := subscriber.receiveNotified(id)
	if got := string(pub.Messages[0].Payload); got != "order-1" {
		t.Fatalf("payload %q, want %q", got, "order-1")
	}
	if pub.DeliveryMode != 1 || pub.Messages[0].Topic != topic {
		t.Fatalf("delivery mode %d topic %q, want 1 and %q", pub.DeliveryMode, pub.Messages[0].Topic, topic)
	}

	subscriber.send(&utp.ControlMessage{MessageID: id, MessageType: utp.PUBLISH, FlowControl: utp.RECEIPT})
	subscriber.waitFor("complete", isControl(utp.PUBLISH, utp.COMPLETE, id))
}

func TestBatchSubscription(t *testing.T) {
	publisher := connectedClient(t, newClientID(t), true)
	subscriber := connectedClient(t, publisher.secondaryClientID(), true)

	const topic = "metrics.cpu"
	subscriber.subscribe(1, topic, 2)
	want := map[string]bool{"10": true, "20": true, "30": true}
	for i, p := range []string{"10", "20", "30"} {
		publisher.publish(uint16(10+i), topic, p, 0)
	}

	deadline := time.Now().Add(waitTimeout)
	for len(want) > 0 && time.Now().Before(deadline) {
		m, ok := subscriber.next(isPublishOn(topic), time.Until(deadline))
		if !ok {
			break
		}
		for _, pm := range m.(*utp.Publish).Messages {
			delete(want, string(pm.Payload))
		}
	}
	if len(want) > 0 {
		t.Fatalf("batched messages not delivered: %v", want)
	}
}

func TestRelayLastMessages(t *testing.T) {
	publisher := connectedClient(t, newClientID(t), true)
	const topic = "history.t1"
	want := map[string]bool{}
	for i := 0; i < 3; i++ {
		p := fmt.Sprintf("stored-%d", i)
		want[p] = true
		publisher.publish(uint16(10+i), topic, p, 0)
	}

	// A new connection of the same contract relays the stored messages.
	relayer := connectedClient(t, publisher.secondaryClientID(), true)
	relayer.send(&utp.Relay{MessageID: 1, RelayRequests: []*utp.RelayRequest{{Topic: topic, Last: "1m"}}})
	relayer.waitFor("relay acknowledge", isAck(utp.RELAY, 1))

	deadline := time.Now().Add(waitTimeout)
	for len(want) > 0 && time.Now().Before(deadline) {
		m, ok := relayer.next(isPublishOn(topic), time.Until(deadline))
		if !ok {
			break
		}
		for _, pm := range m.(*utp.Publish).Messages {
			delete(want, string(pm.Payload))
		}
	}
	if len(want) > 0 {
		t.Fatalf("stored messages not relayed: %v", want)
	}
}

func TestRelayWithoutLastSendsNothing(t *testing.T) {
	publisher := connectedClient(t, newClientID(t), true)
	publisher.publish(1, "history.t2", "stored", 0)

	publisher.send(&utp.Relay{MessageID: 2, RelayRequests: []*utp.RelayRequest{{Topic: "history.t2"}}})
	publisher.waitFor("relay acknowledge", isAck(utp.RELAY, 2))
	publisher.expectNone("relayed message", isPublishOn("history.t2"), 500*time.Millisecond)
}

func TestInsecureUnsubscribe(t *testing.T) {
	publisher := connectedClient(t, newClientID(t), true)
	subscriber := connectedClient(t, publisher.secondaryClientID(), true)

	subscriber.subscribe(1, "news", 0)
	subscriber.unsubscribe(2, "news")
	publisher.publish(3, "news", "after unsubscribe", 0)
	subscriber.expectNone("message after unsubscribe", isPublishOn("news"), 500*time.Millisecond)

	// Topic options are not part of the subscription.
	subscriber.subscribe(4, "sports?last=1m", 0)
	subscriber.unsubscribe(5, "sports")
	publisher.publish(6, "sports", "after unsubscribe", 0)
	subscriber.expectNone("message after unsubscribe", isPublishOn("sports"), 500*time.Millisecond)
}

func TestGRPCStream(t *testing.T) {
	c := dialGRPC(t)
	if ack := c.connect(&utp.Connect{ClientID: newClientID(t), InsecureFlag: true}); ack.ReturnCode != utp.Accepted {
		t.Fatalf("connect return code %d, want %d", ack.ReturnCode, utp.Accepted)
	}

	c.send(&utp.Pingreq{})
	c.waitFor("ping acknowledge", isAck(utp.PINGREQ, 0))

	c.subscribe(1, "grpc.topic", 0)
	c.publish(2, "grpc.topic", "over grpc", 0)
	m := c.waitFor("published message", isPublishOn("grpc.topic"))
	if got := string(payloadOf(m)); got != "over grpc" {
		t.Fatalf("payload %q, want %q", got, "over grpc")
	}
}

func TestPubSubBetweenTCPAndGRPC(t *testing.T) {
	publisher := connectedClient(t, newClientID(t), true)
	subscriber := dialGRPC(t)
	if ack := subscriber.connect(&utp.Connect{ClientID: publisher.secondaryClientID(), InsecureFlag: true}); ack.ReturnCode != utp.Accepted {
		t.Fatalf("connect return code %d, want %d", ack.ReturnCode, utp.Accepted)
	}

	subscriber.subscribe(1, "mixed.topic", 0)
	publisher.publish(2, "mixed.topic", "tcp to grpc", 0)
	subscriber.waitFor("published message", isPublishOn("mixed.topic"))
}

func TestSessionIsNotSharedAcrossClients(t *testing.T) {
	// Two unrelated primary client ids issued in the same second.
	epoch := func(id string) uint32 {
		cid, err := uid.Decode([]byte(id), Globals.Service.mac)
		if err != nil {
			t.Fatal(err)
		}
		return cid.Epoch()
	}
	var idA, idB string
	for {
		idA, idB = newClientID(t), newClientID(t)
		if epoch(idA) == epoch(idB) {
			break
		}
	}

	a := dialTCP(t)
	a.rawConnect(&utp.Connect{ClientID: idA, InsecureFlag: true})

	// A receives a message, which the server keeps in A's session log.
	a.subscribe(1, "private.inbox", 0)
	a.publish(2, "private.inbox", "secret", 0)
	a.waitFor("own message", isPublishOn("private.inbox"))

	// B connects with the default session key and must not see A's message.
	b := dialTCP(t)
	b.rawConnect(&utp.Connect{ClientID: idB, InsecureFlag: true})
	m, ok := b.next(func(m lp.MessagePack) bool {
		ctrl, ok := m.(*utp.ControlMessage)
		return ok && ctrl.FlowControl == utp.NOTIFY
	}, 500*time.Millisecond)
	if !ok {
		return
	}
	b.send(&utp.ControlMessage{MessageID: m.(*utp.ControlMessage).MessageID, MessageType: utp.PUBLISH, FlowControl: utp.RECEIVE})
	if m, ok := b.next(isPublishOn("private.inbox"), time.Second); ok {
		t.Fatalf("client B received client A's message %q", payloadOf(m))
	}
	t.Fatal("client B was notified about client A's messages")
}

func TestSessionKeyIsScopedToContract(t *testing.T) {
	// Client A uses an explicit session key and receives a message.
	a := dialTCP(t)
	a.rawConnect(&utp.Connect{ClientID: newClientID(t), InsecureFlag: true, SessKey: 424242})
	a.subscribe(1, "private.inbox", 0)
	a.publish(2, "private.inbox", "secret", 0)
	a.waitFor("own message", isPublishOn("private.inbox"))

	// Client B of another contract claims the same session key.
	b := dialTCP(t)
	b.rawConnect(&utp.Connect{ClientID: newClientID(t), InsecureFlag: true, SessKey: 424242})
	if m, ok := b.next(func(m lp.MessagePack) bool {
		ctrl, ok := m.(*utp.ControlMessage)
		return ok && ctrl.FlowControl == utp.NOTIFY
	}, 500*time.Millisecond); ok {
		t.Fatalf("client B was notified about client A's message %d", m.(*utp.ControlMessage).MessageID)
	}
}

func TestSessionKey(t *testing.T) {
	idA, _ := uid.NewClientID(1)
	idB, _ := uid.NewClientID(1)
	idB.SetEpoch(idA.Epoch())

	if sessionKey(idA, 0) == sessionKey(idB, 0) {
		t.Fatal("client ids issued in the same second share a session key")
	}
	if sessionKey(idA, 7) == sessionKey(idB, 7) {
		t.Fatal("clients of different contracts share an explicit session key")
	}
	if sessionKey(idA, 0) != sessionKey(idA, 0) || sessionKey(idA, 7) == sessionKey(idA, 0) {
		t.Fatal("session keys are not stable per client and session key")
	}
	if sessionKey(idA, 0)>>48 == 0 {
		t.Fatal("session keys must not overlap message log keys")
	}
}

func TestEmptyTopicIsRejected(t *testing.T) {
	c := connectedClient(t, newClientID(t), true)
	for i, topic := range []string{"", "/"} {
		id := uint16(i*3 + 1)
		c.send(&utp.Subscribe{MessageID: id, Subscriptions: []*utp.Subscription{{Topic: topic}}})
		if e := c.serverError(id); e.Status != types.ErrBadRequest.Status {
			t.Fatalf("subscribe %q: status %d, want %d", topic, e.Status, types.ErrBadRequest.Status)
		}
		c.send(&utp.Publish{MessageID: id + 1, Messages: []*utp.PublishMessage{{Topic: topic, Payload: []byte("x")}}})
		if e := c.serverError(id + 1); e.Status != types.ErrBadRequest.Status {
			t.Fatalf("publish %q: status %d, want %d", topic, e.Status, types.ErrBadRequest.Status)
		}
		c.send(&utp.Relay{MessageID: id + 2, RelayRequests: []*utp.RelayRequest{{Topic: topic, Last: "1m"}}})
		if e := c.serverError(id + 2); e.Status != types.ErrBadRequest.Status {
			t.Fatalf("relay %q: status %d, want %d", topic, e.Status, types.ErrBadRequest.Status)
		}
	}
	// The server is still up.
	c.send(&utp.Pingreq{})
	c.waitFor("ping acknowledge", isAck(utp.PINGREQ, 0))
}

func TestResumeReliableMessage(t *testing.T) {
	// A new session's id is its first connection id. Pick a session id with
	// bit 4 set, which resume used to mistake for an inbound log entry. Each
	// attempt needs a new primary client id: secondary ids issued in the same
	// second are identical and so share one session.
	var subscriberID string
	var sub *testClient
	for i := 0; ; i++ {
		if i == 64 {
			t.Fatal("no session id with bit 4 set")
		}
		subscriberID = newClientID(t)
		sub = dialTCP(t)
		ack := sub.rawConnect(&utp.Connect{ClientID: subscriberID, InsecureFlag: true})
		if ack.ConnID&(1<<4) != 0 {
			break
		}
		sub.conn.Close()
	}
	publisher := connectedClient(t, sub.secondaryClientID(), true)

	sub.subscribe(1, "resume.express", 0)
	sub.subscribe(2, "resume.reliable", 1)
	publisher.publish(10, "resume.express", "express", 0)
	sub.waitFor("express message", isPublishOn("resume.express"))
	publisher.publish(11, "resume.reliable", "reliable", 1)
	id := sub.waitFor("notify", isNotify).(*utp.ControlMessage).MessageID

	// Leave without receiving the reliable message.
	sub.send(&utp.Disconnect{})
	sub.conn.Close()

	// The same client reconnects and is notified again, and only about the
	// reliable message.
	sub = dialTCP(t)
	sub.rawConnect(&utp.Connect{ClientID: subscriberID, InsecureFlag: true})
	sub.waitFor("resumed notify", isControl(utp.PUBLISH, utp.NOTIFY, id))
	sub.expectNone("other resumed notify", func(m lp.MessagePack) bool {
		ctrl, ok := m.(*utp.ControlMessage)
		return ok && ctrl.FlowControl == utp.NOTIFY
	}, 300*time.Millisecond)

	if got := string(sub.receiveNotified(id).Messages[0].Payload); got != "reliable" {
		t.Fatalf("payload %q, want %q", got, "reliable")
	}
	sub.send(&utp.ControlMessage{MessageID: id, MessageType: utp.PUBLISH, FlowControl: utp.RECEIPT})
	sub.waitFor("complete", isControl(utp.PUBLISH, utp.COMPLETE, id))

	// Messages the server sends after a resume get fresh ids.
	sub.subscribe(3, "resume.after", 0)
	sub.send(&utp.Publish{MessageID: 12, Messages: []*utp.PublishMessage{{Topic: "resume.after", Payload: []byte("after")}}})
	sub.waitFor("message after resume", isPublishOn("resume.after"))
}

func TestSpecialRequestIsAcknowledged(t *testing.T) {
	c := connectedClient(t, newClientID(t), false)
	req, _ := json.Marshal([]types.KeyGenRequest{{Topic: "teams.a", Type: "rw"}})
	c.send(&utp.Publish{MessageID: 7, Messages: []*utp.PublishMessage{{Topic: "unitdb/keygen", Payload: req}}})
	c.waitFor("keygen response", isPublishOn("unitdb/keygen"))
	c.waitFor("publish acknowledge", isAck(utp.PUBLISH, 7))
}

func TestClientIDMustDecrypt(t *testing.T) {
	victim := connectedClient(t, newClientID(t), true)
	secondaryID := victim.secondaryClientID()
	// Connecting with the secondary id caches it on the server.
	member := connectedClient(t, secondaryID, true)
	member.subscribe(1, "tenant.inbox", 0)

	// Keep only the characters the cache used to key on, 4 to 7.
	forged := []byte(strings.Repeat("A", len(secondaryID)))
	copy(forged[4:8], secondaryID[4:8])

	c := dialTCP(t)
	c.send(&utp.Connect{ClientID: string(forged), InsecureFlag: true, KeepAlive: 30, SessKey: nextSessKey()})
	m := c.waitFor("connect acknowledge", isConnack)
	ack := &utp.ConnectAcknowledge{}
	ack.FromBinary(utp.FixedHeader{}, m.(*utp.ControlMessage).Message)
	if ack.ReturnCode == utp.Accepted {
		// Show what the forged client can reach.
		c.subscribe(1, "tenant.inbox", 0)
		victim.publish(2, "tenant.inbox", "tenant secret", 0)
		if m, ok := c.next(isPublishOn("tenant.inbox"), time.Second); ok {
			t.Fatalf("forged client id joined the victim's contract and read %q", payloadOf(m))
		}
		t.Fatal("forged client id was accepted")
	}

	// The genuine secondary id still connects.
	connectedClient(t, secondaryID, true)
}

func TestInvalidMessageLengthClosesConnection(t *testing.T) {
	c := connectedClient(t, newClientID(t), true)
	h, _ := proto.Marshal(&pbx.FixedHeader{MessageType: pbx.MessageType(utp.PUBLISH), MessageLength: -5})
	if _, err := c.conn.Write(append([]byte{byte(len(h))}, h...)); err != nil {
		t.Fatal(err)
	}
	// The bad connection is dropped...
	c.waitClosed()
	// ...and the server keeps serving others.
	other := connectedClient(t, newClientID(t), true)
	other.send(&utp.Pingreq{})
	other.waitFor("ping acknowledge", isAck(utp.PINGREQ, 0))
}

func TestKeyPermissions(t *testing.T) {
	owner := connectedClient(t, newClientID(t), false)
	readKey := owner.keygen("perm.t", "r")
	writeKey := owner.keygen("perm.t", "w")
	sub := connectedClient(t, owner.secondaryClientID(), false)

	// A write-only key can't subscribe.
	sub.send(&utp.Subscribe{MessageID: 1, Subscriptions: []*utp.Subscription{{Topic: writeKey + "/perm.t"}}})
	if e := sub.serverError(1); e.Status != types.ErrUnauthorized.Status {
		t.Fatalf("subscribe with a write-only key: status %d, want %d", e.Status, types.ErrUnauthorized.Status)
	}
	// A read-only key can.
	sub.subscribe(2, readKey+"/perm.t", 0)

	// A read-only key can't publish.
	owner.send(&utp.Publish{MessageID: 3, Messages: []*utp.PublishMessage{{Topic: readKey + "/perm.t", Payload: []byte("from reader")}}})
	if e := owner.serverError(3); e.Status != types.ErrUnauthorized.Status {
		t.Fatalf("publish with a read-only key: status %d, want %d", e.Status, types.ErrUnauthorized.Status)
	}
	sub.expectNone("message published with a read-only key", isPublishOn("perm.t"), 300*time.Millisecond)

	// A write-only key can.
	owner.publish(4, writeKey+"/perm.t", "from writer", 0)
	m := sub.waitFor("message published with a write-only key", isPublishOn("perm.t"))
	if got := string(payloadOf(m)); got != "from writer" {
		t.Fatalf("payload %q, want %q", got, "from writer")
	}
}

func TestSignedKeys(t *testing.T) {
	ownerID := newClientID(t)
	owner := connectedClient(t, ownerID, false)
	readKey := owner.keygen("signed.t", "r")
	if len(readKey) != security.SignedKeyLen {
		t.Fatalf("keygen returned %q, want a signed key", readKey)
	}
	sub := connectedClient(t, owner.secondaryClientID(), false)
	sub.subscribe(1, readKey+"/signed.t", 0)

	publishRejected := func(id uint16, key, desc string, status int) {
		t.Helper()
		owner.send(&utp.Publish{MessageID: id, Messages: []*utp.PublishMessage{{Topic: key + "/signed.t", Payload: []byte(desc)}}})
		if e := owner.serverError(id); e.Status != status {
			t.Fatalf("%s: status %d, want %d", desc, e.Status, status)
		}
		sub.expectNone(desc, isPublishOn("signed.t"), 200*time.Millisecond)
	}

	// A read key whose permission byte is edited to read/write.
	raw, _ := base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(readKey)
	raw[0] = byte(security.AllowReadWrite)
	edited := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)
	publishRejected(2, edited, "edited key", types.ErrUnauthorized.Status)

	// A key minted without the server secret, the way unsigned keys could be.
	cid, err := uid.Decode([]byte(ownerID), Globals.Service.mac)
	if err != nil {
		t.Fatal(err)
	}
	minted, _ := security.GenerateKey(cid.Contract(), "signed.t", security.AllowReadWrite)
	publishRejected(3, minted, "unsigned key", types.ErrUnauthorized.Status)

	// Unsigned keys are accepted while the server is configured to.
	Globals.Service.setAcceptUnsignedKeys(true)
	defer Globals.Service.setAcceptUnsignedKeys(false)
	owner.publish(4, minted+"/signed.t", "unsigned key accepted", 0)
	sub.waitFor("message published with an unsigned key", isPublishOn("signed.t"))
}

func TestKeyGenRequiresPrimaryClient(t *testing.T) {
	owner := connectedClient(t, newClientID(t), false)
	secondary := connectedClient(t, owner.secondaryClientID(), false)

	req, _ := json.Marshal([]types.KeyGenRequest{{Topic: "admin...", Type: "o"}})
	secondary.send(&utp.Publish{MessageID: 1, Messages: []*utp.PublishMessage{{Topic: "unitdb/keygen", Payload: req}}})
	m := secondary.waitFor("keygen response", isPublishOn("unitdb/keygen"))
	var e types.Error
	if err := json.Unmarshal(payloadOf(m), &e); err != nil {
		t.Fatalf("keygen response %s: %v", payloadOf(m), err)
	}
	if e.Status != types.ErrKeyGenForbidden.Status {
		t.Fatalf("secondary client keygen: %s", payloadOf(m))
	}
}

const (
	shutdownHelperEnv = "UNITDB_SHUTDOWN_HELPER"
	shutdownOK        = "shutdown checks passed"
)

// TestShutdownUnderLoad closes a service while clients publish and subscribe.
// The store is process wide, so the service runs in a child process: see
// TestShutdownHelper. Under -race the child is built with the race detector.
func TestShutdownUnderLoad(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestShutdownHelper$", "-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), shutdownHelperEnv+"=1")
	out, _ := cmd.CombinedOutput()
	output := string(out)

	// Any race marks the child test failed, so its own checks report success
	// with a marker instead.
	if strings.Contains(output, "DATA RACE") || strings.Contains(output, "panic:") || !strings.Contains(output, shutdownOK) {
		if len(output) > 8000 {
			output = output[len(output)-8000:]
		}
		t.Fatalf("shutdown helper failed:\n%s", output)
	}
}

func TestShutdownHelper(t *testing.T) {
	if os.Getenv(shutdownHelperEnv) == "" {
		t.Skip("run by TestShutdownUnderLoad")
	}
	dir, err := ioutil.TempDir("", "unitdb-shutdown-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	tcpAddr, grpcAddr = freeAddr(), freeAddr()
	svc, err := NewService(&config.Config{
		Listen:           tcpAddr,
		GrpcListen:       grpcAddr,
		EncryptionConfig: json.RawMessage(`{"key":"4BWm1vZletvrCDGWsF6mex8oBSd59m6I","identifier":"local"}`),
		DBPath:           dir,
		StoreConfig:      json.RawMessage(`{"reset":true,"adapters":{"unitdb":{"mem_size":16777216}}}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	svc.listen(tcpAddr)

	publisher := connectedClient(t, newClientID(t), true)
	var conns []net.Conn
	for i := 0; i < 4; i++ {
		sub := connectedClient(t, publisher.secondaryClientID(), true)
		sub.subscribe(1, "load.t", uint8(i%2))
		conns = append(conns, sub.conn)
	}
	g := dialGRPC(t)
	g.connect(&utp.Connect{ClientID: publisher.secondaryClientID(), InsecureFlag: true})
	g.subscribe(1, "load.t", 0)

	// Publish as fast as possible until the end: express and reliable, so
	// that subscribers are both sent messages and notified.
	stop := make(chan struct{})
	defer close(stop)
	raw := func(conn net.Conn, m lp.MessagePack) {
		buf, _ := m.ToBinary()
		conn.Write(buf.Bytes())
	}
	for _, conn := range append(conns, publisher.conn, g.conn) {
		go func(conn net.Conn) {
			for id := uint16(1); ; id++ {
				select {
				case <-stop:
					return
				default:
				}
				raw(conn, &utp.Publish{MessageID: id, DeliveryMode: uint8(id % 2), Messages: []*utp.PublishMessage{{Topic: "load.t", Payload: []byte("load")}}})
			}
		}(conn)
	}
	time.Sleep(300 * time.Millisecond)
	if n := len(Globals.connCache.all()); n < 6 {
		t.Fatalf("only %d connections open under load, want 6", n)
	}

	done := make(chan struct{})
	go func() {
		svc.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("Close did not return")
	}
	svc.Close() // a second Close is a no-op

	if store.IsOpen() {
		t.Fatal("store still open after Close")
	}
	// The listener is closed.
	if conn, err := net.DialTimeout("tcp", tcpAddr, time.Second); err == nil {
		conn.Close()
		t.Fatal("service still accepts connections after Close")
	}
	fmt.Println(shutdownOK)
}

func TestReceiveUnknownMessage(t *testing.T) {
	c := connectedClient(t, newClientID(t), true)
	// A client resuming a notification the server no longer has.
	c.send(&utp.ControlMessage{MessageID: 777, MessageType: utp.PUBLISH, FlowControl: utp.RECEIVE})
	c.waitFor("complete", isControl(utp.PUBLISH, utp.COMPLETE, 777))
	// The connection stays open.
	c.send(&utp.Pingreq{})
	c.waitFor("ping acknowledge", isAck(utp.PINGREQ, 0))
}

func TestLargeMessage(t *testing.T) {
	// Larger than the old 512 KiB gRPC limit, within the 4 MiB frame limit.
	payload := strings.Repeat("x", 1<<20)
	for name, dial := range map[string]func(*testing.T) *testClient{"tcp": dialTCP, "grpc": dialGRPC} {
		t.Run(name, func(t *testing.T) {
			c := dial(t)
			if ack := c.connect(&utp.Connect{ClientID: newClientID(t), InsecureFlag: true}); ack.ReturnCode != utp.Accepted {
				t.Fatalf("connect return code %d", ack.ReturnCode)
			}
			c.subscribe(1, "large.t", 0)
			c.publish(2, "large.t", payload, 0)
			m := c.waitFor("large message", isPublishOn("large.t"))
			if got := len(payloadOf(m)); got != len(payload) {
				t.Fatalf("payload of %d bytes, want %d", got, len(payload))
			}
		})
	}
}

// receiveNotified answers a notification with RECEIVE and returns the message.
func (c *testClient) receiveNotified(id uint16) *utp.Publish {
	c.t.Helper()
	c.send(&utp.ControlMessage{MessageID: id, MessageType: utp.PUBLISH, FlowControl: utp.RECEIVE})
	m := c.waitFor("received message", func(m lp.MessagePack) bool {
		pub, ok := m.(*utp.Publish)
		return ok && pub.MessageID == id
	})
	return m.(*utp.Publish)
}

func isNotify(m lp.MessagePack) bool {
	ctrl, ok := m.(*utp.ControlMessage)
	return ok && ctrl.FlowControl == utp.NOTIFY
}

func TestReliableMessagesFromPublishersWithTheSameID(t *testing.T) {
	a := connectedClient(t, newClientID(t), true)
	b := connectedClient(t, a.secondaryClientID(), true)
	sub := connectedClient(t, a.secondaryClientID(), true)
	sub.subscribe(1, "same.id", 1)

	// Both publishers use message id 5.
	a.publish(5, "same.id", "from a", 1)
	b.publish(5, "same.id", "from b", 1)

	n1 := sub.waitFor("first notify", isNotify).(*utp.ControlMessage).MessageID
	n2 := sub.waitFor("second notify", isNotify).(*utp.ControlMessage).MessageID
	if n1 == n2 {
		t.Fatalf("both messages were notified with id %d", n1)
	}
	got := map[string]bool{
		string(sub.receiveNotified(n1).Messages[0].Payload): true,
		string(sub.receiveNotified(n2).Messages[0].Payload): true,
	}
	if !got["from a"] || !got["from b"] {
		t.Fatalf("received %v, want both messages", got)
	}
}

func TestReliableMessageHoldsOnlyTheSubscribedMessage(t *testing.T) {
	owner := connectedClient(t, newClientID(t), false)
	rw := owner.keygen("multi.x", "rw")
	other := owner.keygen("multi.y", "rw")
	sub := connectedClient(t, owner.secondaryClientID(), false)
	sub.subscribe(1, owner.keygen("multi.x", "r")+"/multi.x", 1)

	// One publish with messages for two topics.
	owner.send(&utp.Publish{MessageID: 7, DeliveryMode: 1, Messages: []*utp.PublishMessage{
		{Topic: rw + "/multi.x", Payload: []byte("x")},
		{Topic: other + "/multi.y", Payload: []byte("y")},
	}})
	owner.waitFor("publish acknowledge", isAck(utp.PUBLISH, 7))

	id := sub.waitFor("notify", isNotify).(*utp.ControlMessage).MessageID
	pub := sub.receiveNotified(id)
	if len(pub.Messages) != 1 || string(pub.Messages[0].Payload) != "x" {
		var got []string
		for _, m := range pub.Messages {
			got = append(got, m.Topic+"="+string(m.Payload))
		}
		t.Fatalf("received %v, want only multi.x=x", got)
	}
	// The publisher's key is not handed to the subscriber.
	if pub.Messages[0].Topic != "multi.x" {
		t.Fatalf("received topic %q, want %q", pub.Messages[0].Topic, "multi.x")
	}
}
