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

package internal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"net"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/message/security"
	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/utp"
)

type _Conn struct {
	sync.Mutex
	socket net.Conn
	send   chan lp.MessagePack
	recv   chan lp.MessagePack
	pub    chan *utp.Publish
	stop   chan interface{}
	// insecure is set for a connection whose requests skip topic key checks:
	// a trusted service's, one a service vouched for, or on a standalone
	// server with allow_insecure, one that sent the insecure flag. On a
	// connection proxied for another node, it is what that node says, per
	// request, if the node advertises capService.
	insecure atomic.Bool
	// serviceTrusted is set only by service trust (a service client id, or
	// unitdb/service), never by the insecure flag.
	serviceTrusted     atomic.Bool
	username           string         // The username provided by the client during connect.
	message.MessageIds                // local identifier of messages
	clientID           uid.ID         // The clientid provided by client during connect or new Id assigned.
	idClaims           uid.Claims     // What the client id said of itself.
	connID             uid.LID        // The locally unique id of the connection.
	sessID             uid.LID        // The locally unique session id of the connection.
	service            *_Service      // The service for this connection.
	subs               *message.Stats // The subscriptions for this connection.
	// Where the client's subscriptions are held, by subscription key. Set only
	// for client connections, not cluster RPC sessions.
	routes map[string]*subRoute
	// Reference to the cluster node where the connection has originated. Set only for cluster RPC sessions
	clnode *ClusterNode
	// Cluster nodes to inform when disconnected
	nodes map[string]bool

	// Batch
	batchManager *batchManager

	// wmu serializes writes to the socket: the write loop's, and the last
	// of a connection refused (writeNow).
	wmu sync.Mutex

	// Close.
	closeW  sync.WaitGroup
	closeC  chan struct{}
	closed  uint32
	tracked bool // counted in service.conns until closed
}

func (s *_Service) newConn(t net.Conn) *_Conn {
	sessID := uid.NewLID()
	c := &_Conn{
		socket:     t,
		MessageIds: message.NewMessageIds(),
		send:       make(chan lp.MessagePack, 1), // buffered
		recv:       make(chan lp.MessagePack),
		pub:        make(chan *utp.Publish),
		stop:       make(chan interface{}, 1), // Buffered by 1 just to make it non-blocking
		connID:     sessID,
		sessID:     sessID,
		service:    s,
		subs:       message.NewStats(),
		// Close
		closeC: make(chan struct{}),
	}

	// Increment the connection counter
	s.meter.Connections.Inc(1)

	Globals.connCache.add(c)
	return c
}

// newRpcConn a new connection in cluster
func (s *_Service) newRpcConn(conn interface{}, connID, sessID uid.LID, clientID uid.ID) *_Conn {
	c := &_Conn{
		connID:     connID,
		clientID:   clientID,
		sessID:     sessID,
		MessageIds: message.NewMessageIds(),
		send:       make(chan lp.MessagePack, 1), // buffered
		recv:       make(chan lp.MessagePack),
		pub:        make(chan *utp.Publish),
		stop:       make(chan interface{}, 1), // Buffered by 1 just to make it non-blocking
		service:    s,
		subs:       message.NewStats(),
		clnode:     conn.(*ClusterNode),
		nodes:      make(map[string]bool, 3),
		closeC:     make(chan struct{}),
	}

	Globals.connCache.add(c)
	return c
}

// ID returns the unique identifier of the subscriber.
func (c *_Conn) ID() string {
	return strconv.FormatUint(uint64(c.connID), 10)
}

// Type returns the type of the subscriber
func (c *_Conn) Type() message.SubscriberType {
	return message.SubscriberDirect
}

// Send forwards the message to the underlying client.
func (c *_Conn) SendMessage(msg *message.Message) bool {
	pubMsg := &utp.PublishMessage{
		Topic:   msg.Topic,   // The topic for this message.
		Payload: msg.Payload, // The payload for this message.
	}
	if msg.MessageID == 0 {
		msg.MessageID = uint16(c.MessageIds.NextID(utp.PUBLISH))
	}
	pub := &utp.Publish{
		MessageID:    msg.MessageID,    // The ID of the message
		DeliveryMode: msg.DeliveryMode, // The delivery mode of the message
		Messages:     []*utp.PublishMessage{pubMsg},
	}

	// Check batch, relay or delay delivery.
	if msg.DeliveryMode == 2 || msg.Delay > 0 {
		c.batchManager.add(msg.Delay, pubMsg)
		return true
	}

	// Express messages are delivered at most once, so they are not logged
	// for resume: nothing would ever remove them from the log.

	// Acknowledge the publication
	select {
	case c.pub <- pub:
	case <-c.closeC:
		return false
	case <-time.After(publishWaitTimeout):
		return false
	}

	return true
}

// writeNow writes msgs to the socket, in order, without the write loop: a
// connection refused closes once its handler returns, and the write loop
// stops at the close, maybe before writing what was queued for it.
func (c *_Conn) writeNow(msgs ...lp.MessagePack) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	for _, m := range msgs {
		buf, err := lp.Encode(m)
		if err != nil {
			return err
		}
		if _, err := c.socket.Write(buf.Bytes()); err != nil {
			return err
		}
	}
	return nil
}

// queue queues m for the write loop, unless the connection closes first.
func (c *_Conn) queue(m lp.MessagePack) bool {
	select {
	case c.send <- m:
		return true
	case <-c.closeC:
		return false
	}
}

// Send forwards raw bytes to the underlying client.
func (c *_Conn) SendRawBytes(buf []byte) bool {
	if c == nil {
		return true
	}
	c.closeW.Add(1)
	defer c.closeW.Done()

	select {
	case <-c.closeC:
		return false
	case <-time.After(time.Microsecond * 50):
		return false
	default:
		c.wmu.Lock()
		c.socket.Write(buf)
		c.wmu.Unlock()
	}

	return true
}

// subscriptionKey returns the key the connection's subscription to topic is
// counted under: the topic key, or for an insecure request without a key,
// a key generated for the topic.
func (c *_Conn) subscriptionKey(topic *security.Topic) (string, error) {
	if topic.Key != "" {
		return topic.Key, nil
	}
	return security.GenerateKey(c.clientID.Contract(), topic.Topic[:topic.Size], security.AllowNone)
}

// errPartialWildcard reports a wildcard subscription some nodes could not be
// sent. It is held elsewhere meanwhile, and sent to them again on rebalance.
var errPartialWildcard = errors.New("wildcard subscription not held by every node")

// subRoute is where a client's subscription is held in the cluster.
type subRoute struct {
	sub     *utp.Subscription // the subscription as the client sent it
	name    string            // the topic without options
	localID []byte            // the subscription's id in this node's store, if held here
	nodes   map[string]bool   // the other nodes holding it
}

// subscribe subscribes to a particular topic. A client's subscription is held
// where the cluster ring puts it (see reconcile). A subscription forwarded by
// another node is held here, once however often it is sent.
func (c *_Conn) subscribe(subMsg utp.Subscribe, topic *security.Topic, sub *utp.Subscription) (err error) {
	c.Lock()
	defer c.Unlock()

	key, err := c.subscriptionKey(topic)
	if err != nil {
		log.ErrLogger.Err(err).Str("context", "conn.subscribe")
		return err
	}
	name := topic.Topic[:topic.Size]
	if subMsg.IsForwarded {
		// Held here for a client of another node, which sends it again after
		// a rehash or a reconnect.
		if c.subs.Exist(key) {
			return nil
		}
		messageId, err := c.putSubscription(name, sub)
		if err != nil {
			return err
		}
		c.subs.Increment(name, key, messageId)
		return nil
	}
	// Count repeats locally, so that only the first is placed and only the
	// last unsubscribe removes it.
	if first := c.subs.Increment(name, key, nil); !first {
		return nil
	}
	r := &subRoute{sub: sub, name: name, nodes: make(map[string]bool)}
	// A topic's owner that is not connected, dead until the ring replaces it,
	// is tried again, as for a publish.
	deadline := time.Now().Add(forwardRetryFor)
	for {
		err := c.reconcile(r, nil)
		if err == nil {
			break
		}
		if err == errPartialWildcard {
			// Some nodes didn't take the wildcard: one dead until the ring
			// replaces it, or one that rejected it for now, as one that hasn't
			// heard this node's capabilities yet does a trusted connection's.
			// They are tried again (reconcile skips the nodes holding it),
			// then left to the rebalance.
			if time.Now().Before(deadline) {
				time.Sleep(forwardRetry)
				continue
			}
			break
		}
		if retryable(err) {
			if time.Now().Before(deadline) {
				time.Sleep(forwardRetry)
				continue
			}
			// The owner is still out of reach, as when failure detection
			// takes longer than the retries. The subscription is kept: the
			// rebalance once the ring drops the owner, or once the owner is
			// back, places it.
			log.ErrLogger.Warn().Err(err).Str("context", "conn.subscribe").Int64("connid", int64(c.connID)).Str("topic", name).Msg("topic owner out of reach: the subscription is placed when the ring changes")
			break
		}
		c.release(r)
		c.subs.Decrement(name, key)
		log.ErrLogger.Err(err).Str("context", "conn.subscribe").Int64("connid", int64(c.connID)).Msg("unable to subscribe to topic")
		return err
	}
	if c.routes == nil {
		c.routes = make(map[string]*subRoute)
	}
	c.routes[key] = r
	return nil
}

// unsubscribe unsubscribes this client from a particular topic.
func (c *_Conn) unsubscribe(unsubMsg utp.Unsubscribe, topic *security.Topic, sub *utp.Subscription) (err error) {
	c.Lock()
	defer c.Unlock()

	key, err := c.subscriptionKey(topic)
	if err != nil {
		log.ErrLogger.Err(err).Str("context", "conn.unsubscribe")
		return err
	}
	name := topic.Topic[:topic.Size]
	// Remove the subscription from stats and if there's no more subscriptions, notify everyone.
	last, messageID := c.subs.Decrement(name, key)
	if !last {
		return nil
	}
	if unsubMsg.IsForwarded {
		if messageID != nil {
			c.deleteSubscription(name, messageID)
		}
		return nil
	}
	if r := c.routes[key]; r != nil {
		delete(c.routes, key)
		c.release(r)
	}
	return nil
}

// rehome moves the connection's subscriptions to where the current ring holds
// them, and sends them again to the nodes in resend. It reports whether every
// subscription was placed, and logs the ones that were not if last is set.
func (c *_Conn) rehome(resend map[string]bool, last bool) bool {
	c.Lock()
	defer c.Unlock()
	ok := true
	for _, r := range c.routes {
		if err := c.reconcile(r, resend); err != nil {
			ok = false
			if last {
				log.ErrLogger.Err(err).Str("context", "conn.rehome").Int64("connid", int64(c.connID)).Str("topic", r.name).Msg("unable to move subscription")
			}
		}
	}
	return ok
}

// reconcile places a client's subscription where the ring holds it: here, on
// the topic's owner, or for a wildcard here and on every other node. It adds
// the new places before it removes the old ones, so the subscription is not
// missing while it moves, and keeps the old ones if the topic's owner could
// not take it. Nodes in resend are sent the subscription again although they
// hold it. It returns an error if the subscription could not be placed here or
// with the topic's owner, or errPartialWildcard if a wildcard subscription
// could not be sent to every node. The caller holds the connection's lock.
func (c *_Conn) reconcile(r *subRoute, resend map[string]bool) error {
	local, nodes := Globals.Cluster.holders(c.clientID.Contract(), r.name)
	if local && r.localID == nil {
		id, err := c.putSubscription(r.name, r.sub)
		if err != nil {
			return err
		}
		r.localID = id
	}
	var partial error
	want := make(map[string]bool, len(nodes))
	for _, n := range nodes {
		want[n] = true
		if r.nodes[n] && !resend[n] {
			continue
		}
		if err := Globals.Cluster.subscribeAt(n, r.sub, c); err != nil {
			if !isWildcardTopic(r.name) {
				return err
			}
			// A wildcard is also held by the other nodes: place it there, and
			// on this one again later.
			partial = errPartialWildcard
			continue
		}
		r.nodes[n] = true
	}
	if !local && r.localID != nil {
		c.deleteSubscription(r.name, r.localID)
		r.localID = nil
	}
	for n := range r.nodes {
		if !want[n] {
			c.unsubscribeAt(n, r)
		}
	}
	return partial
}

// release removes a client's subscription from everywhere it is held. The
// caller holds the connection's lock.
func (c *_Conn) release(r *subRoute) {
	if r.localID != nil {
		c.deleteSubscription(r.name, r.localID)
		r.localID = nil
	}
	for n := range r.nodes {
		c.unsubscribeAt(n, r)
	}
}

// unsubscribeAt removes a client's subscription from node n. A node that
// cannot be reached has left the ring or restarted, and lost it either way.
func (c *_Conn) unsubscribeAt(n string, r *subRoute) {
	if err := Globals.Cluster.unsubscribeAt(n, r.sub, c); err != nil {
		log.ErrLogger.Err(err).Str("context", "conn.unsubscribeAt").Int64("connid", int64(c.connID)).Msg("unable to unsubscribe on " + n)
	}
	delete(r.nodes, n)
}

// putSubscription stores a subscription of this connection to topic in this
// node's store, and returns its id.
func (c *_Conn) putSubscription(topic string, sub *utp.Subscription) ([]byte, error) {
	messageId, err := store.Subscription.NewID()
	if err != nil {
		log.ErrLogger.Err(err).Str("context", "conn.subscribe")
		return nil, err
	}
	// Subscribe the subscriber
	payload := make([]byte, 9)
	payload[0] = sub.DeliveryMode
	binary.LittleEndian.PutUint32(payload[1:5], uint32(c.connID))
	binary.LittleEndian.PutUint32(payload[5:9], uint32(sub.Delay))
	if err := store.Subscription.Put(c.clientID.Contract(), messageId, topic, payload); err != nil {
		log.ErrLogger.Err(err).Str("context", "conn.subscribe").Str("topic", topic).Int64("connid", int64(c.connID)).Msg("unable to subscribe to topic") // Unable to subscribe
		return nil, err
	}
	// Increment the subscription counter
	c.service.meter.Subscriptions.Inc(1)
	return messageId, nil
}

// deleteSubscription removes a subscription of this connection from this
// node's store.
func (c *_Conn) deleteSubscription(topic string, messageID []byte) {
	// Unsubscribe the subscriber
	if err := store.Subscription.Delete(c.clientID.Contract(), messageID, topic); err != nil {
		log.ErrLogger.Err(err).Str("context", "conn.unsubscribe").Str("topic", topic).Int64("connid", int64(c.connID)).Msg("unable to unsubscribe to topic") // Unable to subscribe
		return
	}
	// Decrement the subscription counter
	c.service.meter.Subscriptions.Dec(1)
}

// isReliable reports whether delivery mode is RELIABLE or BATCH.
func isReliable(mode uint8) bool {
	return mode == 1 || mode == 2
}

// publish publishes a message to everyone and returns the number of outgoing bytes written.
func (c *_Conn) publish(pub utp.Publish, topic *security.Topic, pubMsg *utp.PublishMessage) (err error) {
	c.service.meter.InMsgs.Inc(1)
	c.service.meter.InBytes.Inc(int64(len(pubMsg.Payload)))
	// subscription count
	msgCount := 0

	subscriptions, err := store.Subscription.Get(c.clientID.Contract(), topic.Topic)
	if err != nil {
		log.ErrLogger.Err(err).Str("context", "conn.publish")
		return err
	}
	msg := &message.Message{
		MessageID: pub.MessageID,
		Topic:     string(topic.Topic[:topic.Size]),
		Payload:   pubMsg.Payload,
	}
	connections := make(map[uint32]*utp.Subscription)
	for _, subscription := range subscriptions {
		connID := binary.LittleEndian.Uint32(subscription[1:5])
		if _, ok := connections[connID]; !ok {
			connections[connID] = &utp.Subscription{
				DeliveryMode: subscription[0],
				Delay:        int32(binary.LittleEndian.Uint32(subscription[5:9])),
			}
		}
	}
	// Messages for clients of other nodes, by node, sent below.
	remote := make(map[*ClusterNode][]Delivery)
	for connID, subscription := range connections {
		sub := Globals.connCache.get(uid.LID(connID))
		if sub == nil {
			continue
		}
		out := *msg
		out.DeliveryMode = subscription.DeliveryMode
		out.Delay = subscription.Delay
		// Publisher's and subscriber's DeliveryMode RELIABLE or BATCH: the
		// subscriber fetches the message from the log.
		reliable := isReliable(pub.DeliveryMode) && isReliable(subscription.DeliveryMode)
		if sub.clnode != nil {
			remote[sub.clnode] = append(remote[sub.clnode], Delivery{ConnID: sub.connID, Message: &out, Reliable: reliable})
			if !reliable {
				msgCount++
			}
			continue
		}
		if !sub.deliver(&out, reliable) {
			log.ErrLogger.Error().Str("context", "conn.publish").Int64("connid", int64(connID)).Msg("unable to deliver message")
			continue
		}
		if !reliable {
			msgCount++
		}
	}
	Globals.Cluster.deliverRemote(remote)
	c.service.meter.OutMsgs.Inc(int64(msgCount))
	c.service.meter.OutBytes.Inc(msg.Size() * int64(msgCount))

	return nil
}

// deliver delivers a message to the connection's client: logged for the client
// to fetch when reliable, sent otherwise. A proxied connection hands the
// message to the node the client is connected to, which delivers it in the
// same way, so the client's flow control and message ids stay on one node.
func (c *_Conn) deliver(m *message.Message, reliable bool) bool {
	if c.clnode != nil {
		var unused bool
		if err := c.clnode.call("Cluster.Proxy", &ClusterResp{Message: m, Reliable: reliable, FromConnID: c.connID}, &unused); err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.deliver").Int64("connid", int64(c.connID)).Msg("unable to forward message to origin node")
			return false
		}
		return true
	}
	if !reliable {
		return c.SendMessage(m)
	}
	// Log a copy for this subscriber until it completes the flow: only this
	// message, on the topic without the publisher's key, under an id of the
	// subscriber's connection so that messages from different publishers
	// don't overwrite each other.
	id := uint16(c.MessageIds.NextID(utp.PUBLISH))
	store.Log.PersistOutbound(uint32(c.sessID), &utp.Publish{
		MessageID:    id,
		DeliveryMode: m.DeliveryMode,
		Messages: []*utp.PublishMessage{{
			Topic:   m.Topic,
			Payload: m.Payload,
		}},
	})
	return c.queue(&utp.ControlMessage{
		MessageType: utp.PUBLISH,
		FlowControl: utp.NOTIFY,
		MessageID:   id,
	})
}

// Load all stored messages and resend them to ensure DeliveryMode > 1,2 even after an application crash.
func (c *_Conn) resume(prefix uint32) {
	// contract is used as blockId and key prefix
	keys := store.Log.Keys(prefix)
	for _, k := range keys {
		msg := store.Log.Get(k)
		if msg == nil {
			continue
		}

		// The log keeps publish messages until the subscriber completes the
		// flow; anything else left over is stale.
		switch msg.Type() {
		case utp.PUBLISH:
			pub := msg.(*utp.Publish)
			c.MessageIds.ResumeID(message.MID(pub.MessageID))
			notify := &utp.ControlMessage{
				MessageType: utp.PUBLISH,
				FlowControl: utp.NOTIFY,
				MessageID:   pub.MessageID,
			}
			c.queue(notify)
		default:
			store.Log.Delete(k)
		}
	}
}

// sendClientID generate unique client and send it to new client
func (c *_Conn) sendClientID(clientidentifier string) {
	c.SendMessage(&message.Message{
		Topic:   "unitdb/clientid/",
		Payload: []byte(clientidentifier),
	})
}

// notifyError notifies the connection about an error
func (c *_Conn) notifyError(err *types.Error, messageID uint16) {
	// The types.Err* values are shared by every connection, so set the ID on a
	// copy: writing it in place raced across connections and could send one
	// client's message ID to another.
	notice := *err
	notice.ID = int(messageID)
	if b, err := json.Marshal(notice); err == nil {
		c.SendMessage(&message.Message{
			Topic:   "unitdb/error/",
			Payload: b,
		})
	}
}

func (c *_Conn) unsubAll() {
	for _, stat := range c.subs.All() {
		if stat.ID == nil {
			continue // held by a remote node, which cleans it up on connGone
		}
		store.Subscription.Delete(c.clientID.Contract(), stat.ID, stat.Topic)
	}
}

// TimeNow returns current wall time in UTC rounded to milliseconds.
func TimeNow() time.Time {
	return time.Now().UTC().Round(time.Millisecond)
}

func (c *_Conn) storeInbound(m lp.MessagePack) {
	if c.clientID != nil {
		store.Log.PersistInbound(uint32(c.sessID), m)
	}
}

func (c *_Conn) storeOutbound(m lp.MessagePack) {
	if c.clientID != nil {
		store.Log.PersistOutbound(uint32(c.sessID), m)
	}
}

// close terminates the connection.
func (c *_Conn) close() error {
	if r := recover(); r != nil {
		defer log.ErrLogger.Debug().Str("context", "conn.closing").Msgf("panic recovered '%v'", debug.Stack())
	}
	if !c.setClosed() {
		return errors.New("error disconnecting client")
	}

	if c.socket != nil {
		defer c.socket.Close()
	}

	// Signal all goroutines. The read loop is blocked reading the socket, so
	// expire its read deadline; the write loop may still finish a write.
	close(c.closeC)
	if c.socket != nil {
		c.socket.SetReadDeadline(time.Now())
	}
	c.closeW.Wait()
	// Unsubscribe from everything, no need to lock since each Unsubscribe is
	// already locked. Locking the 'Close()' would result in a deadlock.
	// Don't close clustered connection, their servers are not being shut down.
	if c.clnode == nil {
		c.Lock()
		for _, r := range c.routes {
			if r.localID != nil {
				// Held by remote nodes too, which clean it up on connGone.
				c.deleteSubscription(r.name, r.localID)
			}
		}
		c.routes = nil
		c.Unlock()
	}

	Globals.connCache.delete(c.connID)
	defer log.ConnLogger.Info().Str("context", "conn.close").Int64("connid", int64(c.connID)).Msg("conn closed")
	Globals.Cluster.connGone(c)
	// c.send is left open: goroutines that may still send on it give up on
	// closeC instead.

	c.batchManager.close()

	// Decrement the connection counter
	c.service.meter.Connections.Dec(1)

	if c.tracked {
		c.service.conns.Done()
	}
	return nil
}

// clientDisconnect close connection when client send disconnect request or an error occurs.
func (c *_Conn) clientDisconnect(err error) {
	log.ConnLogger.Debug().Err(err).Str("context", "conn.internalConnLost")
	if err := c.ok(); err != nil {
		log.ConnLogger.Debug().Str("context", "conn.clientDisconnect").Int64("connid", int64(c.connID)).Msg("client disconnect called but not connected")
		return
	}
	c.close()
}

// internalConnLost close connection when connection is lost or an error occurs
func (c *_Conn) internalConnLost(err error) {
	// It is possible that internalConnLost will be called multiple times simultaneously
	// (including after sending a DisconnectMessage) as such we only do cleanup etc if the
	// routines were actually running and are not being disconnected at users request
	log.ConnLogger.Debug().Err(err).Str("context", "conn.internalConnLost")
	if err := c.ok(); err != nil {
		log.ConnLogger.Debug().Str("context", "conn.internalConnLost").Int64("connid", int64(c.connID)).Msg("not connected")
		return
	}
	c.close()
}

// Set closed flag; return true if not already closed.
func (c *_Conn) setClosed() bool {
	return atomic.CompareAndSwapUint32(&c.closed, 0, 1)
}

// Check whether connection was closed.
func (c *_Conn) isClosed() bool {
	return atomic.LoadUint32(&c.closed) != 0
}

// Check read ok status.
func (c *_Conn) ok() error {
	if c.isClosed() {
		return errors.New("client connection is closed")
	}
	return nil
}
