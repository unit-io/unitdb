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
	"fmt"
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
	socket             net.Conn
	send               chan lp.MessagePack
	recv               chan lp.MessagePack
	pub                chan *utp.Publish
	stop               chan interface{}
	insecure           bool           // The insecure flag provided by client will not perform key validation and permissions check on the topic.
	username           string         // The username provided by the client during connect.
	message.MessageIds                // local identifier of messages
	clientID           uid.ID         // The clientid provided by client during connect or new Id assigned.
	connID             uid.LID        // The locally unique id of the connection.
	sessID             uid.LID        // The locally unique session id of the connection.
	service            *_Service      // The service for this connection.
	subs               *message.Stats // The subscriptions for this connection.
	// Reference to the cluster node where the connection has originated. Set only for cluster RPC sessions
	clnode *ClusterNode
	// Cluster nodes to inform when disconnected
	nodes map[string]bool

	// Batch
	batchManager *batchManager

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
		c.socket.Write(buf)
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

// subscribe subscribes to a particular topic.
func (c *_Conn) subscribe(subMsg utp.Subscribe, topic *security.Topic, sub *utp.Subscription) (err error) {
	c.Lock()
	defer c.Unlock()

	key, err := c.subscriptionKey(topic)
	if err != nil {
		log.ErrLogger.Err(err).Str("context", "conn.subscribe")
		return err
	}
	if !subMsg.IsForwarded && Globals.Cluster.isRemoteContract(fmt.Sprint(c.clientID.Contract())) {
		// The contract is handled by a remote node: subscribe there on the first
		// subscription. Count it locally, with no store entry, so repeats are
		// not forwarded again and unsubscribe and close know about it.
		if first := c.subs.Increment(topic.Topic[:topic.Size], key, nil); first {
			if err := Globals.Cluster.routeToContract(&subMsg, topic, message.SUBSCRIBE, &message.Message{}, c); err != nil {
				c.subs.Decrement(topic.Topic[:topic.Size], key)
				log.ErrLogger.Err(err).Str("context", "conn.subscribe").Int64("connid", int64(c.connID)).Msg("unable to subscribe to remote topic")
				return err
			}
		}
	} else {
		messageId, err := store.Subscription.NewID()
		if err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.subscribe")
			return err
		}
		if first := c.subs.Increment(topic.Topic[:topic.Size], key, messageId); first {
			// Subscribe the subscriber
			payload := make([]byte, 9)
			payload[0] = uint8(sub.DeliveryMode)
			binary.LittleEndian.PutUint32(payload[1:5], uint32(c.connID))
			binary.LittleEndian.PutUint32(payload[5:9], uint32(sub.Delay))
			if err = store.Subscription.Put(c.clientID.Contract(), messageId, topic.Topic[:topic.Size], payload); err != nil {
				log.ErrLogger.Err(err).Str("context", "conn.subscribe").Str("topic", string(topic.Topic[:topic.Size])).Int64("connid", int64(c.connID)).Msg("unable to subscribe to topic") // Unable to subscribe
				return err
			}
			// Increment the subscription counter
			c.service.meter.Subscriptions.Inc(1)
		}
	}
	return nil
}

// unsubscribe unsubscribes this client from a particular topic.
func (c *_Conn) unsubscribe(unsubMsg utp.Unsubscribe, topic *security.Topic) (err error) {
	c.Lock()
	defer c.Unlock()

	key, err := c.subscriptionKey(topic)
	if err != nil {
		log.ErrLogger.Err(err).Str("context", "conn.unsubscribe")
		return err
	}
	remote := !unsubMsg.IsForwarded && Globals.Cluster.isRemoteContract(fmt.Sprint(c.clientID.Contract()))
	// Remove the subscription from stats and if there's no more subscriptions, notify everyone.
	last, messageID := c.subs.Decrement(topic.Topic[:topic.Size], key)
	if last && messageID != nil {
		// Unsubscribe the subscriber
		if err = store.Subscription.Delete(c.clientID.Contract(), messageID, topic.Topic[:topic.Size]); err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.unsubscribe").Str("topic", string(topic.Topic[:topic.Size])).Int64("connid", int64(c.connID)).Msg("unable to unsubscribe to topic") // Unable to subscribe
			return err
		}
		// Decrement the subscription counter
		c.service.meter.Subscriptions.Dec(1)
	}
	if remote && last {
		// The topic is handled by a remote node, which holds the subscription.
		if err := Globals.Cluster.routeToContract(&unsubMsg, topic, message.UNSUBSCRIBE, &message.Message{}, c); err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.unsubscribe").Int64("connid", int64(c.connID)).Msg("unable to unsubscribe to remote topic")
			return err
		}
	}
	return nil
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
	for _, subscription := range subscriptions {
		msg.DeliveryMode = subscription[0]
		connID := uid.LID(binary.LittleEndian.Uint32(subscription[1:5]))
		msg.Delay = int32(uid.LID(binary.LittleEndian.Uint32(subscription[5:9])))
		sub := Globals.connCache.get(connID)
		if sub != nil {
			if msg.MessageID == 0 {
				msg.MessageID = uint16(c.MessageIds.NextID(utp.PUBLISH))
			}
			switch pub.DeliveryMode {
			// Publisher's DeliveryMode RELIABLE or BATCH
			case 1, 2:
				switch msg.DeliveryMode {
				// Subscriber's DeliveryMode RELIABLE or BATCH
				case 1, 2:
					// Log a copy for this subscriber until it completes the flow:
					// only this message, on the topic without the publisher's key,
					// under an id of the subscriber's connection so that messages
					// from different publishers don't overwrite each other.
					id := uint16(sub.MessageIds.NextID(utp.PUBLISH))
					store.Log.PersistOutbound(uint32(sub.sessID), &utp.Publish{
						MessageID:    id,
						DeliveryMode: msg.DeliveryMode,
						Messages: []*utp.PublishMessage{{
							Topic:   msg.Topic,
							Payload: msg.Payload,
							Ttl:     pubMsg.Ttl,
						}},
					})
					sub.queue(&utp.ControlMessage{
						MessageType: utp.PUBLISH,
						FlowControl: utp.NOTIFY,
						MessageID:   id,
					})
				// Subscriber's DeliveryMode EXPRESS
				case 0:
					if !sub.SendMessage(msg) {
						log.ErrLogger.Err(err).Str("context", "conn.publish")
					}
					msgCount++
				}
			// Publisher's DeliveryMode Express
			case 0:
				if !sub.SendMessage(msg) {
					log.ErrLogger.Err(err).Str("context", "conn.publish")
				}
				msgCount++
			}
		}
	}
	c.service.meter.OutMsgs.Inc(int64(msgCount))
	c.service.meter.OutBytes.Inc(msg.Size() * int64(msgCount))

	if !pub.IsForwarded && Globals.Cluster.isRemoteContract(fmt.Sprint(c.clientID.Contract())) {
		if err = Globals.Cluster.routeToContract(&pub, topic, message.PUBLISH, msg, c); err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.publish").Int64("connid", int64(c.connID)).Msg("unable to publish to a remote topic")
			return err
		}
	}
	return nil
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
		for _, stat := range c.subs.All() {
			if stat.ID == nil {
				continue // held by a remote node, which cleans it up on connGone
			}
			store.Subscription.Delete(c.clientID.Contract(), stat.ID, stat.Topic)
			// Decrement the subscription counter
			c.service.meter.Subscriptions.Dec(1)
		}
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
