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
	"fmt"
	"net"
	"runtime/debug"
	"strconv"
	"sync"
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
	closeW sync.WaitGroup
	closeC chan struct{}
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

	// persist outbound
	store.Log.PersistOutbound(uint32(c.connID), pub)

	// Acknowledge the publication
	select {
	case c.pub <- pub:
	case <-time.After(publishWaitTimeout):
		return false
	}

	return true
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

// subscribe subscribes to a particular topic.
func (c *_Conn) subscribe(subMsg utp.Subscribe, topic *security.Topic, sub *utp.Subscription) (err error) {
	c.Lock()
	defer c.Unlock()

	key := string(topic.Key)
	if key == "" {
		key, err = security.GenerateKey(c.clientID.Contract(), topic.Topic, security.AllowNone)
		if err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.subscribe")
			return err
		}
	}
	if exists := c.subs.Exist(key); exists && !subMsg.IsForwarded && Globals.Cluster.isRemoteContract(fmt.Sprint(c.clientID.Contract())) {
		// The contract is handled by a remote node. Forward message to it.
		if err := Globals.Cluster.routeToContract(&subMsg, topic, message.SUBSCRIBE, &message.Message{}, c); err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.subscribe").Int64("connid", int64(c.connID)).Msg("unable to subscribe to remote topic")
			return err
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
			if err = store.Subscription.Put(c.clientID.Contract(), messageId, topic.Topic, payload); err != nil {
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

	key := string(topic.Key)
	// Remove the subscription from stats and if there's no more subscriptions, notify everyone.
	if last, messageID := c.subs.Decrement(topic.Topic[:topic.Size], key); last {
		// Unsubscribe the subscriber
		if err = store.Subscription.Delete(c.clientID.Contract(), messageID, topic.Topic[:topic.Size]); err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.unsubscribe").Str("topic", string(topic.Topic[:topic.Size])).Int64("connid", int64(c.connID)).Msg("unable to unsubscribe to topic") // Unable to subscribe
			return err
		}
		// Decrement the subscription counter
		c.service.meter.Subscriptions.Dec(1)
	}
	if !unsubMsg.IsForwarded && Globals.Cluster.isRemoteContract(fmt.Sprint(c.clientID.Contract())) {
		// The topic is handled by a remote node. Forward message to it.
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
					notify := &utp.ControlMessage{
						MessageType: utp.PUBLISH,
						FlowControl: utp.NOTIFY,
						MessageID:   msg.MessageID,
					}
					// persist outbound
					store.Log.PersistOutbound(uint32(sub.sessID), &pub)
					sub.send <- notify
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

		// isKeyOutbound
		if (k & (1 << 4)) == 0 {
			switch msg.Type() {
			case utp.PUBLISH:
				pub := msg.(*utp.Publish)
				c.MessageIds.ResumeID(message.MID(pub.MessageID))
				notify := &utp.ControlMessage{
					MessageType: utp.PUBLISH,
					FlowControl: utp.NOTIFY,
					MessageID:   pub.MessageID,
				}
				c.send <- notify
			default:
				store.Log.Delete(k)
			}
		} else {
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
	err.ID = int(messageID)
	if b, err := json.Marshal(err); err == nil {
		c.SendMessage(&message.Message{
			Topic:   "unitdb/error/",
			Payload: b,
		})
	}
}

func (c *_Conn) unsubAll() {
	for _, stat := range c.subs.All() {
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
	defer c.socket.Close()

	c.batchManager.close()

	// Signal all goroutines.
	close(c.closeC)
	c.closeW.Wait()
	// Unsubscribe from everything, no need to lock since each Unsubscribe is
	// already locked. Locking the 'Close()' would result in a deadlock.
	// Don't close clustered connection, their servers are not being shut down.
	if c.clnode == nil {
		for _, stat := range c.subs.All() {
			store.Subscription.Delete(c.clientID.Contract(), stat.ID, stat.Topic)
			// Decrement the subscription counter
			c.service.meter.Subscriptions.Dec(1)
		}
	}

	Globals.connCache.delete(c.connID)
	defer log.ConnLogger.Info().Str("context", "conn.close").Int64("connid", int64(c.connID)).Msg("conn closed")
	Globals.Cluster.connGone(c)
	close(c.send)
	// Decrement the connection counter
	c.service.meter.Connections.Dec(1)
	return nil
}

// clientDisconnect cleanup when client send disconnect request or an error occurs.
func (c *_Conn) clientDisconnect(err error) {
	c.close()
}
