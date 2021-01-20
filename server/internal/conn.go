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
	"github.com/unit-io/unitdb/server/internal/net/grpc"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
)

type _Conn struct {
	sync.Mutex
	tracked uint32 // Whether the connection was already tracked or not.
	// protocol - NONE (unset), RPC, GRPC, WEBSOCK, CLUSTER
	proto  lp.ProtoAdapter
	socket net.Conn
	// send     chan []byte
	send               chan lp.Packet
	recv               chan lp.Packet
	pub                chan *lp.Publish
	stop               chan interface{}
	insecure           bool           // The insecure flag provided by client will not perform key validation and permissions check on the topic.
	username           string         // The username provided by the client during connect.
	message.MessageIds                // local identifier of messages
	clientid           uid.ID         // The clientid provided by client during connect or new Id assigned.
	connid             uid.LID        // The locally unique id of the connection.
	service            *_Service      // The service for this connection.
	subs               *message.Stats // The subscriptions for this connection.
	// Reference to the cluster node where the connection has originated. Set only for cluster RPC sessions
	clnode *_ClusterNode
	// Cluster nodes to inform when disconnected
	nodes map[string]bool

	// Close.
	closeW sync.WaitGroup
	closeC chan struct{}
}

func (s *_Service) newConn(t net.Conn, proto lp.Proto) *_Conn {
	var lineProto lp.ProtoAdapter
	switch proto {
	case lp.GRPC:
		lineProto = &grpc.LineProto{}
	case lp.GRPC_WEB:
		lineProto = &grpc.LineProto{}
	}

	c := &_Conn{
		proto:      lineProto,
		socket:     t,
		MessageIds: message.NewMessageIds(),
		send:       make(chan lp.Packet, 1), // buffered
		recv:       make(chan lp.Packet),
		pub:        make(chan *lp.Publish),
		stop:       make(chan interface{}, 1), // Buffered by 1 just to make it non-blocking
		connid:     uid.NewLID(),
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
func (s *_Service) newRpcConn(conn interface{}, connid uid.LID, clientid uid.ID) *_Conn {
	c := &_Conn{
		connid:     connid,
		clientid:   clientid,
		MessageIds: message.NewMessageIds(),
		send:       make(chan lp.Packet, 1), // buffered
		recv:       make(chan lp.Packet),
		pub:        make(chan *lp.Publish),
		stop:       make(chan interface{}, 1), // Buffered by 1 just to make it non-blocking
		service:    s,
		subs:       message.NewStats(),
		clnode:     conn.(*_ClusterNode),
		nodes:      make(map[string]bool, 3),
	}

	Globals.connCache.add(c)
	return c
}

// ID returns the unique identifier of the subscriber.
func (c *_Conn) ID() string {
	return strconv.FormatUint(uint64(c.connid), 10)
}

// Type returns the type of the subscriber
func (c *_Conn) Type() message.SubscriberType {
	return message.SubscriberDirect
}

// Send forwards the message to the underlying client.
func (c *_Conn) SendMessage(msg *message.Message) bool {
	m := lp.Publish{
		FixedHeader: lp.FixedHeader{
			Qos: msg.Qos,
		},
		MessageID: msg.MessageID, // The ID of the message
		Topic:     msg.Topic,     // The topic for this message.
		Payload:   msg.Payload,   // The payload for this message.
	}

	// Acknowledge the publication
	select {
	case c.pub <- &m:
	case <-time.After(time.Microsecond * 50):
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
func (c *_Conn) subscribe(msg lp.Subscribe, topic *security.Topic) (err error) {
	c.Lock()
	defer c.Unlock()

	key := string(topic.Key)
	if exists := c.subs.Exist(key); exists && !msg.IsForwarded && Globals.Cluster.isRemoteContract(string(c.clientid.Contract())) {
		// The contract is handled by a remote node. Forward message to it.
		if err := Globals.Cluster.routeToContract(&msg, topic, message.SUBSCRIBE, &message.Message{}, c); err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.subscribe").Int64("connid", int64(c.connid)).Msg("unable to subscribe to remote topic")
			return err
		}
		// Add the subscription to Counters
	} else {
		messageId, err := store.Subscription.NewID()
		if err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.subscribe")
		}
		if first := c.subs.Increment(topic.Topic[:topic.Size], key, messageId); first {
			// Subscribe the subscriber
			payload := make([]byte, 5)
			payload[0] = msg.Qos
			binary.LittleEndian.PutUint32(payload[1:5], uint32(c.connid))
			if err = store.Subscription.Put(c.clientid.Contract(), messageId, topic.Topic, payload); err != nil {
				log.ErrLogger.Err(err).Str("context", "conn.subscribe").Str("topic", string(topic.Topic[:topic.Size])).Int64("connid", int64(c.connid)).Msg("unable to subscribe to topic") // Unable to subscribe
				return err
			}
			// Increment the subscription counter
			c.service.meter.Subscriptions.Inc(1)
		}
	}
	return nil
}

// unsubscribe unsubscribes this client from a particular topic.
func (c *_Conn) unsubscribe(msg lp.Unsubscribe, topic *security.Topic) (err error) {
	c.Lock()
	defer c.Unlock()

	key := string(topic.Key)
	// Remove the subscription from stats and if there's no more subscriptions, notify everyone.
	if last, messageId := c.subs.Decrement(topic.Topic[:topic.Size], key); last {
		// Unsubscribe the subscriber
		if err = store.Subscription.Delete(c.clientid.Contract(), messageId, topic.Topic[:topic.Size]); err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.unsubscribe").Str("topic", string(topic.Topic[:topic.Size])).Int64("connid", int64(c.connid)).Msg("unable to unsubscribe to topic") // Unable to subscribe
			return err
		}
		// Decrement the subscription counter
		c.service.meter.Subscriptions.Dec(1)
	}
	if !msg.IsForwarded && Globals.Cluster.isRemoteContract(string(c.clientid.Contract())) {
		// The topic is handled by a remote node. Forward message to it.
		if err := Globals.Cluster.routeToContract(&msg, topic, message.UNSUBSCRIBE, &message.Message{}, c); err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.unsubscribe").Int64("connid", int64(c.connid)).Msg("unable to unsubscribe to remote topic")
			return err
		}
	}
	return nil
}

// publish publishes a message to everyone and returns the number of outgoing bytes written.
func (c *_Conn) publish(msg lp.Publish, messageID uint16, topic *security.Topic, payload []byte) (err error) {
	c.service.meter.InMsgs.Inc(1)
	c.service.meter.InBytes.Inc(int64(len(payload)))
	// subscription count
	msgCount := 0

	conns, err := store.Subscription.Get(c.clientid.Contract(), topic.Topic)
	if err != nil {
		log.ErrLogger.Err(err).Str("context", "conn.publish")
	}
	m := &message.Message{
		MessageID: messageID,
		Topic:     topic.Topic[:topic.Size],
		Payload:   payload,
	}
	for _, connid := range conns {
		qos := connid[0]
		lid := uid.LID(binary.LittleEndian.Uint32(connid[1:5]))
		sub := Globals.connCache.get(lid)
		if sub != nil {
			if qos != 0 && m.MessageID == 0 {
				mID := c.MessageIds.NextID(lp.PUBLISH)
				m.MessageID = c.outboundID(mID)
				m.Qos = qos
			}
			if !sub.SendMessage(m) {
				log.ErrLogger.Err(err).Str("context", "conn.publish")
			}
			msgCount++
		}
	}
	c.service.meter.OutMsgs.Inc(int64(msgCount))
	c.service.meter.OutBytes.Inc(m.Size() * int64(msgCount))

	if !msg.IsForwarded && Globals.Cluster.isRemoteContract(string(c.clientid.Contract())) {
		if err = Globals.Cluster.routeToContract(&msg, topic, message.PUBLISH, m, c); err != nil {
			log.ErrLogger.Err(err).Str("context", "conn.publish").Int64("connid", int64(c.connid)).Msg("unable to publish to remote topic")
		}
	}
	return err
}

// sendClientID generate unique client and send it to new client
func (c *_Conn) sendClientID(clientidentifier string) {
	c.SendMessage(&message.Message{
		Topic:   []byte("unitdb/clientid/"),
		Payload: []byte(clientidentifier),
	})
}

// notifyError notifies the connection about an error
func (c *_Conn) notifyError(err *types.Error, messageID uint16) {
	err.ID = int(messageID)
	if b, err := json.Marshal(err); err == nil {
		c.SendMessage(&message.Message{
			Topic:   []byte("unitdb/error/"),
			Payload: b,
		})
	}
}

func (c *_Conn) unsubAll() {
	for _, stat := range c.subs.All() {
		store.Subscription.Delete(c.clientid.Contract(), stat.ID, stat.Topic)
	}
}

func (c *_Conn) inboundID(id uint16) message.MID {
	return message.MID(uint32(c.connid) - uint32(id))
}

func (c *_Conn) outboundID(mid message.MID) (id uint16) {
	return uint16(uint32(c.connid) - (uint32(mid)))
}

func (c *_Conn) storeInbound(m lp.Packet) {
	if c.clientid != nil {
		blockId := uint64(c.clientid.Contract())
		k := uint64(c.inboundID(m.Info().MessageID))<<32 + blockId
		fmt.Println("inbound: type, key, qos", m.Type(), k, m.Info().Qos)
		store.Log.PersistInbound(c.proto, k, m)
	}
}

func (c *_Conn) storeOutbound(m lp.Packet) {
	if c.clientid != nil {
		blockId := uint64(c.clientid.Contract())
		k := uint64(c.inboundID(m.Info().MessageID))<<32 + blockId
		fmt.Println("inbound: type, key, qos", m.Type(), k, m.Info().Qos)
		store.Log.PersistOutbound(c.proto, k, m)
	}
}

// close terminates the connection.
func (c *_Conn) close() error {
	if r := recover(); r != nil {
		defer log.ErrLogger.Debug().Str("context", "conn.closing").Msgf("panic recovered '%v'", debug.Stack())
	}
	defer c.socket.Close()
	// Signal all goroutines.
	close(c.closeC)
	c.closeW.Wait()
	// Unsubscribe from everything, no need to lock since each Unsubscribe is
	// already locked. Locking the 'Close()' would result in a deadlock.
	// Don't close clustered connection, their servers are not being shut down.
	if c.clnode == nil {
		for _, stat := range c.subs.All() {
			store.Subscription.Delete(c.clientid.Contract(), stat.ID, stat.Topic)
			// Decrement the subscription counter
			c.service.meter.Subscriptions.Dec(1)
		}
	}

	Globals.connCache.delete(c.connid)
	defer log.ConnLogger.Info().Str("context", "conn.close").Int64("connid", int64(c.connid)).Msg("conn closed")
	Globals.Cluster.connGone(c)
	close(c.send)
	// Decrement the connection counter
	c.service.meter.Connections.Dec(1)
	return nil
}
