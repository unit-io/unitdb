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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/message/security"
	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/net/pubsub"
	"github.com/unit-io/unitdb/server/internal/pkg/crypto"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/stats"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
)

const (
	requestClientId = 2682859131 // hash("clientid")
	requestKeygen   = 812942072  // hash("keygen")
)

func (c *_Conn) readLoop() error {
	defer func() {
		log.Info("conn.Handler", "closing...")
		c.close()
	}()

	reader := bufio.NewReaderSize(c.socket, 65536)

	for {
		// Set read/write deadlines so we can close dangling connections
		c.socket.SetDeadline(time.Now().Add(time.Second * 120))

		// Decode an incoming packet
		pkt, err := lp.ReadPacket(c.adp, reader)
		if err != nil {
			fmt.Println("readPacket: err", err)
			return err
		}

		// Message handler
		if err := c.handle(pkt); err != nil {
			return err
		}
	}
}

// handle handles inbound packets.
func (c *_Conn) handle(pkt lp.LineProtocol) error {
	start := time.Now()
	var status int = 200
	defer func() {
		c.service.meter.ConnTimeSeries.AddTime(time.Since(start))
		c.service.stats.PrecisionTiming("conn_time_ns", time.Since(start), stats.IntTag("status", status))
	}()

	// Persist incoming
	c.storeInbound(pkt)

	switch pkt.Type() {
	// An attempt to connect.
	case lp.CONNECT:
		var returnCode uint8
		packet := *pkt.(*lp.Connect)

		// switch the proto adapter based on protoname in the connect packet.
		switch packet.ProtoName {
		case "TELEMETRY":
			c.adp = &pubsub.Packet{}
		case "INGESTION":
		case "QUERY":
		}
		c.insecure = packet.InsecureFlag
		c.username = string(packet.Username)
		clientid, err := c.onConnect([]byte(packet.ClientID))
		if err != nil {
			status = err.Status
			returnCode = 0x05 // Unauthorized
		}

		// Write the ack
		connack := &lp.Connack{ReturnCode: returnCode, ConnID: uint32(c.connid)}
		c.send <- connack

		if err == types.ErrInvalidClientId {
			c.sendClientID(clientid.Encode(c.service.mac))
			return err
		}

		c.clientid = clientid
		c.MessageIds.Reset(message.MID(c.connid))
		// Take care of any messages in the store
		if !packet.CleanSessFlag {
			c.resume()
		} else {
			store.Log.Reset()
		}

	// An attempt to subscribe to a topic.
	case lp.SUBSCRIBE:
		packet := *pkt.(*lp.Subscribe)
		ack := &lp.Suback{
			MessageID: packet.MessageID,
			Qos:       make([]uint8, 0, len(packet.Subscriptions)),
		}

		// Subscribe for each subscription
		for _, sub := range packet.Subscriptions {
			if err := c.onSubscribe(packet, sub.Topic); err != nil {
				status = err.Status
				ack.Qos = append(ack.Qos, 0x80) // 0x80 indicate subscription failure
				c.notifyError(err, packet.MessageID)
				continue
			}

			// Append the QoS
			ack.Qos = append(ack.Qos, sub.Qos)
		}

		if packet.IsForwarded {
			return nil
		}
		c.send <- ack

	// An attempt to unsubscribe from a topic.
	case lp.UNSUBSCRIBE:
		packet := *pkt.(*lp.Unsubscribe)
		ack := &lp.Unsuback{MessageID: packet.MessageID}

		// Unsubscribe from each subscription
		for _, sub := range packet.Subscriptions {
			if err := c.onUnsubscribe(packet, sub.Topic); err != nil {
				status = err.Status
				c.notifyError(err, packet.MessageID)
			}
		}

		c.send <- ack

	// Ping response, respond appropriately.
	case lp.PINGREQ:
		resp := &lp.Pingresp{}
		c.send <- resp

	case lp.DISCONNECT:

	case lp.PUBLISH:
		packet := *pkt.(*lp.Publish)
		if err := c.onPublish(packet, packet.MessageID, packet.Topic, packet.Payload); err != nil {
			status = err.Status
			c.notifyError(err, packet.MessageID)
		}
	case lp.PUBACK:

	case lp.PUBREC:
		packet := *pkt.(*lp.Pubrec)
		pubrel := &lp.Pubrel{
			FixedHeader: lp.FixedHeader{
				Qos: packet.Qos,
			},
			MessageID: packet.MessageID,
		}
		c.send <- pubrel

	case lp.PUBREL:
		// persist outbound
		c.storeOutbound(pkt)

		packet := *pkt.(*lp.Pubrel)
		pubcomp := &lp.Pubcomp{MessageID: packet.MessageID}
		c.send <- pubcomp

	case lp.PUBCOMP:
	}

	return nil
}

// writeLook handles outbound packets
func (c *_Conn) writeLoop(ctx context.Context) {
	c.closeW.Add(1)
	defer c.closeW.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeC:
			return
		case msg, ok := <-c.pub:
			if !ok {
				// Channel closed.
				return
			}
			m, err := lp.Encode(c.adp, msg)
			if err != nil {
				log.Error("conn.writeLoop", err.Error())
				return
			}
			c.socket.Write(m.Bytes())
		case msg, ok := <-c.send:
			if !ok {
				// Channel closed.
				return
			}
			m, err := lp.Encode(c.adp, msg)
			if err != nil {
				log.Error("conn.writeLoop", err.Error())
				return
			}
			c.socket.Write(m.Bytes())
		}
	}
}

// onConnect is a handler for Connect events.
func (c *_Conn) onConnect(clientID []byte) (uid.ID, *types.Error) {
	start := time.Now()
	defer log.ErrLogger.Debug().Str("context", "conn.onConnect").Int64("duration", time.Since(start).Nanoseconds()).Msg("")
	var clientid = uid.ID{}
	if clientID != nil && len(clientID) > c.service.mac.Overhead() {
		if contract, ok := c.service.cache.Load(crypto.SignatureToUint32(clientID[crypto.EpochSize:crypto.MessageOffset])); ok {
			clientid, err := uid.CachedClientID(contract.(uint32))
			if err != nil {
				return nil, types.ErrUnauthorized
			}
			return clientid, nil
		}
	}

	clientid, err := uid.Decode(clientID, c.service.mac)

	if err != nil {
		clientid, err = uid.NewClientID(1)
		if err != nil {
			return nil, types.ErrUnauthorized
		}

		return clientid, types.ErrInvalidClientId
	}

	//do not cache primary client Id
	if !clientid.IsPrimary() {
		cid := []byte(clientid.Encode(c.service.mac))
		c.service.cache.LoadOrStore(crypto.SignatureToUint32(cid[crypto.EpochSize:crypto.MessageOffset]), clientid.Contract())
	}

	return clientid, nil
}

// onSubscribe is a handler for Subscribe events.
func (c *_Conn) onSubscribe(pkt lp.Subscribe, msgTopic []byte) *types.Error {
	start := time.Now()
	defer log.ErrLogger.Debug().Str("context", "conn.onSubscribe").Int64("duration", time.Since(start).Nanoseconds()).Msg("")

	//Parse the key
	topic := security.ParseKey(msgTopic)
	if topic.TopicType == security.TopicInvalid {
		return types.ErrBadRequest
	}

	if !c.insecure {
		if _, err := c.onSecureRequest(topic); err != nil {
			return err
		}
	}

	// persist outbound
	c.storeOutbound(&pkt)

	c.subscribe(pkt, topic)

	// if t0, t1, limit, ok := topic.Last(); ok {
	msgs, err := store.Message.Get(c.clientid.Contract(), topic.Topic)
	if err != nil {
		log.Error("conn.OnSubscribe", "query last messages"+err.Error())
		return types.ErrServerError
	}

	// Range over the messages in the channel and forward them
	for _, m := range msgs {
		msg := m // Copy message
		c.SendMessage(&msg)
	}

	return nil
}

// ------------------------------------------------------------------------------------

// onUnsubscribe is a handler for Unsubscribe events.
func (c *_Conn) onUnsubscribe(pkt lp.Unsubscribe, msgTopic []byte) *types.Error {
	start := time.Now()
	defer log.ErrLogger.Debug().Str("context", "conn.onUnsubscribe").Int64("duration", time.Since(start).Nanoseconds()).Msg("")

	//Parse the key
	topic := security.ParseKey(msgTopic)
	if topic.TopicType == security.TopicInvalid {
		return types.ErrBadRequest
	}

	if !c.insecure {
		if _, err := c.onSecureRequest(topic); err != nil {
			return err
		}
	}

	// persist outbound
	c.storeOutbound(&pkt)

	c.unsubscribe(pkt, topic)

	return nil
}

// OnPublish is a handler for Publish events.
func (c *_Conn) onPublish(pkt lp.Publish, messageID uint16, msgTopic []byte, payload []byte) *types.Error {
	start := time.Now()
	defer log.ErrLogger.Debug().Str("context", "conn.onPublish").Int64("duration", time.Since(start).Nanoseconds()).Msg("")

	//Parse the key
	topic := security.ParseKey(msgTopic)
	if topic.TopicType == security.TopicInvalid {
		return types.ErrBadRequest
	}

	// Check whether the key is 'unitdb' which means it's an API request
	if len(topic.Key) == 5 && string(topic.Key) == "unitdb" {
		c.onSpecialRequest(topic, payload)
		return nil
	}

	if !c.insecure {
		wildcard, err := c.onSecureRequest(topic)
		if err != nil {
			return err
		}
		if wildcard {
			return types.ErrForbidden
		}
	}

	err := store.Message.Put(c.clientid.Contract(), topic.Topic, payload)
	if err != nil {
		log.Error("conn.onPublish", "store message "+err.Error())
		return types.ErrServerError
	}

	// persist outbound
	c.storeOutbound(&pkt)

	// Iterate through all subscribers and send them the message
	c.publish(pkt, messageID, topic, payload)

	// acknowledge a packet
	return c.ack(pkt)
}

// ack acknowledges a packet
func (c *_Conn) ack(pkt lp.Publish) *types.Error {
	switch pkt.FixedHeader.Qos {
	case 2:
		pubrec := &lp.Pubrec{
			FixedHeader: lp.FixedHeader{
				Qos: pkt.Qos,
			},
			MessageID: pkt.MessageID,
		}
		c.send <- pubrec
	case 1:
		puback := &lp.Puback{MessageID: pkt.MessageID}
		// persist outbound
		c.storeOutbound(puback)
		c.send <- puback
	case 0:
		// do nothing, since there is no need to send an ack packet back
	}
	return nil
}

// Load all stored messages and resend them to ensure QOS > 1,2 even after an application crash.
func (c *_Conn) resume() {
	// contract is used as blockId and key prefix
	keys := store.Log.Keys()
	for _, k := range keys {
		msg := store.Log.Get(c.adp, k)
		if msg == nil {
			continue
		}
		// isKeyOutbound
		if (k & (1 << 4)) == 0 {
			switch msg.(type) {
			case *lp.Pubrel:
				c.send <- msg
			case *lp.Publish:
				c.send <- msg
			default:
				store.Log.Delete(k)
			}
		} else {
			switch msg.(type) {
			case *lp.Pubrel:
				c.recv <- msg
			default:
				store.Log.Delete(k)
			}
		}
	}
}

func (c *_Conn) onSecureRequest(topic *security.Topic) (bool, *types.Error) {
	// Attempt to decode the key
	key, err := security.DecodeKey(topic.Key)
	if err != nil {
		return false, types.ErrBadRequest
	}

	// Check if the key has the permission to read the topic
	if !key.HasPermission(security.AllowRead) {
		return false, types.ErrUnauthorized
	}

	// Check if the key has the permission for the topic
	ok, wildcard := key.ValidateTopic(c.clientid.Contract(), topic.Topic[:topic.Size])
	if !ok {
		return wildcard, types.ErrUnauthorized
	}
	return wildcard, nil
}

// onSpecialRequest processes an special request.
func (c *_Conn) onSpecialRequest(topic *security.Topic, payload []byte) (ok bool) {
	var resp interface{}
	defer func() {
		if b, err := json.Marshal(resp); err == nil {
			c.SendMessage(&message.Message{
				Topic:   []byte("unitdb/" + string(topic.Topic[:topic.Size])),
				Payload: b,
			})
		}
	}()

	// Check query
	resp = types.ErrNotFound
	if len(topic.Topic[:topic.Size]) < 1 {
		return
	}

	switch topic.Target() {
	case requestClientId:
		resp, ok = c.onClientIDRequest()
		return
	case requestKeygen:
		resp, ok = c.onKeyGen(payload)
		return
	default:
		return
	}
}

// onClientIdRequest is a handler that returns new client id for the request.
func (c *_Conn) onClientIDRequest() (interface{}, bool) {
	if !c.clientid.IsPrimary() {
		return types.ErrClientIdForbidden, false
	}

	clientid, err := uid.NewSecondaryClientID(c.clientid)
	if err != nil {
		return types.ErrBadRequest, false
	}
	cid := clientid.Encode(c.service.mac)
	return &types.ClientIdResponse{
		Status:   200,
		ClientId: cid,
	}, true

}

// onKeyGen processes a keygen request.
func (c *_Conn) onKeyGen(payload []byte) (interface{}, bool) {
	// Deserialize the payload.
	msg := types.KeyGenRequest{}
	if err := json.Unmarshal(payload, &msg); err != nil {
		return types.ErrBadRequest, false
	}

	// Use the cipher to generate the key
	key, err := security.GenerateKey(c.clientid.Contract(), []byte(msg.Topic), msg.Access())
	if err != nil {
		switch err {
		case security.ErrTargetTooLong:
			return types.ErrTargetTooLong, false
		default:
			return types.ErrServerError, false
		}
	}

	// Success, return the response
	return &types.KeyGenResponse{
		Status: 200,
		Key:    key,
		Topic:  msg.Topic,
	}, true
}
