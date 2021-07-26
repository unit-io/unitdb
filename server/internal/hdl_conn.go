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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/message/security"
	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/pkg/crypto"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/stats"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/internal/store"
	"github.com/unit-io/unitdb/server/internal/types"
	"github.com/unit-io/unitdb/server/utp"
)

const (
	requestClientId = 2682859131 // hash("clientid")
	requestKeygen   = 812942072  // hash("keygen")
)

func (c *_Conn) readLoop(ctx context.Context) (err error) {
	defer func() {
		log.Info("conn.Handler", "closing...")
		c.closeW.Done()
	}()

	reader := bufio.NewReaderSize(c.socket, 65536)

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-c.closeC:
			return nil
		default:
			// Set read/write deadlines so we can close dangling connections
			c.socket.SetDeadline(time.Now().Add(time.Second * 120))

			// Decode an incoming Message
			pkt, err := lp.Read(reader)
			if err != nil {
				return err
			}

			// Message handler
			if err = c.handler(pkt); err != nil {
				fmt.Println("read:: handler error", err)
				return err
			}
		}
	}
}

// handler handles inbound Messages.
func (c *_Conn) handler(inMsg lp.MessagePack) error {
	start := time.Now()
	var status int = 200
	defer func() {
		c.service.meter.ConnTimeSeries.AddTime(time.Since(start))
		c.service.stats.PrecisionTiming("conn_time_ns", time.Since(start), stats.IntTag("status", status))
	}()

	switch inMsg.Type() {
	// An attempt to connect.
	case utp.CONNECT:
		var returnCode uint8
		m := *inMsg.(*utp.Connect)

		c.insecure = m.InsecureFlag
		c.username = string(m.Username)
		clientID, err := c.onConnect([]byte(m.ClientID))
		if err != nil {
			status = err.Status
			returnCode = err.ReturnCode // Unauthorized
		}

		// Write the ack
		connack := &utp.ConnectAcknowledge{ReturnCode: returnCode, Epoch: int32(clientID.Epoch()), ConnID: int32(c.connID)}
		rawAck, err1 := connack.ToBinary()
		if err1 != nil {
			return types.ErrServerError
		}
		ack := &utp.ControlMessage{
			MessageType: utp.CONNECT,
			FlowControl: utp.ACKNOWLEDGE,
			Message:     rawAck.Bytes(),
		}
		c.send <- ack

		if err == types.ErrInvalidClientID {
			c.sendClientID(clientID.Encode(c.service.mac))
			return err
		}

		c.clientID = clientID
		c.MessageIds.Reset()

		// batch manager
		c.newBatchManager(&batchOptions{
			batchDuration:       time.Duration(m.BatchDuration) * time.Millisecond,
			batchByteThreshold:  int(m.BatchByteThreshold),
			batchCountThreshold: int(m.BatchCountThreshold),
		})

		var sessKey int32
		if m.SessKey != 0 {
			sessKey = m.SessKey
		} else {
			sessKey = int32(c.clientID.Epoch())
		}

		// Take care of any messages in the store
		sessID := c.sessID
		if rawSess, err := store.Session.Get(uint64(sessKey)); err == nil {
			sessID := binary.LittleEndian.Uint32(rawSess[:4])
			if !m.CleanSessFlag {
				c.resume(sessID)
			} else {
				store.Log.Reset(sessID)
			}
			c.sessID = uid.LID(sessID)
		}
		rawSess := make([]byte, 4)
		binary.LittleEndian.PutUint32(rawSess[0:4], uint32(sessID))
		store.Session.Put(uint64(sessKey), rawSess)
		if sessKey != int32(c.clientID.Epoch()) {
			store.Session.Put(uint64(c.clientID.Epoch()), rawSess)
		}
	case utp.DISCONNECT:
		c.clientDisconnect(errors.New("client initiated disconnect")) // no harm in calling this if the connection is already down (better than stopping!)
		// An attempt to relay to a topic.
	case utp.RELAY:
		m := *inMsg.(*utp.Relay)
		ack := &utp.ControlMessage{
			MessageType: utp.RELAY,
			FlowControl: utp.ACKNOWLEDGE,
			MessageID:   m.MessageID,
		}
		// Relay for each request
		for _, req := range m.RelayRequests {
			if err := c.onRelay(req); err != nil {
				status = err.Status
				c.notifyError(err, m.MessageID)
				continue
			}
		}

		if m.IsForwarded {
			return nil
		}
		c.send <- ack
	// An attempt to subscribe to a topic.
	case utp.SUBSCRIBE:
		m := *inMsg.(*utp.Subscribe)
		ack := &utp.ControlMessage{
			MessageType: utp.SUBSCRIBE,
			FlowControl: utp.ACKNOWLEDGE,
			MessageID:   m.MessageID,
		}
		// Subscribe for each subscription
		for _, subsc := range m.Subscriptions {
			if err := c.onSubscribe(m, subsc); err != nil {
				status = err.Status
				c.notifyError(err, m.MessageID)
				continue
			}

		}

		if m.IsForwarded {
			return nil
		}
		c.send <- ack

	// An attempt to unsubscribe from a topic.
	case utp.UNSUBSCRIBE:
		m := *inMsg.(*utp.Unsubscribe)
		ack := &utp.ControlMessage{
			MessageType: utp.UNSUBSCRIBE,
			FlowControl: utp.ACKNOWLEDGE,
			MessageID:   m.MessageID,
		}
		// Unsubscribe from each subscription
		for _, sub := range m.Subscriptions {
			if err := c.onUnsubscribe(m, sub); err != nil {
				status = err.Status
				c.notifyError(err, m.MessageID)
			}
		}

		c.send <- ack

	// Ping response, respond appropriately.
	case utp.PINGREQ:
		ack := &utp.ControlMessage{
			MessageType: utp.PINGREQ,
			FlowControl: utp.ACKNOWLEDGE,
		}
		c.send <- ack

	case utp.PUBLISH:
		m := *inMsg.(*utp.Publish)
		if err := c.onPublish(m); err != nil {
			status = err.Status
			c.notifyError(err, m.MessageID)
		}
	case utp.FLOWCONTROL:
		// Persist incoming
		c.storeInbound(inMsg)

		ctrlMsg := *inMsg.(*utp.ControlMessage)
		switch ctrlMsg.FlowControl {
		case utp.RECEIVE:
			key := uint64(ctrlMsg.Info().MessageID)<<32 + uint64(c.sessID)
			// Get message from Log store
			msg := store.Log.Get(key)
			if msg == nil {
				return types.ErrServerError
			}
			switch msg.(type) {
			case *utp.Publish:
				c.send <- msg
			}
		case utp.RECEIPT:
			comp := &utp.ControlMessage{
				MessageType: utp.PUBLISH,
				FlowControl: utp.COMPLETE,
				MessageID:   ctrlMsg.MessageID,
			}
			c.storeOutbound(comp)
			c.send <- comp
		}
	}

	return nil
}

// writeLook handles outbound Messages
func (c *_Conn) writeLoop(ctx context.Context) {
	defer c.closeW.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.closeC:
			return
		case pub, ok := <-c.pub:
			if !ok {
				// Channel closed.
				return
			}
			buf, err := lp.Encode(pub)
			if err != nil {
				log.Error("conn.writeLoop", err.Error())
				return
			}
			c.socket.Write(buf.Bytes())
		case outMsg, ok := <-c.send:
			if !ok {
				// Channel closed.
				return
			}
			buf, err := lp.Encode(outMsg)
			if err != nil {
				log.Error("conn.writeLoop", err.Error())
				return
			}
			c.socket.Write(buf.Bytes())
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

		return clientid, types.ErrInvalidClientID
	}

	//do not cache primary client Id
	if !clientid.IsPrimary() {
		cid := []byte(clientid.Encode(c.service.mac))
		c.service.cache.LoadOrStore(crypto.SignatureToUint32(cid[crypto.EpochSize:crypto.MessageOffset]), clientid.Contract())
	}

	return clientid, nil
}

// onRelay is a handler for Subscribe events of delivery mode type RELAY.
func (c *_Conn) onRelay(req *utp.RelayRequest) *types.Error {
	start := time.Now()
	defer log.ErrLogger.Debug().Str("context", "conn.onSubscribe").Int64("duration", time.Since(start).Nanoseconds()).Msg("")

	//Parse the key
	topic := security.ParseKey([]byte(req.Topic))
	if topic.TopicType == security.TopicInvalid {
		return types.ErrBadRequest
	}

	if !c.insecure {
		if _, err := c.onSecureRequest(topic); err != nil {
			return err
		}
	}

	if req.Last != "" {
		msgs, err := store.Message.Get(c.clientID.Contract(), topic.Topic, req.Last)
		if err != nil {
			log.Error("conn.onRelay", "query last messages"+err.Error())
			return types.ErrServerError
		}

		// Range over the messages from the store and forward them
		for _, msg := range msgs {
			newMsg := msg           // Copy message
			newMsg.DeliveryMode = 2 // Set Delivery Mode to Batch delivery of messages on relay request
			c.SendMessage(msg)
		}
	}

	return nil
}

// onSubscribe is a handler for Subscribe events.
func (c *_Conn) onSubscribe(subMsg utp.Subscribe, sub *utp.Subscription) *types.Error {
	start := time.Now()
	defer log.ErrLogger.Debug().Str("context", "conn.onSubscribe").Int64("duration", time.Since(start).Nanoseconds()).Msg("")

	//Parse the key
	topic := security.ParseKey([]byte(sub.Topic))
	if topic.TopicType == security.TopicInvalid {
		return types.ErrBadRequest
	}

	if !c.insecure {
		if _, err := c.onSecureRequest(topic); err != nil {
			return err
		}
	}

	if err := c.subscribe(subMsg, topic, sub); err != nil {
		return types.ErrServerError
	}

	return nil
}

// ------------------------------------------------------------------------------------

// onUnsubscribe is a handler for Unsubscribe events.
func (c *_Conn) onUnsubscribe(unsubMsg utp.Unsubscribe, sub *utp.Subscription) *types.Error {
	start := time.Now()
	defer log.ErrLogger.Debug().Str("context", "conn.onUnsubscribe").Int64("duration", time.Since(start).Nanoseconds()).Msg("")

	//Parse the key
	topic := security.ParseKey([]byte(sub.Topic))
	if topic.TopicType == security.TopicInvalid {
		return types.ErrBadRequest
	}

	if !c.insecure {
		if _, err := c.onSecureRequest(topic); err != nil {
			return err
		}
	}

	if err := c.unsubscribe(unsubMsg, topic); err != nil {
		return types.ErrServerError
	}

	return nil
}

// OnPublish is a handler for Publish events.
func (c *_Conn) onPublish(pub utp.Publish) *types.Error {
	start := time.Now()
	defer log.ErrLogger.Debug().Str("context", "conn.onPublish").Int64("duration", time.Since(start).Nanoseconds()).Msg("")

	for _, pubMsg := range pub.Messages {
		//Parse the key
		topic := security.ParseKey([]byte(pubMsg.Topic))
		if topic.TopicType == security.TopicInvalid {
			return types.ErrBadRequest
		}

		// Check whether the key is 'unitdb' which means it's an API request
		if len(topic.Key) == 6 && string(topic.Key) == "unitdb" {
			c.onSpecialRequest(topic, pubMsg.Payload)
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

		err := store.Message.Put(c.clientID.Contract(), topic.Topic, pubMsg.Payload, pubMsg.Ttl)
		if err != nil {
			log.Error("conn.onPublish", "store message "+err.Error())
			return types.ErrServerError
		}
		// Iterate through all subscribers and send them the message
		go c.publish(pub, topic, pubMsg)
		// time.Sleep(100*time.Millisecond)
		// panic("exit on publish")
	}

	// acknowledge a Message
	return c.acknowledge(pub)
}

// acknowledge acknowledges a Publish Message
func (c *_Conn) acknowledge(pub utp.Publish) *types.Error {
	ack := &utp.ControlMessage{
		MessageType: utp.PUBLISH,
		FlowControl: utp.ACKNOWLEDGE,
		MessageID:   pub.MessageID,
	}
	c.send <- ack
	return nil
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
	ok, wildcard := key.ValidateTopic(c.clientID.Contract(), topic.Topic[:topic.Size])
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
				Topic:   "unitdb/" + string(topic.Topic[:topic.Size]),
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
	if !c.clientID.IsPrimary() {
		return types.ErrClientIdForbidden, false
	}

	clientid, err := uid.NewSecondaryClientID(c.clientID)
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
	req := []types.KeyGenRequest{}
	if err := json.Unmarshal(payload, &req); err != nil {
		return types.ErrBadRequest, false
	}

	var resp []*types.KeyGenResponse
	// Use the cipher to generate the key
	for _, m := range req {
		key, err := security.GenerateKey(c.clientID.Contract(), []byte(m.Topic), m.Access())
		if err != nil {
			switch err {
			case security.ErrTargetTooLong:
				return types.ErrTargetTooLong, false
			default:
				return types.ErrServerError, false
			}
		}
		r := &types.KeyGenResponse{
			Status: 200,
			Key:    key,
			Topic:  m.Topic,
		}

		resp = append(resp, r)
	}

	// Success, return the response
	return resp, true
}
