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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/fnv"
	"strconv"
	"time"

	"github.com/unit-io/unitdb/server/internal/keys"
	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/message/security"
	lp "github.com/unit-io/unitdb/server/internal/net"
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
	var pkt lp.MessagePack

	defer func() {
		log.Info("conn.Handler", "closing...")
		c.closeW.Done()
		if err != nil {
			c.internalConnLost(err)
		}
	}()

	reader := bufio.NewReaderSize(c.socket, 65536)

	for {
		// Set read/write deadlines so we can close dangling connections. Set
		// them before checking closeC: close expires the read deadline after
		// closing closeC, so the expired deadline is never overwritten.
		c.socket.SetDeadline(time.Now().Add(time.Second * 120))

		select {
		case <-ctx.Done():
			return nil
		case <-c.closeC:
			return nil
		default:
			// Decode an incoming Message
			pkt, err = lp.Read(reader)
			if err != nil {
				return err
			}

			// Message handler
			if err = c.handler(pkt); err != nil {
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

	// Only CONNECT is served before a client id has been accepted.
	if c.clientID == nil && inMsg.Type() != utp.CONNECT {
		return types.ErrUnauthorized
	}

	switch inMsg.Type() {
	// An attempt to connect.
	case utp.CONNECT:
		var returnCode uint8
		m := *inMsg.(*utp.Connect)

		c.insecure.Store(m.InsecureFlag)
		c.serviceTrusted.Store(false)
		c.username = string(m.Username)
		clientID, err := c.onConnect([]byte(m.ClientID))
		if err != nil {
			status = err.Status
			returnCode = err.ReturnCode // Unauthorized
		}

		// Write the ack
		var epoch int32
		if clientID != nil {
			epoch = int32(clientID.Epoch())
		}
		connack := &utp.ConnectAcknowledge{ReturnCode: returnCode, Epoch: epoch, ConnID: int32(c.connID)}
		rawAck, err1 := connack.ToBinary()
		if err1 != nil {
			return types.ErrServerError
		}
		ack := &utp.ControlMessage{
			MessageType: utp.CONNECT,
			FlowControl: utp.ACKNOWLEDGE,
			Message:     rawAck.Bytes(),
		}
		if err == types.ErrInvalidClientID || err == types.ErrV1ClientID {
			// The connection closes once this returns: the refusal, and a
			// new client id, for a client that sent none or one that does
			// not open (not for an expired one, nor a v1 one), are written
			// now. Queued, the close could stop the write loop first.
			msgs := []lp.MessagePack{ack}
			if clientID != nil {
				if text, err := c.service.issueClientID(clientID); err == nil {
					msgs = append(msgs, &utp.Publish{
						MessageID: uint16(c.MessageIds.NextID(utp.PUBLISH)),
						Messages:  []*utp.PublishMessage{{Topic: "unitdb/clientid/", Payload: []byte(text)}},
					})
				}
			}
			c.writeNow(msgs...)
			return err
		}
		c.queue(ack)
		if err != nil {
			// Refused: no session is set up. The client disconnects on the
			// refusal; any request but another CONNECT closes the connection.
			c.insecure.Store(false)
			return nil
		}

		c.clientID = clientID
		c.MessageIds.Reset()

		// batch manager
		c.newBatchManager(&batchOptions{
			batchDuration:       time.Duration(m.BatchDuration) * time.Millisecond,
			batchByteThreshold:  int(m.BatchByteThreshold),
			batchCountThreshold: int(m.BatchCountThreshold),
		})

		// A session is its owner's: the client id's. Its key is derived from
		// the client id, and its row names the owner, so no other client
		// resumes it, whatever session key it sends.
		owner := sessionOwner(c.clientID)
		sessKey := sessionKey(c.clientID, m.SessKey)
		// While the cluster has nodes older than this one, which find a
		// session with a session key by the contract and the key alone, the
		// session is also looked up and kept there, so that a client moving
		// between old and new nodes keeps it.
		var legacyKey uint64
		if m.SessKey != 0 && Globals.Cluster.hasOlderPeers() {
			legacyKey = legacySessionKey(c.clientID, m.SessKey)
		}

		// Other nodes may hold the session: its replicas, and the nodes the
		// client was connected to before.
		if !m.CleanSessFlag {
			Globals.Cluster.fetchSession(sessKey)
		}

		// Take care of any messages in the store
		sessID, found, foreign := ownedSession(sessKey, owner, true)
		keepLegacy := false
		if legacyKey != 0 {
			if !found && !m.CleanSessFlag {
				Globals.Cluster.fetchSession(legacyKey)
			}
			// An old node's row names no owner: it is taken, as an old node
			// takes it, only while old nodes are in the cluster.
			id, owned, other := ownedSession(legacyKey, owner, true)
			if !found && owned {
				sessID, found = id, true
			}
			keepLegacy = !other
		}
		if found {
			if !m.CleanSessFlag {
				c.resume(sessID)
			} else {
				store.Log.Reset(sessID)
			}
			c.sessID = uid.LID(sessID)
		}
		rawSess := sessionRow(uint32(c.sessID), owner)
		if !foreign {
			store.Session.Put(sessKey, rawSess)
		}
		if m.SessKey != 0 {
			store.Session.Put(owner, rawSess)
			if keepLegacy {
				store.Session.Put(legacyKey, rawSess)
			}
		}
		// An id near the end of its lifetime, or of a key being retired,
		// is sent again as a new one.
		c.renewClientID()
	case utp.DISCONNECT:
		go c.clientDisconnect(errors.New("client initiated disconnect")) // no harm in calling this if the connection is already down (better than stopping!)
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
			if err := c.onRelay(m, req); err != nil {
				status = err.Status
				c.notifyError(err, m.MessageID)
				continue
			}
		}

		if m.IsForwarded {
			return nil
		}

		c.queue(ack)
	// An attempt to subscribe to a topic.
	case utp.SUBSCRIBE:
		m := *inMsg.(*utp.Subscribe)
		ack := &utp.ControlMessage{
			MessageType: utp.SUBSCRIBE,
			FlowControl: utp.ACKNOWLEDGE,
			MessageID:   m.MessageID,
		}
		// Subscribe for each subscription
		for _, sub := range m.Subscriptions {
			if err := c.onSubscribe(m, sub); err != nil {
				status = err.Status
				c.notifyError(err, m.MessageID)
				continue
			}

		}

		if m.IsForwarded {
			return nil
		}

		c.queue(ack)

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

		if m.IsForwarded {
			return nil
		}

		c.queue(ack)

	// Ping response, respond appropriately.
	case utp.PINGREQ:
		ack := &utp.ControlMessage{
			MessageType: utp.PINGREQ,
			FlowControl: utp.ACKNOWLEDGE,
		}
		c.queue(ack)

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
				// The message is gone, for example after a clean session: tell
				// the client the flow is complete so it stops asking for it.
				c.queue(&utp.ControlMessage{
					MessageType: utp.PUBLISH,
					FlowControl: utp.COMPLETE,
					MessageID:   ctrlMsg.MessageID,
				})
				return nil
			}
			switch msg.(type) {
			case *utp.Publish:
				c.queue(msg)
			}
		case utp.RECEIPT:
			comp := &utp.ControlMessage{
				MessageType: utp.PUBLISH,
				FlowControl: utp.COMPLETE,
				MessageID:   ctrlMsg.MessageID,
			}
			c.storeOutbound(comp)
			c.queue(comp)
		}
	}

	return nil
}

// writeLook handles outbound Messages
func (c *_Conn) writeLoop(ctx context.Context) (err error) {
	var buf bytes.Buffer

	defer func() {
		c.closeW.Done()
		if err != nil {
			c.internalConnLost(err)
		}
	}()

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
			buf, err = lp.Encode(pub)
			if err != nil {
				return err
			}
			c.wmu.Lock()
			c.socket.Write(buf.Bytes())
			c.wmu.Unlock()
		case outMsg, ok := <-c.send:
			if !ok {
				// Channel closed.
				return
			}
			buf, err = lp.Encode(outMsg)
			if err != nil {
				return err
			}
			c.wmu.Lock()
			c.socket.Write(buf.Bytes())
			c.wmu.Unlock()
		}
	}
}

// sessionKey returns the store key of a connection's session. The key is
// derived from the client's contract, so a client can never resume the
// session of another contract; then from the session's owner, the whole
// client id, so that no client resumes another's session (it used to be
// derived from the contract and the client's session key alone, which any
// client of the contract could send); and last from the session key the
// client sent, if any. Without a session key, the key is the one earlier
// versions used. The whole client id includes the uuid of an id made since
// v2 client ids, so that secondary ids of a contract issued in the same
// second, identical in v1, own different sessions; an id renewed, or sealed
// again from v1 as v2 (server/cmd/mintid -from, or a v0.6.0 server's
// renewal), is the same id and keeps its sessions.
// The top bit keeps session keys apart from message log keys.
func sessionKey(clientID uid.ID, sessKey int32) uint64 {
	h := fnv.New64a()
	h.Write(clientID[8:12]) // contract
	h.Write([]byte{'c'})
	h.Write(clientID)
	if sessKey != 0 {
		var b [5]byte
		b[0] = 's'
		binary.LittleEndian.PutUint32(b[1:], uint32(sessKey))
		h.Write(b[:])
	}
	return h.Sum64() | 1<<63
}

// legacySessionKey is the key versions up to v0.5.0 gave a session with a
// session key: derived from the contract and the session key only.
func legacySessionKey(clientID uid.ID, sessKey int32) uint64 {
	h := fnv.New64a()
	h.Write(clientID[8:12]) // contract
	var b [5]byte
	b[0] = 's'
	binary.LittleEndian.PutUint32(b[1:], uint32(sessKey))
	h.Write(b[:])
	return h.Sum64() | 1<<63
}

// sessionOwner identifies the owner of a session, kept in its row: the
// client id, within its contract. It is also the key of the client's
// session without a session key.
func sessionOwner(clientID uid.ID) uint64 {
	return sessionKey(clientID, 0)
}

// sessionRowLen is the length of a session row: the session id, and its
// owner. Rows of versions up to v0.5.0 hold only the session id.
const sessionRowLen = 12

// sessionRow returns the session row of session sessID owned by owner.
func sessionRow(sessID uint32, owner uint64) []byte {
	row := make([]byte, sessionRowLen)
	binary.LittleEndian.PutUint32(row[0:4], sessID)
	binary.LittleEndian.PutUint64(row[4:12], owner)
	return row
}

// ownedSession reads the session row under key. owned reports a row of
// owner, or, if unowned is set, a row that names no owner, as versions up to
// v0.5.0 wrote; foreign reports a row of another owner, which is left alone.
func ownedSession(key, owner uint64, unowned bool) (sessID uint32, owned, foreign bool) {
	row, err := store.Session.Get(key)
	if err != nil || len(row) < 4 {
		return 0, false, false
	}
	if len(row) < sessionRowLen {
		if !unowned {
			return 0, false, true
		}
	} else if binary.LittleEndian.Uint64(row[4:12]) != owner {
		log.ErrLogger.Warn().Str("context", "conn.ownedSession").Msg("refused to resume a session of another owner")
		return 0, false, true
	}
	return binary.LittleEndian.Uint32(row[:4]), true, false
}

// onConnect is a handler for Connect events.
func (c *_Conn) onConnect(clientID []byte) (uid.ID, *types.Error) {
	start := time.Now()
	defer func() {
		log.ErrLogger.Debug().Str("context", "conn.onConnect").Int64("duration", time.Since(start).Nanoseconds()).Msg("")
	}()
	// Every client id is opened: its seal is what proves the server issued
	// it, with the key it names.
	clientid, claims, err := c.service.keys.OpenClientID(clientID)
	if err == keys.ErrV1ClientID {
		// Refused without a new id, which would be of another contract: the
		// id's owner seals it again as a v2 one, of the same contract.
		log.ErrLogger.Info().Str("context", "conn.onConnect").Msg("refused a v1 client id")
		return nil, types.ErrV1ClientID
	}
	if err != nil {
		clientid, err = uid.NewClientID(1)
		if err != nil {
			return nil, types.ErrUnauthorized
		}

		return clientid, types.ErrInvalidClientID
	}
	if claims.Expired(time.Now().Unix()) {
		// Refused without a new id: the client's owner issues it another.
		return nil, types.ErrInvalidClientID
	}
	// Revoked, or issued before its contract's not-before time: refused
	// without a new id, as an expired one.
	if c.service.revocations.refuses(clientid.Contract(), clientid.Uuid(), claims.IssuedAt) != "" {
		return nil, types.ErrInvalidClientID
	}

	// The insecure flag skips every topic key check, so a client's own flag
	// is taken only by a server that allows insecure clients. A trusted
	// service's id has the trust sealed in (uid.AllowService), with or
	// without the flag.
	service := clientid.IsService()
	if c.insecure.Load() && !service && !c.service.allowInsecure.Load() {
		return nil, types.ErrUnauthorized
	}
	if service {
		c.insecure.Store(true)
		c.serviceTrusted.Store(true)
	}
	c.idClaims = claims

	return clientid, nil
}

// onRelay is a handler for Subscribe events of delivery mode type RELAY.
func (c *_Conn) onRelay(relayMsg utp.Relay, req *utp.RelayRequest) *types.Error {
	start := time.Now()
	defer func() {
		log.ErrLogger.Debug().Str("context", "conn.onSubscribe").Int64("duration", time.Since(start).Nanoseconds()).Msg("")
	}()

	//Parse the key
	topic := security.ParseKey(req.Topic)
	if topic.TopicType == security.TopicInvalid {
		return types.ErrBadRequest
	}
	if security.IsReserved(topic.Topic[:topic.Size]) {
		return types.ErrForbidden
	}

	if !c.insecure.Load() {
		if _, err := c.onSecureRequest(topic, security.AllowRead); err != nil {
			return err
		}
	}

	// A topic's owner holds all its messages: those it stored as the owner, and
	// as a replica of the owner before it. The store does not match wildcard
	// queries, so a wildcard is answered here, as by a standalone server.
	if name := topic.Topic[:topic.Size]; !relayMsg.IsForwarded && !isWildcardTopic(name) && Globals.Cluster != nil {
		fwd := &utp.Relay{MessageID: relayMsg.MessageID, RelayRequests: []*utp.RelayRequest{req}}
		if Globals.Cluster.relayFromHolder(fwd, c.clientID.Contract(), name, c) {
			return nil
		}
	}

	if req.Last != "" {
		msgs, err := store.Message.GetAll(c.clientID.Contract(), topic.Topic, req.Last)
		if err != nil {
			log.Error("conn.onRelay", "query last messages"+err.Error())
			return types.ErrServerError
		}

		// Range over the messages from the store and forward them
		for _, msg := range msgs {
			newMsg := msg           // Copy message
			newMsg.DeliveryMode = 2 // Set Delivery Mode to Batch delivery on relay request
			c.deliver(newMsg, false)
		}
	}

	return nil
}

// onSubscribe is a handler for Subscribe events.
func (c *_Conn) onSubscribe(subMsg utp.Subscribe, sub *utp.Subscription) *types.Error {
	start := time.Now()
	defer func() {
		log.ErrLogger.Debug().Str("context", "conn.onSubscribe").Int64("duration", time.Since(start).Nanoseconds()).Msg("")
	}()

	//Parse the key
	topic := security.ParseKey(sub.Topic)
	if topic.TopicType == security.TopicInvalid {
		return types.ErrBadRequest
	}
	if security.IsReserved(topic.Topic[:topic.Size]) {
		return types.ErrForbidden
	}

	if !c.insecure.Load() {
		if _, err := c.onSecureRequest(topic, security.AllowRead); err != nil {
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
	defer func() {
		log.ErrLogger.Debug().Str("context", "conn.onUnsubscribe").Int64("duration", time.Since(start).Nanoseconds()).Msg("")
	}()

	//Parse the key
	topic := security.ParseKey(sub.Topic)
	if topic.TopicType == security.TopicInvalid {
		return types.ErrBadRequest
	}
	if security.IsReserved(topic.Topic[:topic.Size]) {
		return types.ErrForbidden
	}

	if !c.insecure.Load() {
		if _, err := c.onSecureRequest(topic, security.AllowRead); err != nil {
			return err
		}
	}

	if err := c.unsubscribe(unsubMsg, topic, sub); err != nil {
		return types.ErrServerError
	}

	return nil
}

// OnPublish is a handler for Publish events.
func (c *_Conn) onPublish(pub utp.Publish) *types.Error {
	start := time.Now()
	defer func() {
		log.ErrLogger.Debug().Str("context", "conn.onPublish").Int64("duration", time.Since(start).Nanoseconds()).Msg("")
	}()

	for _, pubMsg := range pub.Messages {
		//Parse the key
		topic := security.ParseKey(pubMsg.Topic)
		if topic.TopicType == security.TopicInvalid {
			return types.ErrBadRequest
		}

		// Check whether the key is 'unitdb' which means it's an API request
		if len(topic.Key) == 6 && string(topic.Key) == "unitdb" {
			// Answered by the client's own node: a node forwards none, so
			// one that comes forwarded is dropped, rather than vouch for a
			// proxied connection.
			if !pub.IsForwarded {
				c.onSpecialRequest(topic, pubMsg.Payload)
			}
			continue
		}
		if security.IsReserved(topic.Topic[:topic.Size]) {
			return types.ErrForbidden
		}

		if !c.insecure.Load() {
			wildcard, err := c.onSecureRequest(topic, security.AllowWrite)
			if err != nil {
				return err
			}
			if wildcard {
				return types.ErrForbidden
			}
		}

		if name := topic.Topic[:topic.Size]; !pub.IsForwarded && Globals.Cluster.isRemoteTopic(c.clientID.Contract(), name) {
			// The topic's owner stores the message and delivers it.
			fwd := &utp.Publish{MessageID: pub.MessageID, DeliveryMode: pub.DeliveryMode, Messages: []*utp.PublishMessage{pubMsg}}
			forwarded, err := Globals.Cluster.routeToTopic(fwd, c.clientID.Contract(), name, c)
			if err != nil {
				log.Error("conn.onPublish", "forward to topic owner "+err.Error())
				return types.ErrServerError
			}
			if forwarded {
				continue
			}
			// The topic moved to this node while the publish was retried.
		}

		err := store.Message.Put(c.clientID.Contract(), topic.Topic, pubMsg.Payload, pubMsg.Ttl)
		if err != nil {
			log.Error("conn.onPublish", "store message "+err.Error())
			return types.ErrServerError
		}
		// A reliable or batch publish is acknowledged once a replica stores it
		// too, so that it survives this node failing right after.
		Globals.Cluster.replicate(c.clientID.Contract(), topic.Topic[:topic.Size], topic.Topic, pubMsg.Payload, pubMsg.Ttl, Globals.Cluster.waitsForReplica(isReliable(pub.DeliveryMode)))
		// Iterate through all subscribers and send them the message
		c.service.inflight.Add(1)
		go func(topic *security.Topic, pubMsg *utp.PublishMessage) {
			defer c.service.inflight.Done()
			c.publish(pub, topic, pubMsg)
		}(topic, pubMsg)
	}

	if pub.IsForwarded {
		return nil
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
	c.queue(ack)
	return nil
}

// onSecureRequest checks that the topic key is valid for the topic and grants
// the permission the request needs: AllowRead to subscribe, unsubscribe or
// relay, AllowWrite to publish. It reports whether the key is a wildcard's,
// which is not taken to publish.
//
// Only v2 keys are taken, whose tag covers the whole topic, that have not
// expired nor been revoked. A v1 signed key or an unsigned one is refused
// since v0.7.0 (types.ErrV1Key, which says why).
func (c *_Conn) onSecureRequest(topic *security.Topic, permission uint32) (bool, *types.Error) {
	switch len(topic.Key) {
	case security.KeyLenV2:
	case security.V1KeyLen, security.UnsignedKeyLen:
		return false, types.ErrV1Key
	default:
		return false, types.ErrBadRequest
	}
	key, err := c.service.keys.DecodeTopicKeyV2(c.clientID.Contract(), topic.Key, topic.Topic[:topic.Size])
	switch err {
	case nil:
	case security.ErrInvalidSignature:
		// A key issued for another topic or contract, or not by this
		// server's keys.
		return false, types.ErrUnauthorized
	default:
		return false, types.ErrBadRequest
	}
	if !key.HasPermission(permission) {
		return key.Wildcard, types.ErrUnauthorized
	}
	if key.Expired(time.Now().Unix()) {
		return key.Wildcard, types.ErrUnauthorized
	}
	if c.service.revocations.refuses(c.clientID.Contract(), key.Uuid, key.IssuedAt) != "" {
		return key.Wildcard, types.ErrUnauthorized
	}
	return key.Wildcard, nil
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
	case requestService:
		resp, ok = c.onService(payload)
		return
	case requestRevoke:
		resp, ok = c.onRevoke(payload)
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
	cid, err := c.service.issueClientID(clientid)
	if err != nil {
		return types.ErrServerError, false
	}
	resp := &types.ClientIdResponse{
		Status:   200,
		ClientId: cid,
	}
	// To revoke it.
	resp.Uuid = strconv.FormatUint(clientid.Uuid(), 10)
	return resp, true

}

// onKeyGen processes a keygen request.
func (c *_Conn) onKeyGen(payload []byte) (interface{}, bool) {
	// Keys grant access to the whole contract, so only its primary client
	// may generate them, or a connection trusted as a service's (a service
	// client id, or one a service vouched for with unitdb/service), whose
	// requests skip key checks anyway: a service hands its users keys. Not a
	// client that only sent the insecure flag: its keys would outlast
	// insecure mode.
	if !c.clientID.IsPrimary() && !c.serviceTrusted.Load() {
		return types.ErrKeyGenForbidden, false
	}

	// Deserialize the payload.
	req := []types.KeyGenRequest{}
	if err := json.Unmarshal(payload, &req); err != nil {
		return types.ErrBadRequest, false
	}

	var resp []*types.KeyGenResponse
	// Use the cipher to generate the key
	for _, m := range req {
		if security.IsReserved(m.Topic) {
			return types.ErrForbidden, false
		}
		ttl := c.service.topicKeyTTL
		if m.Ttl != "" {
			d, err := time.ParseDuration(m.Ttl)
			if err != nil || d < 0 {
				return types.ErrBadRequest, false
			}
			ttl = d
		}
		key, err := c.service.keys.TopicKey(c.clientID.Contract(), m.Topic, m.Access(), ttl)
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
			// To revoke it.
			Uuid: strconv.FormatUint(security.KeyUuidV2(key), 10),
		}

		resp = append(resp, r)
	}

	// Success, return the response
	return resp, true
}
