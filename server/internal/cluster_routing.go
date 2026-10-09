/*
 * Copyright 2026 Saffat Technologies, Ltd.
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

// Routing clients' requests to the nodes holding their topics, the
// stand-ins those nodes keep for other nodes' clients, and delivery back to
// the clients: an independent implementation of
// docs/design/cluster-spec.md, sections 1.7 to 1.9.
//
// A client's node forwards a request (Forward) with the client's
// connection id. The receiving node keeps a stand-in connection for each
// (client node, connection id): its own connection id, from this process's
// counter, so it never clashes with another connection here; requests for
// one stand-in are handled one at a time, in the order they arrived.
// What the stand-in sends its client goes back to the client's node
// (ToClient, Deliver), which writes it to the client's own connection, and
// takes it only from a node that connection's requests were forwarded to.

import (
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/unit-io/unitdb/server/internal/message"
	"github.com/unit-io/unitdb/server/internal/message/security"
	lp "github.com/unit-io/unitdb/server/internal/net"
	"github.com/unit-io/unitdb/server/internal/peerwire"
	"github.com/unit-io/unitdb/server/internal/pkg/log"
	"github.com/unit-io/unitdb/server/internal/pkg/uid"
	"github.com/unit-io/unitdb/server/utp"
)

// ForwardReq is a client's request forwarded to the node holding its
// topic, or with Gone the end of the client's connection.
type ForwardReq struct {
	Node     string
	ConnID   uid.LID // the connection on the client's node
	SessID   uid.LID
	ClientID uid.ID
	// Insecure is the connection's flag for a trusted service.
	Insecure bool
	// One of the four requests.
	Publish     *utp.Publish
	Subscribe   *utp.Subscribe
	Unsubscribe *utp.Unsubscribe
	Relay       *utp.Relay
	Gone        bool
}

// ForwardResp is the answer: Rejected if the node did not take the
// request, by its own ring.
type ForwardResp struct {
	Rejected bool
}

// ToClientReq hands a client of the receiving node a message, or raw bytes
// of its protocol, from the node holding its subscription or request.
type ToClientReq struct {
	Node     string
	ConnID   uid.LID
	Message  *message.Message
	Reliable bool
	Raw      []byte
}

// Delivery is a message for a client of another node.
type Delivery struct {
	ConnID   uid.LID // the connection on the client's node
	Message  *message.Message
	Reliable bool
}

// DeliverReq is every delivery for one node's clients of one publish.
type DeliverReq struct {
	Node       string
	Deliveries []Delivery
}

// ResyncReq asks a node to send its clients' subscriptions to the sender
// again.
type ResyncReq struct {
	Node string
}

// forwardedTo guards every client connection's nodes: written as requests
// are forwarded, from goroutines of their own, and read as it closes.
var forwardedTo sync.Mutex

// noteForwarded records that conn's requests went to node.
func noteForwarded(conn *_Conn, node string) {
	forwardedTo.Lock()
	defer forwardedTo.Unlock()
	if conn.nodes == nil {
		conn.nodes = make(map[string]bool)
	}
	conn.nodes[node] = true
}

// forwardedNodes returns the nodes conn's requests went to.
func forwardedNodes(conn *_Conn) []string {
	forwardedTo.Lock()
	defer forwardedTo.Unlock()
	names := make([]string, 0, len(conn.nodes))
	for n := range conn.nodes {
		names = append(names, n)
	}
	return names
}

func wasForwardedTo(conn *_Conn, node string) bool {
	forwardedTo.Lock()
	defer forwardedTo.Unlock()
	return conn.nodes[node]
}

// forward sends a request of conn's client to node n, and waits until n
// handled it. errRejected if n did not take it.
func (c *Cluster) forward(n *ClusterNode, conn *_Conn, req *ForwardReq) error {
	req.Node = c.thisNodeName
	req.ConnID, req.SessID, req.ClientID = conn.connID, conn.sessID, conn.clientID
	req.Insecure = conn.insecure.Load()
	noteForwarded(conn, n.name)
	var resp ForwardResp
	if err := n.callTimeout("Cluster.Forward", req, &resp, forwardTimeout); err != nil {
		if errors.Is(err, peerwire.ErrTimeout) {
			c.health.forwardTimeouts.Add(1)
		}
		return err
	}
	if resp.Rejected {
		return errRejected
	}
	return nil
}

// isRemoteTopic reports whether another node owns topic.
func (c *Cluster) isRemoteTopic(contract uint32, topic string) bool {
	if c == nil || isWildcardTopic(topic) {
		return false
	}
	return c.getRing().Get(topicRingKey(contract, topic)) != c.thisNodeName
}

// routeToTopic forwards a publish to its topic's owner. It reports false if
// this node owns the topic by the time it checks: the caller stores it. A
// request not processed (not sent, or not taken) goes again to the owner
// in the current ring, for up to forwardRetryFor.
func (c *Cluster) routeToTopic(msg lp.MessagePack, contract uint32, topic string, conn *_Conn) (bool, error) {
	pub, ok := msg.(*utp.Publish)
	if !ok {
		return true, fmt.Errorf("cluster.routeToTopic: not a publish: %T", msg)
	}
	key := topicRingKey(contract, topic)
	deadline := time.Now().Add(forwardRetryFor)
	for {
		owner := c.getRing().Get(key)
		if owner == c.thisNodeName {
			return false, nil
		}
		n := c.nodes[owner]
		if n == nil {
			return true, errors.New("cluster.routeToTopic: no node " + owner)
		}
		err := c.forward(n, conn, &ForwardReq{Publish: pub})
		if err == nil || !retryable(err) || time.Now().After(deadline) {
			return true, err
		}
		time.Sleep(forwardRetry)
	}
}

// relayFromHolder sends a relay to the first of the topic's holders that
// takes it, and reports true; false to answer it here. This node, if it
// comes first, answers it, unless it is catching up.
func (c *Cluster) relayFromHolder(msg *utp.Relay, contract uint32, topic string, conn *_Conn) bool {
	for _, h := range c.getRing().GetN(topicRingKey(contract, topic), c.replicas) {
		if h == c.thisNodeName {
			if !c.catchingUp() {
				return false
			}
			continue
		}
		n := c.nodes[h]
		if n == nil {
			continue
		}
		if err := c.forward(n, conn, &ForwardReq{Relay: msg}); err != nil {
			log.ErrLogger.Warn().Err(err).Str("context", "cluster.relayFromHolder").Str("holder", h).Msg("relay not taken: trying the next holder")
			continue
		}
		return true
	}
	return false
}

// catchingUp reports whether this node is copying its topics' messages.
func (c *Cluster) catchingUp() bool {
	return c.rebuilding.Load() || c.reconciling.Load()
}

// holders says where a client's subscription to topic belongs: here
// (local), and on which other nodes.
func (c *Cluster) holders(contract uint32, topic string) (bool, []string) {
	if c == nil {
		return true, nil
	}
	if isWildcardTopic(topic) {
		var others []string
		for _, n := range c.getRingNodes() {
			if n != c.thisNodeName {
				others = append(others, n)
			}
		}
		return true, others
	}
	if owner := c.getRing().Get(topicRingKey(contract, topic)); owner != c.thisNodeName && owner != "" {
		return false, []string{owner}
	}
	return true, nil
}

// subscribeAt places conn's client's subscription on node name.
func (c *Cluster) subscribeAt(name string, sub *utp.Subscription, conn *_Conn) error {
	n := c.nodes[name]
	if n == nil {
		return errors.New("cluster.subscribeAt: no node " + name)
	}
	return c.forward(n, conn, &ForwardReq{Subscribe: &utp.Subscribe{Subscriptions: []*utp.Subscription{sub}}})
}

// unsubscribeAt removes conn's client's subscription from node name.
func (c *Cluster) unsubscribeAt(name string, sub *utp.Subscription, conn *_Conn) error {
	n := c.nodes[name]
	if n == nil {
		return errors.New("cluster.unsubscribeAt: no node " + name)
	}
	return c.forward(n, conn, &ForwardReq{Unsubscribe: &utp.Unsubscribe{Subscriptions: []*utp.Subscription{sub}}})
}

// connGone tells every node conn's requests went to that it closed: they
// drop its stand-ins, and their subscriptions. It returns the first error.
func (c *Cluster) connGone(conn *_Conn) error {
	if c == nil || conn.clnode != nil {
		return nil
	}
	names := forwardedNodes(conn)
	errs := make(chan error, len(names))
	for _, name := range names {
		n := c.nodes[name]
		if n == nil {
			errs <- nil
			continue
		}
		go func(n *ClusterNode) {
			var resp ForwardResp
			errs <- n.callTimeout("Cluster.Forward", &ForwardReq{Node: c.thisNodeName, ConnID: conn.connID, Gone: true}, &resp, leaveWait)
		}(n)
	}
	var first error
	for range names {
		if err := <-errs; err != nil && first == nil {
			first = err
		}
	}
	return first
}

// proxyKey names a stand-in: the client's node and its connection there.
type proxyKey struct {
	node string
	conn uid.LID
}

// standIn is this node's stand-in for another node's client.
type standIn struct {
	key  proxyKey
	node *ClusterNode

	mu      sync.Mutex
	jobs    []func()
	running bool
	// conn is the stand-in connection, once a request was taken.
	conn *_Conn
}

// enqueue runs job after the jobs queued before it, on a goroutine of the
// stand-in.
func (s *standIn) enqueue(job func()) {
	s.mu.Lock()
	s.jobs = append(s.jobs, job)
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.mu.Unlock()
	go func() {
		for {
			s.mu.Lock()
			if len(s.jobs) == 0 {
				s.running = false
				s.mu.Unlock()
				return
			}
			job := s.jobs[0]
			s.jobs = s.jobs[1:]
			s.mu.Unlock()
			job()
		}
	}()
}

// serveForward takes a forwarded request, on the connection's read loop,
// and queues it behind the earlier ones of its client connection.
func (c *Cluster) serveForward(reqAny interface{}, reply func(interface{}, error)) {
	req := reqAny.(*ForwardReq)
	n := c.nodes[req.Node]
	key := proxyKey{node: req.Node, conn: req.ConnID}
	c.proxyMu.Lock()
	s := c.proxies[key]
	if s == nil {
		if req.Gone {
			c.proxyMu.Unlock()
			reply(&ForwardResp{}, nil)
			return
		}
		s = &standIn{key: key, node: n}
		c.proxies[key] = s
	}
	if req.Gone {
		delete(c.proxies, key)
	}
	c.proxyMu.Unlock()
	s.enqueue(func() {
		resp, err := c.handleForward(s, req)
		reply(resp, err)
	})
}

// handleForward handles a forwarded request on the stand-in's goroutine.
func (c *Cluster) handleForward(s *standIn, req *ForwardReq) (resp *ForwardResp, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.ErrLogger.Error().Msgf("cluster: panic handling a forwarded request: %v\n%s", r, debug.Stack())
			resp, err = nil, errors.New("cluster: the forwarded request failed")
		}
	}()
	if req.Gone {
		s.stop()
		return &ForwardResp{}, nil
	}
	var msg lp.MessagePack
	switch {
	case req.Publish != nil:
		if !c.takesPublish(req.ClientID.Contract(), req.Publish) {
			return &ForwardResp{Rejected: true}, nil
		}
		req.Publish.IsForwarded = true
		msg = req.Publish
	case req.Relay != nil:
		if !c.takesRelay(req.ClientID.Contract(), req.Relay) {
			return &ForwardResp{Rejected: true}, nil
		}
		req.Relay.IsForwarded = true
		msg = req.Relay
	case req.Subscribe != nil:
		req.Subscribe.IsForwarded = true
		msg = req.Subscribe
	case req.Unsubscribe != nil:
		req.Unsubscribe.IsForwarded = true
		msg = req.Unsubscribe
	default:
		return nil, errors.New("cluster: an empty forwarded request")
	}
	conn := s.connection(c, req)
	// A trusted service's flag is taken only from a node that sets it only
	// for one.
	conn.insecure.Store(req.Insecure && s.node.knownToSupport(capService))
	if err := conn.handler(msg); err != nil {
		log.ErrLogger.Warn().Err(err).Str("context", "cluster.handleForward").Str("from", req.Node).Msg("forwarded request failed")
	}
	return &ForwardResp{}, nil
}

// takesPublish reports whether this node owns every valid topic of pub.
func (c *Cluster) takesPublish(contract uint32, pub *utp.Publish) bool {
	ring := c.getRing()
	for _, m := range pub.Messages {
		t := security.ParseKey(m.Topic)
		if t.TopicType == security.TopicInvalid {
			continue
		}
		if ring.Get(topicRingKey(contract, t.Topic[:t.Size])) != c.thisNodeName {
			return false
		}
	}
	return true
}

// takesRelay reports whether this node, not catching up, holds every valid
// topic without wildcards of relay.
func (c *Cluster) takesRelay(contract uint32, relay *utp.Relay) bool {
	if c.catchingUp() {
		return false
	}
	ring := c.getRing()
	for _, r := range relay.RelayRequests {
		t := security.ParseKey(r.Topic)
		name := t.Topic[:t.Size]
		if t.TopicType == security.TopicInvalid || isWildcardTopic(name) {
			continue
		}
		if !containsNode(ring.GetN(topicRingKey(contract, name), c.replicas), c.thisNodeName) {
			return false
		}
	}
	return true
}

// connection returns the stand-in's connection, making it and starting its
// outbound pump if there is none. Called on the stand-in's goroutine.
func (s *standIn) connection(c *Cluster, req *ForwardReq) *_Conn {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn != nil {
		return conn
	}
	conn = Globals.Service.newRpcConn(s.node, req.ConnID, req.SessID, req.ClientID)
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	go c.pump(s, conn)
	return conn
}

// stop ends the stand-in's pump, which drops its subscriptions.
func (s *standIn) stop() {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn == nil {
		return
	}
	select {
	case conn.stop <- nil:
	default:
	}
}

// pump sends what the stand-in's handlers write for the client to the
// client's node, until it is stopped or the client's node can't be
// reached. Then the stand-in's subscriptions are dropped, on its goroutine.
func (c *Cluster) pump(s *standIn, conn *_Conn) {
	send := func(m lp.MessagePack) bool {
		b, err := lp.Encode(m)
		if err != nil {
			log.ErrLogger.Error().Err(err).Str("context", "cluster.pump").Msg("unable to encode a message for a client of " + s.node.name)
			return true
		}
		return c.toClient(s.node, &ToClientReq{ConnID: s.key.conn, Raw: b.Bytes()}) == nil
	}
loop:
	for {
		select {
		case m := <-conn.send:
			if !send(m) {
				break loop
			}
		case p := <-conn.pub:
			if !send(p) {
				break loop
			}
		case v := <-conn.stop:
			if m, ok := v.(lp.MessagePack); ok {
				send(m)
			}
			break loop
		case <-c.quit:
			break loop
		}
	}
	if conn.setClosed() {
		close(conn.closeC)
	}
	s.enqueue(func() {
		Globals.connCache.delete(conn.connID)
		conn.unsubAll()
		s.mu.Lock()
		if s.conn == conn {
			s.conn = nil
		}
		s.mu.Unlock()
	})
}

// dropStandInsOf stops the stand-ins of node's clients.
func (c *Cluster) dropStandInsOf(node string) {
	c.dropStandIns(func(k proxyKey) bool { return k.node == node })
}

// dropStandIns stops the stand-ins whose key matches.
func (c *Cluster) dropStandIns(match func(proxyKey) bool) {
	c.proxyMu.Lock()
	var drop []*standIn
	for k, s := range c.proxies {
		if match(k) {
			drop = append(drop, s)
			delete(c.proxies, k)
		}
	}
	c.proxyMu.Unlock()
	for _, s := range drop {
		s.enqueue(s.stop)
	}
}

// proxyDeliver hands a message for a stand-in's client to the client's
// node, which delivers it as to its own client: logged and numbered there
// when reliable.
func (c *Cluster) proxyDeliver(conn *_Conn, m *message.Message, reliable bool) bool {
	if err := c.toClient(conn.clnode, &ToClientReq{ConnID: conn.remoteID, Message: m, Reliable: reliable}); err != nil {
		log.ErrLogger.Err(err).Str("context", "conn.deliver").Int64("connid", int64(conn.remoteID)).Msg("unable to forward message to origin node")
		return false
	}
	return true
}

// toClient calls a client's node with req.
func (c *Cluster) toClient(n *ClusterNode, req *ToClientReq) error {
	req.Node = c.thisNodeName
	var unused bool
	return n.callTimeout("Cluster.ToClient", req, &unused, deliverTimeout)
}

// localClient returns this node's client connection id, if node is one its
// requests were forwarded to.
func (c *Cluster) localClient(node string, id uid.LID) *_Conn {
	conn := Globals.connCache.get(id)
	if conn == nil || conn.clnode != nil || !wasForwardedTo(conn, node) {
		log.ErrLogger.Debug().Str("from", node).Int64("connid", int64(id)).Msg("cluster: dropped a delivery for an unknown client connection")
		return nil
	}
	return conn
}

// ToClient writes a message, or raw bytes, to a client of this node.
func (c *Cluster) ToClient(req *ToClientReq, ok *bool) error {
	conn := c.localClient(req.Node, req.ConnID)
	if conn == nil {
		return nil
	}
	if req.Message != nil {
		if deliverDelay > 0 {
			time.Sleep(deliverDelay)
		}
		*ok = conn.deliver(req.Message, req.Reliable)
		return nil
	}
	*ok = conn.SendRawBytes(req.Raw)
	return nil
}

// deliverRemote delivers messages to clients of other nodes: one call per
// node, all at once.
func (c *Cluster) deliverRemote(remote map[*ClusterNode][]Delivery) {
	if c == nil || len(remote) == 0 {
		return
	}
	var wg sync.WaitGroup
	for n, ds := range remote {
		wg.Add(1)
		go func(n *ClusterNode, ds []Delivery) {
			defer wg.Done()
			c.deliverTo(n, ds)
		}(n, ds)
	}
	wg.Wait()
}

// deliverTo delivers to clients of node n: in one call, or one per message
// if n lacks it.
func (c *Cluster) deliverTo(n *ClusterNode, ds []Delivery) {
	if hasCapability(capDeliver) && n.supports(capDeliver) {
		var unused bool
		err := n.callTimeout("Cluster.Deliver", &DeliverReq{Node: c.thisNodeName, Deliveries: ds}, &unused, deliverTimeout)
		if err == nil {
			return
		}
		if !n.lacks(err, capDeliver) {
			log.ErrLogger.Warn().Err(err).Str("context", "cluster.deliverRemote").Str("node", n.name).Int("messages", len(ds)).Msg("messages not delivered to clients of the node")
			return
		}
	}
	for _, d := range ds {
		if err := c.toClient(n, &ToClientReq{ConnID: d.ConnID, Message: d.Message, Reliable: d.Reliable}); err != nil {
			log.ErrLogger.Warn().Err(err).Str("context", "cluster.deliverRemote").Str("node", n.name).Msg("message not delivered to a client of the node")
		}
	}
}

// Deliver delivers messages to clients of this node, each client on its
// own goroutine.
func (c *Cluster) Deliver(req *DeliverReq, unused *bool) error {
	if err := refuse(capDeliver); err != nil {
		return err
	}
	if deliverDelay > 0 {
		time.Sleep(deliverDelay)
	}
	byConn := make(map[uid.LID][]Delivery)
	for _, d := range req.Deliveries {
		byConn[d.ConnID] = append(byConn[d.ConnID], d)
	}
	for id, ds := range byConn {
		conn := c.localClient(req.Node, id)
		if conn == nil {
			continue
		}
		go func(conn *_Conn, ds []Delivery) {
			for _, d := range ds {
				conn.deliver(d.Message, d.Reliable)
			}
		}(conn, ds)
	}
	return nil
}

// rebalance moves every client's subscriptions to where the current ring
// holds them, sending them again to the nodes in resend, and stops the
// stand-ins of clients of nodes no longer in the live set.
func (c *Cluster) rebalance(resend map[string]bool) {
	live := c.getRingNodes()
	c.dropStandIns(func(k proxyKey) bool { return !containsNode(live, k.node) })
	var conns []*_Conn
	for _, conn := range Globals.connCache.all() {
		if conn.clnode == nil && conn.clientID != nil && !conn.isClosed() {
			conns = append(conns, conn)
		}
	}
	for attempt := 1; attempt <= rebalanceAttempts && len(conns) > 0; attempt++ {
		var failed []*_Conn
		for _, conn := range conns {
			if !conn.rehome(resend, attempt == rebalanceAttempts) {
				failed = append(failed, conn)
			}
		}
		conns = failed
		if len(conns) > 0 && attempt < rebalanceAttempts {
			select {
			case <-c.quit:
				return
			case <-time.After(rebalanceRetry):
			}
		}
	}
}

// askResync asks node n, put back in the live set, to send its clients'
// subscriptions to this node again, without waiting.
func (c *Cluster) askResync(n *ClusterNode) {
	if !n.supports(capResync) {
		return
	}
	var unused bool
	n.goCall("Cluster.Resync", &ResyncReq{Node: c.thisNodeName}, &unused, rebuildTimeout, func(err error) {
		if err != nil && !n.lacks(err, capResync) {
			log.ErrLogger.Debug().Err(err).Str("peer", n.name).Msg("cluster: resync not asked")
		}
	})
}

// Resync sends this node's clients' subscriptions to the sender again.
func (c *Cluster) Resync(req *ResyncReq, unused *bool) error {
	if err := refuse(capResync); err != nil {
		return err
	}
	c.rebalance(map[string]bool{req.Node: true})
	return nil
}
